(function mobilePublisher() {
  const TEAM_SLUG = (() => {
    const params = new URLSearchParams(window.location.search);
    const fromQuery = (params.get('team') || '').trim();
    if (fromQuery) {
      return fromQuery.replace(/[^a-zA-Z0-9_-]/g, '');
    }
    return 'thinkroot';
  })();

  const TOPIC = `resilitrack/${TEAM_SLUG}/bus/location`;
  const HEARTBEAT_TOPIC = `resilitrack/${TEAM_SLUG}/bus/heartbeat`;
  const MQTT_WSS_URL = 'wss://broker.hivemq.com:8884/mqtt';
  const STORAGE_KEY = 'mobile-gps-buffer-v1';
  const CLIENT_ID_KEY = 'mobile-gps-client-id-v1';
  const PUBLISH_QOS = 1;
  const PUBLISH_FAIL_THRESHOLD = 2;
  const PUBLISH_ACK_TIMEOUT_MS = 4500;
  const HEALTHCHECK_INTERVAL_MS = 5000;
  const BROKER_SILENCE_MS = 9000;

  const statusEl = document.getElementById('status');
  const logsEl = document.getElementById('logs');
  const busIdEl = document.getElementById('busId');
  const startBtn = document.getElementById('startBtn');
  const stopBtn = document.getElementById('stopBtn');

  let client = null;
  let watchId = null;
  let sequenceId = 0;
  let mqttConnected = false;
  let currentMode = 'offline';
  let syncInProgress = false;
  let buffer = loadBuffer();
  let clientId = loadClientId();
  let lastSuccessfulPublishAt = 0;
  let lastBrokerActivityAt = 0;
  let consecutivePublishFailures = 0;
  let hasEverPublished = false;
  let forcedOffline = false;
  let lastForcedOfflineReason = '';
  let heartbeatInFlight = false;

  function loadBuffer() {
    try {
      const raw = localStorage.getItem(STORAGE_KEY);
      return raw ? JSON.parse(raw) : [];
    } catch {
      return [];
    }
  }

  function saveBuffer() {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(buffer));
  }

  function loadClientId() {
    const existing = localStorage.getItem(CLIENT_ID_KEY);
    if (existing) {
      return existing;
    }
    const generated = `mobile-${Math.random().toString(16).slice(2, 10)}`;
    localStorage.setItem(CLIENT_ID_KEY, generated);
    return generated;
  }

  function log(message) {
    const line = `[${new Date().toLocaleTimeString()}] ${message}`;
    logsEl.textContent = `${line}\n${logsEl.textContent}`;
    console.log(line);
  }

  function noteBrokerActivity() {
    lastBrokerActivityAt = Date.now();
  }

  function getBrowserNetworkHint() {
    const connection = navigator.connection || navigator.mozConnection || navigator.webkitConnection;
    if (!connection) {
      return 'strong';
    }
    const type = connection.effectiveType || '';
    if (type === 'slow-2g' || type === '2g') {
      return 'weak';
    }
    return 'strong';
  }

  function computeNetworkMode() {
    if (forcedOffline || !mqttConnected) {
      return 'offline';
    }
    if (!navigator.onLine) {
      return 'offline';
    }
    return getBrowserNetworkHint();
  }

  function applyMode(nextMode) {
    const previousMode = currentMode;
    currentMode = nextMode;
    statusEl.textContent = `Status: ${currentMode.toUpperCase()} | Buffered: ${buffer.length}`;

    if (previousMode === 'offline' && nextMode !== 'offline') {
      log('Connection restored → switching to online');
      syncBufferedMessages();
    }
  }

  function setMode(nextMode) {
    applyMode(nextMode);
  }

  function markForcedOffline(reason) {
    if (reason !== lastForcedOfflineReason) {
      log(reason);
    }
    forcedOffline = true;
    lastForcedOfflineReason = reason;
    currentMode = 'offline';
    statusEl.textContent = `Status: ${currentMode.toUpperCase()} | Buffered: ${buffer.length}`;
  }

  function clearForcedOfflineIfHealthy() {
    if (!forcedOffline) {
      return;
    }
    if (mqttConnected && consecutivePublishFailures === 0) {
      forcedOffline = false;
      lastForcedOfflineReason = '';
      applyMode(computeNetworkMode());
    }
  }

  function connectMqtt() {
    client = window.mqtt.connect(MQTT_WSS_URL, {
      clean: false,
      clientId,
      reconnectPeriod: 2000,
      connectTimeout: 4000,
      keepalive: 10,
      reschedulePings: true,
    });

    client.on('connect', () => {
      mqttConnected = true;
      consecutivePublishFailures = 0;
      lastSuccessfulPublishAt = Date.now();
      noteBrokerActivity();
      clearForcedOfflineIfHealthy();
      setMode(computeNetworkMode());
      log('Connected to public MQTT broker');
      log(`MQTT connected (${MQTT_WSS_URL}) topic=${TOPIC}`);
    });

    client.on('reconnect', () => {
      log('MQTT reconnecting...');
    });

    client.on('offline', () => {
      mqttConnected = false;
      markForcedOffline('MQTT client offline → switching to offline');
    });

    client.on('close', () => {
      mqttConnected = false;
      markForcedOffline('MQTT disconnected → switching to offline');
    });

    client.on('error', (error) => {
      mqttConnected = false;
      markForcedOffline(`MQTT error → switching to offline (${error.message})`);
    });

    client.on('packetsend', () => {
      noteBrokerActivity();
    });

    client.on('packetreceive', () => {
      noteBrokerActivity();
    });
  }

  function publishWithAck(topic, payload, label) {
    return new Promise((resolve, reject) => {
      if (!client || !mqttConnected) {
        reject(new Error('MQTT not connected'));
        return;
      }

      let settled = false;
      const timeoutId = window.setTimeout(() => {
        if (settled) {
          return;
        }
        settled = true;
        consecutivePublishFailures += 1;
        reject(new Error(`${label} ack timeout`));
      }, PUBLISH_ACK_TIMEOUT_MS);

      client.publish(topic, JSON.stringify(payload), { qos: PUBLISH_QOS }, (error) => {
        if (settled) {
          return;
        }
        settled = true;
        window.clearTimeout(timeoutId);
        if (error) {
          consecutivePublishFailures += 1;
          reject(error);
          return;
        }
        consecutivePublishFailures = 0;
        lastSuccessfulPublishAt = Date.now();
        noteBrokerActivity();
        hasEverPublished = true;
        clearForcedOfflineIfHealthy();
        resolve();
      });
    });
  }

  async function publishMessage(message) {
    try {
      await publishWithAck(TOPIC, message, `GPS publish seq=${message.sequence_id}`);
    } catch (error) {
      if (consecutivePublishFailures >= PUBLISH_FAIL_THRESHOLD) {
        markForcedOffline(`Publish failed repeatedly (${consecutivePublishFailures}) → switching to offline`);
      }
      throw error;
    }
  }

  async function sendBrokerHeartbeat() {
    if (!client || !mqttConnected || heartbeatInFlight) {
      return;
    }

    heartbeatInFlight = true;
    try {
      await publishWithAck(
        HEARTBEAT_TOPIC,
        {
          bus_id: busIdEl.value.trim() || 'BUS_01',
          timestamp: Date.now(),
          type: 'heartbeat',
        },
        'Heartbeat'
      );
    } catch (error) {
      markForcedOffline(`Broker heartbeat failed → switching to offline (${error.message})`);
    } finally {
      heartbeatInFlight = false;
    }
  }

  function addToBuffer(message) {
    buffer.push(message);
    saveBuffer();
    log(`Buffered ${buffer.length} messages`);
    statusEl.textContent = `Status: ${currentMode.toUpperCase()} | Buffered: ${buffer.length}`;
  }

  async function syncBufferedMessages() {
    if (syncInProgress || buffer.length === 0 || currentMode === 'offline') {
      return;
    }

    syncInProgress = true;
    log('Syncing...');

    try {
      for (const queued of buffer) {
        const syncedMessage = {
          ...queued,
          network_mode: currentMode,
          buffered: true,
        };
        await publishMessage(syncedMessage);
      }
      buffer = [];
      saveBuffer();
      log('Sync complete');
    } catch (error) {
      log(`Sync paused: ${error.message}`);
    } finally {
      syncInProgress = false;
      statusEl.textContent = `Status: ${currentMode.toUpperCase()} | Buffered: ${buffer.length}`;
    }
  }

  async function handleGpsUpdate(position) {
    const coords = position.coords;
    const message = {
      bus_id: busIdEl.value.trim() || 'BUS_01',
      lat: Number(coords.latitude),
      lng: Number(coords.longitude),
      timestamp: Date.now(),
      speed: Number.isFinite(coords.speed) ? Number(coords.speed) : 0,
      heading: Number.isFinite(coords.heading) ? Number(coords.heading) : 0,
      network_mode: currentMode,
      sequence_id: sequenceId++,
      buffered: false,
    };

    setMode(computeNetworkMode());

    if (currentMode === 'offline') {
      addToBuffer(message);
      return;
    }

    try {
      await publishMessage(message);
      log(`Published seq=${message.sequence_id} lat=${message.lat.toFixed(6)} lng=${message.lng.toFixed(6)} mode=${currentMode}`);
    } catch (error) {
      markForcedOffline(`Publish failed → switching to offline (${error.message})`);
      addToBuffer(message);
      log(`Publish failed, moved to buffer: ${error.message}`);
    }
  }

  function startTracking() {
    if (!('geolocation' in navigator)) {
      log('Geolocation is not supported on this device/browser.');
      return;
    }
    if (!client) {
      connectMqtt();
    }
    if (watchId !== null) {
      return;
    }

    watchId = navigator.geolocation.watchPosition(
      handleGpsUpdate,
      (error) => log(`GPS error: ${error.message}`),
      {
        enableHighAccuracy: true,
        maximumAge: 1000,
        timeout: 10000,
      }
    );
    log('GPS tracking started');
  }

  function stopTracking() {
    if (watchId !== null) {
      navigator.geolocation.clearWatch(watchId);
      watchId = null;
    }
    log('GPS tracking stopped');
  }

  window.addEventListener('online', () => {
    setMode(computeNetworkMode());
  });

  window.addEventListener('offline', () => {
    markForcedOffline('Browser offline → switching to offline');
  });

  startBtn.addEventListener('click', startTracking);
  stopBtn.addEventListener('click', stopTracking);

  setInterval(() => {
    if (watchId === null) {
      return;
    }
    if (!client) {
      return;
    }

    const now = Date.now();
    const sinceLastBrokerActivity = lastBrokerActivityAt ? now - lastBrokerActivityAt : 0;

    if (mqttConnected && sinceLastBrokerActivity > BROKER_SILENCE_MS) {
      const seconds = Math.round(sinceLastBrokerActivity / 1000);
      markForcedOffline(`Broker silent for ${seconds} sec → switching to offline`);
      return;
    }

    if (mqttConnected && sinceLastBrokerActivity >= HEALTHCHECK_INTERVAL_MS && !heartbeatInFlight) {
      sendBrokerHeartbeat();
    }
  }, 1000);

  setMode(computeNetworkMode());
  if (buffer.length > 0) {
    log(`Recovered ${buffer.length} buffered messages from local storage`);
  }
})();
