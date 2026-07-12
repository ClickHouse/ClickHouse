(function () {
  'use strict';

  // Mintlify adapter for clickhouse-docs/src/lib/galaxy. Keep the event
  // envelope and transport compatible with the legacy Docusaurus site so
  // existing dashboards continue to receive the same data.
  var APPLICATION = 'DOCS_WEBSITE';
  var isLocal = /^(localhost|127\.0\.0\.1|\[?::1\]?)$/
    .test(window.location.hostname || '');
  var isMintlifyPreview = /\.mintlify\.app$/
    .test(window.location.hostname || '');
  var isCanonicalDocs = window.location.origin === 'https://clickhouse.com' &&
    /^\/docs(?:\/|$)/.test(window.location.pathname);
  var API_HOST = null;
  if (!isLocal && isMintlifyPreview) {
    API_HOST = 'https://control-plane-internal.clickhouse-dev.com';
  } else if (isCanonicalDocs) {
    API_HOST = 'https://control-plane-internal.clickhouse.cloud';
  }
  var API_PATH = '/api/galaxy?sendGalaxyForensicEvent';
  var USER_ID_KEY = 'glx_anonymous_id';
  var SESSION_ID_KEY = 'glx_id';
  var MAX_COOKIE_AGE = 2147483647;
  var FLUSH_INTERVAL_MS = 5000;
  var KEEPALIVE_LIMIT_BYTES = 60 * 1024;
  var ATTRIBUTION_HOSTS = ['clickhouse.cloud', 'console.clickhouse.cloud'];

  if (window.__clickhouseGalaxyInitialized) return;
  window.__clickhouseGalaxyInitialized = true;

  var eventsQueue = [];
  var flushInFlight = null;

  function uuid() {
    if (window.crypto && typeof window.crypto.randomUUID === 'function') {
      return window.crypto.randomUUID();
    }

    // randomUUID is available in current browsers. This fallback keeps local
    // previews and older embedded browsers functional without adding a bundle
    // dependency solely for UUID generation.
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
      var r;
      if (window.crypto && typeof window.crypto.getRandomValues === 'function') {
        var value = new Uint8Array(1);
        window.crypto.getRandomValues(value);
        r = value[0] & 15;
      } else {
        r = Math.floor(Math.random() * 16);
      }
      return (c === 'x' ? r : (r & 3) | 8).toString(16);
    });
  }

  function getCookie(name) {
    var prefix = encodeURIComponent(name) + '=';
    var cookies = document.cookie ? document.cookie.split(';') : [];
    for (var i = 0; i < cookies.length; i++) {
      var cookie = cookies[i].trim();
      if (cookie.indexOf(prefix) === 0) {
        try {
          return decodeURIComponent(cookie.slice(prefix.length));
        } catch (e) {
          return cookie.slice(prefix.length);
        }
      }
    }
    return null;
  }

  function storageGet(storage, key) {
    try {
      return storage.getItem(key);
    } catch (e) {
      return null;
    }
  }

  function storageSet(storage, key, value) {
    try {
      storage.setItem(key, value);
    } catch (e) {}
  }

  function persistUserId(value) {
    try {
      var cookie = encodeURIComponent(USER_ID_KEY) + '=' + encodeURIComponent(value) +
        '; path=/; max-age=' + MAX_COOKIE_AGE;
      if (window.location.protocol === 'https:') cookie += '; secure';
      document.cookie = cookie;
    } catch (e) {}

    storageSet(window.localStorage, USER_ID_KEY, value);
    storageSet(window.sessionStorage, USER_ID_KEY, value);
  }

  function getUserId() {
    var id = getCookie(USER_ID_KEY) ||
      storageGet(window.localStorage, USER_ID_KEY) ||
      storageGet(window.sessionStorage, USER_ID_KEY) ||
      uuid();
    persistUserId(id);
    return id;
  }

  function getSessionId() {
    var id = storageGet(window.sessionStorage, SESSION_ID_KEY);
    if (!id) {
      id = uuid();
      storageSet(window.sessionStorage, SESSION_ID_KEY, id);
    }
    return id || 'unknown';
  }

  function getContext() {
    return {
      application: APPLICATION,
      page: window.location.href,
      userAgent: window.navigator.userAgent,
    };
  }

  function track(event, properties) {
    if (!event) return;

    var eventProperties = properties || { interaction: 'click' };
    var interaction = eventProperties.interaction;
    var extraProperties = {};
    for (var key in eventProperties) {
      if (Object.prototype.hasOwnProperty.call(eventProperties, key) && key !== 'interaction') {
        extraProperties[key] = eventProperties[key];
      }
    }

    var parts = String(event).split('.');
    var payloadProperties = getContext();
    eventsQueue.push({
      application: APPLICATION,
      timestamp: Date.now(),
      userId: getUserId(),
      namespace: parts[0],
      component: parts[1],
      interaction: interaction,
      orgId: payloadProperties.orgId,
      event: parts[2],
      message: parts[2],
      properties: Object.assign({ properties: payloadProperties }, extraProperties),
    });
  }

  function post(url, requestBody) {
    var json = JSON.stringify(requestBody);
    var blob = new Blob([json], { type: 'application/json;charset=UTF-8' });
    var tooLarge = blob.size > KEEPALIVE_LIMIT_BYTES;

    if (!tooLarge && typeof window.navigator.sendBeacon === 'function') {
      try {
        if (window.navigator.sendBeacon(url, blob)) return Promise.resolve();
      } catch (e) {}
    }

    return window.fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json;charset=UTF-8' },
      body: json,
      keepalive: !tooLarge,
    });
  }

  function flushEvents() {
    if (flushInFlight || eventsQueue.length === 0) {
      return flushInFlight || Promise.resolve();
    }

    var eventCount = eventsQueue.length;
    if (!API_HOST) {
      eventsQueue.splice(0, eventCount);
      return Promise.resolve();
    }

    var request = {
      rpcAction: 'sendGalaxyForensicEvent',
      galaxySessionId: getSessionId(),
      data: eventsQueue.slice(0, eventCount),
    };

    flushInFlight = post(API_HOST + API_PATH, request)
      .then(function () {
        eventsQueue.splice(0, eventCount);
      })
      .catch(function (error) {
        console.error('Could not log to galaxy', error);
      })
      .then(function () {
        flushInFlight = null;
      });

    return flushInFlight;
  }

  window.galaxy = {
    track: track,
    flushEvents: flushEvents,
  };

  window.dispatchEvent(new Event('galaxy:ready'));

  var flushInterval = window.setInterval(flushEvents, FLUSH_INTERVAL_MS);

  function stopGalaxy() {
    window.clearInterval(flushInterval);
    void flushEvents();
  }

  window.addEventListener('beforeunload', stopGalaxy);
  window.addEventListener('pagehide', function (event) {
    // Timers are suspended while a page is in the back/forward cache and
    // resume when it is restored. Clearing the interval here would leave a
    // restored page queueing events without ever flushing them.
    if (!event.persisted) stopGalaxy();
  });

  function pageEventPrefix(path) {
    return path.indexOf('/resources/support-center/knowledge-base/') !== -1
      ? 'knowledgebase.page.' + path
      : 'docs.page.' + path;
  }

  function trackPageEvent(action) {
    track(pageEventPrefix(window.location.pathname) + '.window.' + action, {
      interaction: 'trigger',
    });
  }

  // Docusaurus hooks tracked load, focus, and blur for every docs and KB page.
  // Mintlify is a SPA, so watch the path to emit one load event per route.
  var lastPath = window.location.pathname;
  trackPageEvent('load');
  window.addEventListener('focus', function () { trackPageEvent('focus'); });
  window.addEventListener('blur', function () { trackPageEvent('blur'); });

  function watchPath() {
    var path = window.location.pathname;
    if (path !== lastPath) {
      lastPath = path;
      trackPageEvent('load');
      updateCloudLinks();
    }
    window.requestAnimationFrame(watchPath);
  }
  window.requestAnimationFrame(watchPath);

  // Preserve the attribution behavior from src/clientModules/utmPersistence:
  // retain campaign parameters and attach the Galaxy ID and page paths to
  // links into ClickHouse Cloud.
  function saveAttribution() {
    try {
      var params = new URLSearchParams(window.location.search);
      var values = {};
      params.forEach(function (value, key) {
        if (key.indexOf('utm_') === 0 || key === 'gclid') values[key] = value;
      });
      if (Object.keys(values).length > 0) {
        var expiration = new Date();
        expiration.setDate(expiration.getDate() + 14);
        window.localStorage.setItem('ch-utms', JSON.stringify({
          data: values,
          timestamp: expiration.getTime(),
        }));
      }
      if (!window.localStorage.getItem('origPath')) {
        window.localStorage.setItem('origPath', window.location.pathname);
      }
    } catch (e) {}
  }

  function storedAttribution() {
    try {
      var stored = window.localStorage.getItem('ch-utms');
      if (!stored) return null;
      var parsed = JSON.parse(stored);
      if (Date.now() > Number(parsed.timestamp)) {
        window.localStorage.removeItem('ch-utms');
        return null;
      }
      return parsed.data;
    } catch (e) {
      return null;
    }
  }

  function googleAnalyticsId() {
    return getCookie('_ga');
  }

  function updateCloudLinks() {
    if (!isCanonicalDocs) return;

    saveAttribution();
    var attribution = storedAttribution();
    var links = document.querySelectorAll('a[href*="clickhouse.cloud"]');

    for (var i = 0; i < links.length; i++) {
      try {
        var url = new URL(links[i].href, window.location.href);
        if (ATTRIBUTION_HOSTS.indexOf(url.hostname) === -1) continue;

        if (attribution) {
          for (var key in attribution) {
            if (Object.prototype.hasOwnProperty.call(attribution, key)) {
              url.searchParams.set(key, attribution[key]);
            }
          }
        }
        url.searchParams.set('glxid', getUserId());
        url.searchParams.set('pagePath', window.location.pathname);

        var originalPath = storageGet(window.localStorage, 'origPath');
        if (originalPath) url.searchParams.set('origPath', originalPath);

        var gaId = googleAnalyticsId();
        if (gaId) url.searchParams.set('utm_ga', gaId);

        links[i].href = url.toString();
      } catch (e) {
        console.warn('Failed to update cloud link', e);
      }
    }
  }

  function startLinkObserver() {
    if (!isCanonicalDocs) return;

    updateCloudLinks();
    new MutationObserver(updateCloudLinks).observe(document.body, {
      childList: true,
      subtree: true,
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', startLinkObserver);
  } else {
    startLinkObserver();
  }
})();
