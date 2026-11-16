(function () {
  'use strict';

  // Derive a stable per-user identifier from the GA cookie so Kapa can
  // correlate sessions to a single visitor.
  function extractGoogleAnalyticsUserIdFromCookie(gaCookie) {
    if (!gaCookie) return undefined;
    var parts = gaCookie.split('.').slice(-2);
    if (parts.length === 2) return parts.join('-');
    return undefined;
  }

  function getBrowserCookie(name) {
    if (typeof document !== 'object' || typeof document.cookie !== 'string') return undefined;
    var decoded = decodeURIComponent(document.cookie);
    var prefix = name + '=';
    var parts = decoded.split(';');
    for (var i = 0; i < parts.length; i++) {
      var c = parts[i].trim();
      if (c.indexOf(prefix) === 0) return c.substring(prefix.length);
    }
    return undefined;
  }

  var gaUserId = extractGoogleAnalyticsUserIdFromCookie(getBrowserCookie('_ga'));
  window.kapaSettings = { user: { uniqueClientId: gaUserId } };

  // Kapa doesn't support iOS 16.4 or lower — skip injection on those devices.
  function isOldiOS() {
    var ua = navigator.userAgent;
    if (!/iPad|iPhone/.test(ua)) return false;
    var m = ua.match(/OS (\d+)(?:_(\d+))?/);
    if (!m) return true;
    var major = parseInt(m[1], 10);
    var minor = m[2] ? parseInt(m[2], 10) : 0;
    if (isNaN(minor)) minor = 0;
    return major < 16 || (major === 16 && minor <= 4);
  }

  function reasoningModeColor() {
    return document.documentElement.classList.contains('dark') ? '#FAFF69' : '#161517';
  }

  function syncReasoningModeColor() {
    var script = document.getElementById('kapa-widget-script');
    if (!script) return;

    var color = reasoningModeColor();
    script.setAttribute('data-deep-thinking-button-text-color', color);
    if (window.Kapa && typeof window.Kapa.updateConfig === 'function') {
      window.Kapa.updateConfig({ 'data-deep-thinking-button-text-color': color });
    }
  }

  function observeDocsTheme() {
    var queued = false;
    var observer = new MutationObserver(function () {
      if (queued) return;
      queued = true;
      window.requestAnimationFrame(function () {
        queued = false;
        syncReasoningModeColor();
      });
    });
    observer.observe(document.documentElement, { attributes: true, attributeFilter: ['class'] });
  }

  function insertKapaWidget() {
    if (document.getElementById('kapa-widget-script')) return;
    if (isOldiOS()) {
      console.log('Kapa widget not added: detected iOS 16.4 or lower');
      return;
    }

    var script = document.createElement('script');
    script.id = 'kapa-widget-script';
    script.src = 'https://widget.kapa.ai/kapa-widget.bundle.js';
    script.async = true;
    script.defer = true;

    var attrs = {
      'data-website-id': '5df6cc2b-732e-44e4-b789-aec81b70fe46',
      'data-project-name': 'ClickHouse',
      'data-project-color': '#161517',
      'data-project-color-dark': '#FAFF69',
      'data-project-logo': 'https://avatars.githubusercontent.com/u/54801242?s=200&v=4',
      'data-color-scheme-selector': '.dark',
      'data-surface-color': '#FFFFFF',
      'data-surface-elevated-color': '#F6F7FA',
      'data-surface-hover-color': '#F0F1F3',
      'data-text-color': '#161517',
      'data-text-muted-color': '#696E79',
      'data-border-color': '#E6E7E9',
      'data-anchor-color': '#161517',
      'data-surface-color-dark': '#1C1C1C',
      'data-surface-elevated-color-dark': '#121212',
      'data-surface-hover-color-dark': '#282828',
      'data-text-color-dark': '#E6E6E6',
      'data-text-muted-color-dark': '#8C8C8C',
      'data-border-color-dark': '#2E2E2E',
      'data-anchor-color-dark': '#FAFF69',
      'data-font-family': 'Inter, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif',
      'data-modal-size': '782px',
      'data-modal-content-border-radius': '16px',
      'data-modal-content-border': '1px solid #E6E7E9',
      'data-modal-content-border-dark': '1px solid #2E2E2E',
      'data-modal-header-background-color': '#FFFFFF',
      'data-modal-header-background-color-dark': '#1C1C1C',
      'data-modal-header-border-bottom': '1px solid #E6E7E9',
      'data-modal-header-border-bottom-dark': '1px solid #2E2E2E',
      'data-query-input-background-color': '#FFFFFF',
      'data-query-input-background-color-dark': '#151515',
      'data-query-input-border-color': '#D9DADE',
      'data-query-input-border-color-dark': '#383838',
      'data-query-input-focus-border-color': '#161517',
      'data-query-input-focus-border-color-dark': '#FAFF69',
      'data-submit-button-background-color': '#161517',
      'data-submit-button-color': '#FAFF69',
      'data-submit-button-hover-background-color': '#2B2B2E',
      'data-submit-button-background-color-dark': '#FAFF69',
      'data-submit-button-color-dark': '#151515',
      'data-submit-button-hover-background-color-dark': '#FCFF9E',
      'data-submit-button-hover-color-dark': '#151515',
      'data-deep-thinking-button-text-color': reasoningModeColor(),
      'data-example-question-button-border': '1px solid #E0E1E4',
      'data-example-question-button-border-dark': '1px solid #383838',
      'data-conversation-item-question-background-color': '#F3F4F6',
      'data-conversation-item-question-background-color-dark': '#262626',
      'data-modal-title-ask-ai': 'Ask AI',
      'data-modal-disclaimer': 'This is a custom LLM for ClickHouse with access to all developer documentation, open GitHub Issues, YouTube videos, and resolved StackOverflow posts. Please note that answers are generated by AI and may not be fully accurate, so please use your best judgement.',
      'data-modal-example-questions': 'How to speed up queries?,How to use materialized views?',
      // Disable the MCP install dropdown/tab in the Kapa widget header.
      'data-mcp-enabled': 'false',
      // Hide Kapa's floating launcher — we open it from the Ask AI button
      // injected by ask-ai-button.js.
      'data-launcher-button-hidden': 'true',
    };

    Object.keys(attrs).forEach(function (k) {
      script.setAttribute(k, attrs[k]);
    });

    document.head.appendChild(script);
    observeDocsTheme();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', insertKapaWidget);
  } else {
    try {
      insertKapaWidget();
    } catch (e) {
      console.log('An error occurred while trying to load the Kapa.ai widget:', e);
    }
  }
})();
