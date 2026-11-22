(function () {
  'use strict';

  // A bottom-docked web terminal, inspired by the developer tray on `docs.stripe.com`. A thin
  // bar is always visible on desktop; opening it reveals the ClickHouse web terminal above the
  // bar without navigating away from the documentation page.
  //
  // `user=play` selects the playground's read-only demo user, which has no password, so the
  // terminal connects without prompting for credentials. This is the same endpoint that the
  // terminal on clickhouse.com connects to.
  var TERMINAL_URL = 'https://play.clickhouse.com/webterminal?user=play';
  var TERMINAL_ORIGIN = 'https://play.clickhouse.com';

  var PANEL_ID = 'ch-webterminal-panel';
  var VIEWPORT_ID = 'ch-webterminal-viewport';
  var IFRAME_ID = 'ch-webterminal-iframe';
  var RESIZER_ID = 'ch-webterminal-resizer';
  var TRAY_ID = 'ch-webterminal-tray';
  var TOGGLE_ID = 'ch-webterminal-toggle';
  var ACTION_ID = 'ch-webterminal-action';
  var SPACER_ID = 'ch-webterminal-spacer';
  var OVERLAY_ID = 'ch-webterminal-overlay';
  var STYLE_ID = 'ch-webterminal-styles';
  var OPEN_CLASS = 'ch-webterminal-open';
  var PAGE_LOCK_CLASS = 'ch-webterminal-page-locked';
  var STATE_KEY = 'ch-webterminal-height';

  var BAR_HEIGHT = 32;
  var DEFAULT_HEIGHT_RATIO = 0.4;
  var MIN_TERMINAL_HEIGHT = 120;
  var TOP_MARGIN = 40;
  // Keep the tray aligned with the desktop docs experience. On narrow viewports the terminal
  // would cover too much of the page and there is no room for its full label.
  var DESKTOP_MIN_WIDTH = 1024;

  var panel = null;
  var viewport = null;
  var iframe = null;
  var resizer = null;
  var toggle = null;
  var action = null;
  var terminalOpen = false;
  // Height of the terminal viewport, excluding the tray.
  var terminalHeight = null;
  // True while the panel is being restored after a reload rather than opened by the reader.
  var restoringPanel = false;

  var terminalSvg = '<svg viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg" aria-hidden="true" focusable="false">'
    + '<rect x="2" y="3" width="20" height="18" rx="3" fill="none" stroke="currentColor" stroke-width="1.8"/>'
    + '<path d="M6.5 9l3 3-3 3" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/>'
    + '<line x1="12.5" y1="15" x2="17.5" y2="15" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"/>'
    + '</svg>';

  var chevronSvg = '<svg viewBox="0 0 20 20" xmlns="http://www.w3.org/2000/svg" aria-hidden="true" focusable="false">'
    + '<path d="M5.5 12.5L10 8l4.5 4.5" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"/>'
    + '</svg>';

  function injectStyles() {
    if (document.getElementById(STYLE_ID)) return;
    var style = document.createElement('style');
    style.id = STYLE_ID;
    style.textContent = ''
      + '#' + PANEL_ID + ' { position: fixed; left: 0; right: 0; bottom: 0; height: ' + BAR_HEIGHT + 'px;'
      + ' display: flex; flex-direction: column; background: #0d0d0d; color: #f3f4f6;'
      + ' z-index: 2147483646; box-shadow: 0 -1px 0 #2a2a2a; }'
      + 'html.' + PAGE_LOCK_CLASS + ', html.' + PAGE_LOCK_CLASS + ' body { overflow: hidden !important; }'
      // The spacer puts the fixed tray into the page's layout. At the end of the document the
      // footer can scroll completely above the tray instead of ending underneath it.
      + '#' + SPACER_ID + ' { display: block; width: 100%; height: ' + BAR_HEIGHT + 'px;'
      + ' flex: 0 0 ' + BAR_HEIGHT + 'px; pointer-events: none; }'
      // Mintlify pins the desktop navigation sidebar to the viewport bottom. Its language picker
      // lives on that edge, so shorten the sidebar by the tray height instead of covering it.
      + '#sidebar { bottom: ' + BAR_HEIGHT + 'px !important; }'
      + '#' + PANEL_ID + '.' + OPEN_CLASS + ' { box-shadow: 0 -12px 32px rgba(0, 0, 0, 0.35); }'
      + '#' + VIEWPORT_ID + ' { flex: 1 1 auto; min-height: 0; overflow: hidden; background: #000;'
      + ' box-sizing: border-box; overscroll-behavior: contain; }'
      + '#' + PANEL_ID + '.' + OPEN_CLASS + ' #' + VIEWPORT_ID + ' { padding: 8px 0 0 10px; }'
      // Keep the embedded terminal's native scrollbar just outside the clipped viewport. The
      // docs dock scrolls through wheel, trackpad and keyboard input without showing two adjacent
      // vertical scrollbars; the standalone web terminal retains its own scrollbar.
      + '#' + IFRAME_ID + ' { display: block; width: calc(100% + 8px); max-width: none; height: 100%; border: none; background: #000;'
      + ' overscroll-behavior: contain; }'
      + '#' + RESIZER_ID + ' { position: absolute; top: -4px; left: 0; right: 0; height: 8px;'
      + ' cursor: row-resize; user-select: none; opacity: 0; }'
      + '#' + PANEL_ID + '.' + OPEN_CLASS + ' #' + RESIZER_ID + ':hover,'
      + ' #' + PANEL_ID + '.' + OPEN_CLASS + ' #' + RESIZER_ID + '.ch-webterminal-dragging { opacity: 1;'
      + ' background: linear-gradient(to bottom, transparent 3px, #faff69 3px, #faff69 5px, transparent 5px); }'
      + '#' + TRAY_ID + ' { flex: 0 0 ' + BAR_HEIGHT + 'px; height: ' + BAR_HEIGHT + 'px; display: flex;'
      + ' align-items: stretch; justify-content: space-between; border-top: 1px solid #2a2a2a;'
      + ' background: #151515; color: #f3f4f6; font: 500 12px/1 Inter, ui-sans-serif, system-ui, sans-serif; }'
      + '#' + TOGGLE_ID + ', #' + ACTION_ID + ' { appearance: none; border: 0; margin: 0; color: inherit;'
      + ' background: transparent; cursor: pointer; }'
      + '#' + TOGGLE_ID + ' { display: flex; align-items: center; gap: 6px; min-width: 152px; padding: 0 10px 0 26px;'
      + ' border-right: 1px solid transparent; text-align: left; }'
      + '#' + TOGGLE_ID + ':hover { background: #202020; }'
      + '#' + TOGGLE_ID + ':focus-visible, #' + ACTION_ID + ':focus-visible { outline: 2px solid #faff69; outline-offset: -3px; }'
      + '#' + TOGGLE_ID + ' svg { width: 14px; height: 14px; color: #faff69; }'
      + '#' + TOGGLE_ID + ' .ch-webterminal-label { white-space: nowrap; }'
      + '#' + TOGGLE_ID + ' .ch-webterminal-shortcut { margin-left: 2px; padding: 1px 4px; border: 1px solid #404040;'
      + ' border-radius: 3px; color: #a9adb7; font: 9px/1 ui-monospace, SFMono-Regular, Menlo, monospace; }'
      + '#' + ACTION_ID + ' { display: flex; align-items: center; justify-content: center; width: ' + BAR_HEIGHT + 'px;'
      + ' border-left: 1px solid #2a2a2a; color: #a9adb7; }'
      + '#' + ACTION_ID + ':hover { color: #fff; background: #202020; }'
      + '#' + ACTION_ID + ' svg { width: 16px; height: 16px; }'
      + '#' + PANEL_ID + '.' + OPEN_CLASS + ' #' + ACTION_ID + ' svg { transform: rotate(180deg); }'
      + '@media (max-width: ' + (DESKTOP_MIN_WIDTH - 1) + 'px) { #' + PANEL_ID + ', #' + SPACER_ID + ' { display: none; }'
      + ' #sidebar { bottom: 0 !important; } }';
    document.head.appendChild(style);
  }

  function maxTerminalHeight() {
    return Math.max(MIN_TERMINAL_HEIGHT, window.innerHeight - BAR_HEIGHT - TOP_MARGIN);
  }

  function applyPanelGeometry() {
    if (!panel) return;
    var visibleTerminalHeight = terminalOpen ? Math.min(terminalHeight, maxTerminalHeight()) : 0;
    panel.style.height = (visibleTerminalHeight + BAR_HEIGHT) + 'px';
  }

  function postToTerminal(message) {
    if (iframe && iframe.contentWindow) iframe.contentWindow.postMessage(message, TERMINAL_ORIGIN);
  }

  // Client-side navigation keeps the page — and with it the terminal session — alive. Remember
  // whether the tray was open for full-page navigation and reloads. The WebSocket cannot survive
  // a reload, so a restored tray starts a new session.
  function saveState() {
    try {
      if (terminalOpen) sessionStorage.setItem(STATE_KEY, String(terminalHeight));
      else sessionStorage.removeItem(STATE_KEY);
    } catch (e) { /* storage unavailable */ }
  }

  function savedHeight() {
    try {
      var value = parseInt(sessionStorage.getItem(STATE_KEY), 10);
      return value > 0 ? value : null;
    } catch (e) {
      return null;
    }
  }

  function ensureIframe() {
    if (iframe) return;
    iframe = document.createElement('iframe');
    iframe.id = IFRAME_ID;
    iframe.title = 'ClickHouse web terminal';
    iframe.src = TERMINAL_URL;
    iframe.addEventListener('load', function () {
      postToTerminal({type: 'webterminal-hello'});
      // The terminal focuses itself when it loads. A tray restored after a reload should not
      // redirect the reader's first keystrokes into a session they did not just open.
      if (restoringPanel && document.activeElement === iframe) iframe.blur();
    });
    viewport.appendChild(iframe);
  }

  function createPanel() {
    if (panel) return;
    injectStyles();

    var spacer = document.createElement('div');
    spacer.id = SPACER_ID;
    spacer.setAttribute('aria-hidden', 'true');
    document.body.appendChild(spacer);

    panel = document.createElement('section');
    panel.id = PANEL_ID;
    panel.setAttribute('aria-label', 'ClickHouse web terminal');

    viewport = document.createElement('div');
    viewport.id = VIEWPORT_ID;
    // Wheel events normally stay inside the cross-origin iframe. If the browser retargets one
    // to the host after the terminal reaches the end of its scrollback, consume it here instead
    // of letting the documentation page move behind the panel.
    viewport.addEventListener('wheel', function (e) {
      if (!terminalOpen) return;
      e.preventDefault();
      e.stopPropagation();
    }, {passive: false});
    panel.appendChild(viewport);

    resizer = document.createElement('div');
    resizer.id = RESIZER_ID;
    resizer.setAttribute('role', 'separator');
    resizer.setAttribute('aria-label', 'Resize web terminal');
    resizer.setAttribute('aria-orientation', 'horizontal');
    resizer.addEventListener('pointerdown', startResize);
    resizer.addEventListener('touchstart', function (e) { e.preventDefault(); });
    panel.appendChild(resizer);

    var tray = document.createElement('div');
    tray.id = TRAY_ID;

    toggle = document.createElement('button');
    toggle.id = TOGGLE_ID;
    toggle.type = 'button';
    toggle.title = 'Toggle the web terminal (~)';
    toggle.setAttribute('aria-controls', VIEWPORT_ID);
    toggle.innerHTML = terminalSvg
      + '<span class="ch-webterminal-label">ClickHouse terminal</span>'
      + '<span class="ch-webterminal-shortcut">~</span>';
    toggle.addEventListener('click', toggleTerminal);
    tray.appendChild(toggle);

    action = document.createElement('button');
    action.id = ACTION_ID;
    action.type = 'button';
    action.innerHTML = chevronSvg;
    action.addEventListener('click', toggleTerminal);
    tray.appendChild(action);

    panel.appendChild(tray);
    document.body.appendChild(panel);
    updateControls();
  }

  function updateControls() {
    if (!toggle || !action) return;
    toggle.setAttribute('aria-expanded', terminalOpen ? 'true' : 'false');
    action.setAttribute('aria-label', terminalOpen ? 'Collapse web terminal' : 'Open web terminal');
    action.title = terminalOpen ? 'Collapse the web terminal' : 'Open the web terminal';
  }

  // `focusTerminal` is false when restoring after a reload so the page keeps the keyboard.
  function openTerminal(focusTerminal) {
    if (terminalOpen || window.innerWidth < DESKTOP_MIN_WIDTH) return;
    restoringPanel = !focusTerminal;
    ensureIframe();
    terminalOpen = true;
    if (terminalHeight === null) terminalHeight = Math.round(window.innerHeight * DEFAULT_HEIGHT_RATIO);
    terminalHeight = Math.max(MIN_TERMINAL_HEIGHT, Math.min(terminalHeight, maxTerminalHeight()));
    panel.classList.add(OPEN_CLASS);
    document.documentElement.classList.add(PAGE_LOCK_CLASS);
    applyPanelGeometry();
    updateControls();
    saveState();
    requestAnimationFrame(function () {
      postToTerminal({type: 'webterminal-refit', focus: focusTerminal});
      if (focusTerminal && iframe) iframe.focus();
    });
  }

  // Collapse to the tray but keep the iframe alive, preserving the session and its scrollback.
  function hideTerminal() {
    if (!terminalOpen) return;
    var terminalHadFocus = document.activeElement === iframe;
    terminalOpen = false;
    panel.classList.remove(OPEN_CLASS);
    document.documentElement.classList.remove(PAGE_LOCK_CLASS);
    applyPanelGeometry();
    updateControls();
    if (terminalHadFocus) {
      iframe.blur();
      toggle.focus();
    }
    saveState();
  }

  function closeTerminal() {
    hideTerminal();
    if (iframe) {
      iframe.remove();
      iframe = null;
    }
  }

  function toggleTerminal() {
    if (terminalOpen) hideTerminal();
    else openTerminal(true);
  }

  // Resizing a bottom-docked panel is the inverse of a top drop-down: dragging its upper edge
  // upward makes it taller, and dragging down until only the tray remains collapses it.
  var drag = {active: false, startY: 0, startHeight: 0};

  function startResize(e) {
    if (!terminalOpen || e.button !== 0) return;
    drag.active = true;
    drag.startY = e.clientY;
    drag.startHeight = terminalHeight;
    resizer.classList.add('ch-webterminal-dragging');

    var overlay = document.createElement('div');
    overlay.id = OVERLAY_ID;
    overlay.style.cssText = 'position:fixed;inset:0;z-index:2147483647;cursor:row-resize;';
    document.body.appendChild(overlay);

    document.addEventListener('pointermove', moveResize);
    document.addEventListener('pointerup', stopResize);
    document.addEventListener('pointercancel', stopResize);
  }

  function moveResize(e) {
    if (!drag.active) return;
    var newHeight = drag.startHeight + (drag.startY - e.clientY);
    if (newHeight < MIN_TERMINAL_HEIGHT) {
      terminalHeight = drag.startHeight;
      stopResize();
      hideTerminal();
      return;
    }
    terminalHeight = Math.min(newHeight, maxTerminalHeight());
    applyPanelGeometry();
  }

  function stopResize() {
    drag.active = false;
    if (resizer) resizer.classList.remove('ch-webterminal-dragging');
    var overlay = document.getElementById(OVERLAY_ID);
    if (overlay) overlay.remove();
    saveState();
    document.removeEventListener('pointermove', moveResize);
    document.removeEventListener('pointerup', stopResize);
    document.removeEventListener('pointercancel', stopResize);
  }

  function isTyping(target) {
    if (!target) return false;
    var tag = target.tagName;
    return tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT' || target.isContentEditable;
  }

  function onKeyDown(e) {
    if ((e.key === '`' || e.key === '~') && !e.ctrlKey && !e.altKey && !e.metaKey) {
      if (isTyping(e.target)) return;
      e.preventDefault();
      toggleTerminal();
      return;
    }
    if (e.key === 'Escape' && terminalOpen && !isTyping(e.target)
        && (document.activeElement === null || document.activeElement === document.body)) {
      hideTerminal();
    }
  }

  function onMessage(e) {
    if (!iframe || e.source !== iframe.contentWindow || e.origin !== TERMINAL_ORIGIN || !e.data) return;
    if (e.data.type === 'webterminal-escape') hideTerminal();
    else if (e.data.type === 'webterminal-closed') closeTerminal();
  }

  function init() {
    createPanel();
    document.addEventListener('keydown', onKeyDown);
    window.addEventListener('message', onMessage);
    window.addEventListener('resize', function () {
      if (window.innerWidth < DESKTOP_MIN_WIDTH) hideTerminal();
      else applyPanelGeometry();
    });

    var restored = savedHeight();
    if (restored !== null && window.innerWidth >= DESKTOP_MIN_WIDTH) {
      terminalHeight = restored;
      openTerminal(false);
    }
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
