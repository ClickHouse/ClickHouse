(function () {
  'use strict';

  // A drop-down ("Quake style") web terminal, matching the one on clickhouse.com and in the
  // `/play` Web SQL UI. It embeds the ClickHouse web terminal (`/webterminal`) of the public
  // playground in an iframe that slides down from the top of the page.
  //
  // `user=play` selects the playground's read-only demo user, which has no password, so the
  // terminal connects without prompting for credentials. This is the same endpoint that the
  // terminal on clickhouse.com connects to.
  var TERMINAL_URL = 'https://play.clickhouse.com/webterminal?user=play';
  var TERMINAL_ORIGIN = 'https://play.clickhouse.com';

  var PANEL_ID = 'ch-webterminal-panel';
  var IFRAME_ID = 'ch-webterminal-iframe';
  var RESIZER_ID = 'ch-webterminal-resizer';
  var OVERLAY_ID = 'ch-webterminal-overlay';
  var ICON_ID = 'ch-webterminal-icon';
  var STYLE_ID = 'ch-webterminal-styles';
  var ACTIVE_CLASS = 'ch-webterminal-active';
  var STATE_KEY = 'ch-webterminal-height';

  // Fraction of the viewport height the panel takes when it is opened for the first time, and
  // the height below which a resize drag closes the panel instead of leaving a sliver on screen.
  var DEFAULT_HEIGHT_RATIO = 0.4;
  var MIN_HEIGHT = 60;
  // Kept below the panel's own height so the terminal never covers the whole viewport.
  var BOTTOM_MARGIN = 40;
  // Tailwind's `lg` breakpoint, below which the theme hides the desktop tab bar — the same check
  // `tab-nav.js` makes. The icon lives in that bar, so the terminal is a desktop-only feature.
  var DESKTOP_MIN_WIDTH = 1024;

  var panel = null;
  var iframe = null;
  var resizer = null;
  var terminalOpen = false;
  // Remembered while the panel is closed so reopening restores the height the user dragged to.
  var panelHeight = null;

  // Same glyph as the terminal icon in `/play` (`programs/server/play.html`).
  var terminalSvg = '<svg viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg" aria-hidden="true" focusable="false">'
    + '<rect x="1" y="1" width="22" height="22" rx="3" fill="none" stroke="currentColor" stroke-width="2"/>'
    + '<path d="M6 8l4 4-4 4" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>'
    + '<line x1="13" y1="16" x2="18" y2="16" stroke="currentColor" stroke-width="2" stroke-linecap="round"/>'
    + '</svg>';

  function injectStyles() {
    if (document.getElementById(STYLE_ID)) return;
    var style = document.createElement('style');
    style.id = STYLE_ID;
    style.textContent = ''
      // Panel: full width, pinned below the navbar, above everything else on the page.
      + '#' + PANEL_ID + ' { position: fixed; left: 0; right: 0; display: none; background: #000;'
      + ' z-index: 2147483646; box-shadow: 0 8px 24px rgba(0, 0, 0, 0.45); }'
      + '#' + PANEL_ID + '.ch-webterminal-open { display: block; }'
      + '#' + IFRAME_ID + ' { display: block; width: 100%; height: 100%; border: none; }'
      // Drag handle along the bottom edge of the panel.
      + '#' + RESIZER_ID + ' { position: absolute; left: 0; right: 0; bottom: 0; height: 8px;'
      + ' cursor: row-resize; user-select: none; border-bottom: 1px solid #3a3a3a;'
      + ' background: linear-gradient(to top, #000, #2b2b2b); }'
      + '#' + RESIZER_ID + ':hover, #' + RESIZER_ID + '.ch-webterminal-dragging'
      + ' { background: linear-gradient(to top, #000, #4d4d4d); }'
      // Navbar icon: sized and coloured like the neighbouring nav tabs.
      + '#' + ICON_ID + ' { display: flex; align-items: center; color: #4b5563; text-decoration: none;'
      + ' transition: color 0.15s; }'
      + '#' + ICON_ID + ' svg { width: 17px; height: 17px; }'
      + '#' + ICON_ID + ':hover { color: #1f2937; }'
      + '.dark #' + ICON_ID + ' { color: #9ca3af; }'
      + '.dark #' + ICON_ID + ':hover { color: #d1d5db; }'
      + '#' + ICON_ID + '.' + ACTIVE_CLASS + ' { color: #1c1c1c; }'
      + '.dark #' + ICON_ID + '.' + ACTIVE_CLASS + ' { color: #fdff75; }';
    document.head.appendChild(style);
  }

  // The panel drops down from just below the top navigation bar, so the terminal icon stays
  // visible and can close the panel again. The navbar is `position: fixed` and its height is a
  // layout detail of the theme, so measure it rather than hardcoding it. Until the navbar is
  // mounted it measures as a zero-height box, and the panel drops from the very top of the
  // viewport; the observer below re-anchors the panel once the navbar appears.
  function panelTop() {
    var navbar = document.getElementById('navbar-transition-maple');
    if (!navbar) return 0;
    var rect = navbar.getBoundingClientRect();
    if (rect.height === 0) return 0;
    return Math.round(rect.bottom);
  }

  function maxPanelHeight() {
    return Math.max(MIN_HEIGHT, window.innerHeight - panelTop() - BOTTOM_MARGIN);
  }

  function applyPanelGeometry() {
    if (!panel) return;
    var top = panelTop();
    panel.style.top = top + 'px';
    panel.style.height = Math.min(panelHeight, maxPanelHeight()) + 'px';
  }

  function postToTerminal(message) {
    if (iframe && iframe.contentWindow) {
      iframe.contentWindow.postMessage(message, TERMINAL_ORIGIN);
    }
  }

  // Client-side navigation keeps the page — and with it the terminal session — alive, but some
  // links (the top-level tabs, and every link that leaves the docs) reload the page. Remember
  // that the terminal was open, and at which height, so it comes back after such a reload. The
  // session itself cannot survive a reload: the WebSocket dies with the page, so the restored
  // terminal is a new session. `sessionStorage` scopes this to the tab it was opened in and
  // throws when storage is unavailable, in which case the terminal simply does not persist.
  function saveState() {
    try {
      if (terminalOpen) sessionStorage.setItem(STATE_KEY, String(panelHeight));
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

  function createPanel() {
    if (panel) return;

    injectStyles();
    panel = document.createElement('div');
    panel.id = PANEL_ID;

    iframe = document.createElement('iframe');
    iframe.id = IFRAME_ID;
    iframe.title = 'ClickHouse web terminal';
    iframe.src = TERMINAL_URL;
    // The terminal learns the embedding origin from this handshake, which is what lets it post
    // `webterminal-escape` and `webterminal-closed` back to us. It only accepts the handshake
    // from `clickhouse.com` and `clickhouse.cloud` origins, so on preview deployments and on
    // localhost the terminal still works but cannot notify us when it is closed from inside.
    iframe.addEventListener('load', function () {
      postToTerminal({type: 'webterminal-hello'});
    });
    panel.appendChild(iframe);

    resizer = document.createElement('div');
    resizer.id = RESIZER_ID;
    resizer.addEventListener('pointerdown', startResize);
    // Prevent the page from scrolling while the handle is dragged on a touch screen.
    resizer.addEventListener('touchstart', function (e) { e.preventDefault(); });
    panel.appendChild(resizer);

    document.body.appendChild(panel);
  }

  // `focusTerminal` is false when the panel is restored after a reload: taking the keyboard away
  // from the page the reader just landed on would be rude, and it would also swallow their first
  // keystrokes into a terminal they did not just ask for.
  function openTerminal(focusTerminal) {
    if (terminalOpen) return;
    createPanel();
    terminalOpen = true;
    if (panelHeight === null) panelHeight = Math.round(window.innerHeight * DEFAULT_HEIGHT_RATIO);
    applyPanelGeometry();
    panel.classList.add('ch-webterminal-open');
    updateIconState();
    saveState();
    // The terminal sizes itself to its container; ask it to refit now that the panel is visible,
    // because while the panel was hidden the iframe had no usable dimensions to measure.
    requestAnimationFrame(function () {
      postToTerminal({type: 'webterminal-refit'});
      if (focusTerminal && iframe) iframe.focus();
    });
  }

  // Hide the panel but keep the iframe alive, so the session and the scrollback survive
  // closing and reopening the terminal.
  function hideTerminal() {
    if (!terminalOpen) return;
    terminalOpen = false;
    panel.classList.remove('ch-webterminal-open');
    updateIconState();
    saveState();
  }

  // Drop the session entirely: used when the terminal itself reports that it disconnected.
  function closeTerminal() {
    hideTerminal();
    if (panel) {
      panel.remove();
      panel = null;
      iframe = null;
      resizer = null;
    }
  }

  function toggleTerminal() {
    if (terminalOpen) hideTerminal();
    // Do not open a panel that the narrow layout gives no way to close: there the tab bar that
    // holds the icon is hidden, and a keyboard shortcut would be the only way back out.
    else if (window.innerWidth >= DESKTOP_MIN_WIDTH) openTerminal(true);
  }

  // ── Resizing ──────────────────────────────────────────────────────────────
  var drag = {active: false, startY: 0, startHeight: 0};

  function startResize(e) {
    if (e.button !== 0) return;
    drag.active = true;
    drag.startY = e.clientY;
    drag.startHeight = panel.offsetHeight;
    resizer.classList.add('ch-webterminal-dragging');

    // The iframe swallows pointer events, so cover the page with a transparent overlay for the
    // duration of the drag; without it the pointer "sticks" as soon as it enters the terminal.
    var overlay = document.createElement('div');
    overlay.id = OVERLAY_ID;
    overlay.style.cssText = 'position:fixed;top:0;left:0;width:100%;height:100%;'
      + 'z-index:2147483647;cursor:row-resize;';
    document.body.appendChild(overlay);

    document.addEventListener('pointermove', moveResize);
    document.addEventListener('pointerup', stopResize);
    document.addEventListener('pointercancel', stopResize);
  }

  function moveResize(e) {
    if (!drag.active) return;
    var newHeight = drag.startHeight + (e.clientY - drag.startY);
    if (newHeight < MIN_HEIGHT) {
      // Dragging the handle up to the top closes the terminal. Restore the height the panel had
      // when the drag started, so that reopening it does not bring back the barely visible
      // sliver the pointer passed through on its way up.
      panelHeight = drag.startHeight;
      stopResize();
      hideTerminal();
      return;
    }
    panelHeight = Math.min(newHeight, maxPanelHeight());
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

  // ── Navbar icon ───────────────────────────────────────────────────────────
  function updateIconState() {
    var icon = document.getElementById(ICON_ID);
    if (!icon) return;
    icon.classList.toggle(ACTIVE_CLASS, terminalOpen);
    icon.setAttribute('aria-expanded', terminalOpen ? 'true' : 'false');
  }

  // The theme renders one `.nav-tabs` element per layout variant and lays out only the active
  // one, so pick the container that is actually on screen. Returns null on narrow viewports,
  // where the desktop tab bar is hidden altogether and the terminal has no place to live.
  function tabsContainer() {
    var candidates = document.querySelectorAll('.nav-tabs');
    for (var i = 0; i < candidates.length; i++) {
      if (candidates[i].getClientRects().length) return candidates[i];
    }
    return null;
  }

  // Placed as the first item of the top navigation, before the "Home" tab. Rendered as an anchor
  // so that a middle-click or Ctrl/Shift+click opens the terminal in a new browser tab natively,
  // the way the terminal icon in `/play` behaves.
  function injectIcon() {
    var tabs = tabsContainer();
    if (!tabs) return;

    var existing = document.getElementById(ICON_ID);
    if (existing) {
      // Re-parent the icon when the theme switches to another tab bar, for example when the
      // viewport grows past the mobile breakpoint and a different `.nav-tabs` takes over.
      if (existing.parentElement !== tabs) tabs.insertBefore(existing, tabs.firstChild);
      return;
    }

    injectStyles();

    var icon = document.createElement('a');
    icon.id = ICON_ID;
    icon.className = 'nav-tabs-item';
    icon.href = TERMINAL_URL;
    icon.title = 'Web terminal (~) — middle-click or Ctrl/Shift+click to open in a new tab';
    icon.setAttribute('aria-label', 'Web terminal');
    icon.setAttribute('aria-expanded', terminalOpen ? 'true' : 'false');
    icon.innerHTML = terminalSvg;

    icon.addEventListener('click', function (e) {
      // Let the browser handle modifier clicks natively: they open the terminal in a new tab.
      if (e.ctrlKey || e.metaKey || e.shiftKey) return;
      e.preventDefault();
      e.stopPropagation();
      toggleTerminal();
    });

    tabs.insertBefore(icon, tabs.firstChild);
    updateIconState();
  }

  // ── Keyboard ──────────────────────────────────────────────────────────────
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
    // Escape closes the terminal, but only when nothing else on the page owns the keyboard:
    // the search modal and other overlays put focus into their own controls and close on Escape
    // themselves. Escape pressed inside the terminal is handled by the terminal, which reports
    // it back through a `webterminal-escape` message.
    if (e.key === 'Escape' && terminalOpen && !isTyping(e.target)
        && (document.activeElement === null || document.activeElement === document.body)) {
      hideTerminal();
    }
  }

  // ── Messages from the terminal ────────────────────────────────────────────
  function onMessage(e) {
    if (!iframe || e.source !== iframe.contentWindow) return;
    if (e.origin !== TERMINAL_ORIGIN) return;
    if (!e.data) return;

    if (e.data.type === 'webterminal-escape') {
      hideTerminal();
    } else if (e.data.type === 'webterminal-closed') {
      // The session ended (for example, the user typed `exit`); drop the iframe so that
      // reopening the terminal starts a fresh session.
      closeTerminal();
    }
  }

  // ── Init ──────────────────────────────────────────────────────────────────
  function init() {
    injectIcon();

    // Mintlify re-renders the navbar on client-side navigation, which drops the injected icon;
    // re-add it whenever that happens. Debounced with a frame so a burst of React updates costs
    // a single pass. The panel is appended to `<body>`, outside the React tree, so it and the
    // terminal session survive navigation untouched.
    var scheduled = false;
    var observer = new MutationObserver(function () {
      if (scheduled) return;
      scheduled = true;
      requestAnimationFrame(function () {
        scheduled = false;
        injectIcon();
        // The navbar mounts and changes height while the page settles (and the terminal may be
        // restored before it exists at all), so keep the panel anchored to it.
        if (terminalOpen) applyPanelGeometry();
      });
    });
    observer.observe(document.documentElement, {childList: true, subtree: true});

    document.addEventListener('keydown', onKeyDown);
    window.addEventListener('message', onMessage);
    window.addEventListener('resize', function () {
      if (!terminalOpen) return;
      // Narrowing the window past the breakpoint takes the tab bar, and with it the icon that
      // closes the terminal, off the screen. Close the panel instead of leaving it stranded on
      // top of the page; the session stays alive, so widening the window and clicking the icon
      // again returns to it.
      if (window.innerWidth < DESKTOP_MIN_WIDTH) hideTerminal();
      else applyPanelGeometry();
    });

    // Reopen the terminal if it was open before a page reload.
    var restored = savedHeight();
    if (restored !== null && window.innerWidth >= DESKTOP_MIN_WIDTH) {
      panelHeight = restored;
      openTerminal(false);
    }
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
