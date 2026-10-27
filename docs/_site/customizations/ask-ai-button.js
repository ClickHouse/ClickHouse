(function () {
  'use strict';

  var BTN_ID = 'ch-ask-ai-btn';
  var ROW_ID = 'ch-ask-ai-row';
  var PAGE_ROW_ID = 'ch-page-ask-row';
  var PAGE_BTN_ID = 'ch-page-ask-btn';
  var MOBILE_BTN_ID = 'ch-ask-ai-btn-mobile';

  var sparkleSvg = '<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 18 18"'
    + ' class="ch-ai-icon size-4 shrink-0 text-gray-700">'
    + '<g fill="currentColor">'
    + '<path d="M5.658,2.99l-1.263-.421-.421-1.263c-.137-.408-.812-.408-.949,0l-.421,1.263-1.263,.421c-.204,.068-.342,.259-.342,.474s.138,.406,.342,.474l1.263,.421,.421,1.263c.068,.204,.26,.342,.475,.342s.406-.138,.475-.342l.421-1.263,1.263-.421c.204-.068,.342-.259,.342-.474s-.138-.406-.342-.474Z" fill="currentColor" data-stroke="none" stroke="none"></path>'
    + '<polygon points="9.5 2.75 11.412 7.587 16.25 9.5 11.412 11.413 9.5 16.25 7.587 11.413 2.75 9.5 7.587 7.587 9.5 2.75" fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5"></polygon>'
    + '</g></svg>';

  function trackDocsAi(entryPoint, pageContext) {
    var eventNames = {
      home: 'docs.home.ask-ai',
      sidebar: 'docs.docsai.sidebar',
      'page-title': 'docs.docsai.page',
      troubleshooting: 'docs.docsai.troubleshooting',
      'mobile-header': 'docs.docsai.mobile',
      'keyboard-shortcut': 'docs.docsai.keyboard'
    };
    var properties = {
      entry_point: entryPoint,
      page_context: String(Boolean(pageContext)),
      interaction: 'click'
    };
    if (pageContext) properties.page_path = window.location.pathname;
    var track = function () {
      if (window.galaxy && typeof window.galaxy.track === 'function') {
        window.galaxy.track(eventNames[entryPoint] || 'docs.docsai.open', properties);
      }
    };

    if (window.galaxy && typeof window.galaxy.track === 'function') track();
    else window.addEventListener('galaxy:ready', track, { once: true });
  }

  // Wait briefly for Kapa to mount (it's loaded async), then open. Passing a
  // page query prefills context without submitting a synthetic question.
  function clearKapaQuery() {
    var widgetContainer = document.getElementById('kapa-widget-container');
    var input = widgetContainer && widgetContainer.shadowRoot
      && widgetContainer.shadowRoot.getElementById('kapa-ask-ai-input');
    if (!input) return false;

    // Kapa retains its prior draft when it is opened without a query. Update
    // the native value and emit an input event so React also clears its state.
    var setter = Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, 'value').set;
    setter.call(input, '');
    input.dispatchEvent(new Event('input', { bubbles: true, composed: true }));
    return true;
  }

  function focusKapaQueryEnd() {
    var widgetContainer = document.getElementById('kapa-widget-container');
    var input = widgetContainer && widgetContainer.shadowRoot
      && widgetContainer.shadowRoot.getElementById('kapa-ask-ai-input');
    if (!input) return false;

    input.focus({ preventScroll: true });
    var end = input.value.length;
    input.setSelectionRange(end, end);
    return true;
  }

  function openKapa(query, submit) {
    var opts = { mode: 'ai' };
    if (query) {
      opts.query = query;
      opts.submit = Boolean(submit);
    }
    var attempts = 0;
    var iv = setInterval(function () {
      attempts++;
      if (window.Kapa && typeof window.Kapa.open === 'function') {
        clearInterval(iv);
        window.Kapa.open(opts);
        var inputAttempts = 0;
        var prepareInput = setInterval(function () {
          inputAttempts++;
          var complete = query ? focusKapaQueryEnd() : clearKapaQuery();
          if (complete || inputAttempts > 20) clearInterval(prepareInput);
        }, 50);
      } else if (attempts > 60) {
        clearInterval(iv);
      }
    }, 50);
  }

  function pageContextQuery(title) {
    return 'I am reading “' + title + '” (' + window.location.href
      + '). Use this page as context for my question:\n\n';
  }

  function openDocsAi(entryPoint, pageTitle) {
    trackDocsAi(entryPoint, Boolean(pageTitle));
    openKapa(pageTitle ? pageContextQuery(pageTitle) : undefined, false);
  }

  function bindPointerFocusBehavior(btn) {
    // Kapa restores focus to its trigger after closing. Preserve a focus
    // indicator for keyboard users, but do not show it after a pointer click.
    btn.addEventListener('pointerdown', function () {
      btn.dataset.chAskAiPointerTrigger = 'true';
    });
    document.addEventListener('keydown', function (event) {
      if (event.key === 'Tab') delete btn.dataset.chAskAiPointerTrigger;
    });
  }

  function makeButton(id, label, entryPoint, pageTitle) {
    var btn = document.createElement('button');
    btn.id = id;
    btn.type = 'button';
    btn.className = 'ch-ask-ai-button';
    btn.setAttribute('aria-label', pageTitle ? 'Ask AI about ' + pageTitle : 'Ask AI');
    btn.setAttribute('aria-keyshortcuts', 'Meta+I');
    btn.innerHTML = sparkleSvg + '<span class="ch-ask-ai-label">' + label + '</span>';
    bindPointerFocusBehavior(btn);
    btn.addEventListener('click', function (e) {
      e.stopPropagation();
      openDocsAi(entryPoint, pageTitle);
    });
    return btn;
  }

  function bindTaggedEntries() {
    if (document.documentElement.dataset.chAskAiEntriesBound) return;
    document.documentElement.dataset.chAskAiEntriesBound = 'true';
    document.addEventListener('click', function (event) {
      var target = event.target;
      var entry = target && typeof target.closest === 'function'
        && target.closest('[data-docs-ai-entry]');
      if (!entry) return;

      // These entries live in MDX, including localized troubleshooting pages.
      // Intercept their legacy Kapa handler so they use the shared flow.
      event.preventDefault();
      event.stopImmediatePropagation();
      openDocsAi(entry.getAttribute('data-docs-ai-entry'));
    }, true);
  }

  function injectSidebarButton() {
    if (document.getElementById(BTN_ID)) return true;

    var searchBar = document.getElementById('search-bar-entry');
    if (!searchBar) return false;

    var host = searchBar.closest('div.flex.flex-col.gap-4.mt-6') || searchBar.parentNode;
    if (!host) return false;

    var row = document.createElement('div');
    row.id = ROW_ID;
    row.appendChild(makeButton(BTN_ID, 'Ask AI <small>· docs assistant</small><kbd>⌘I</kbd>', 'sidebar', ''));
    host.appendChild(row);
    return true;
  }

  function injectPageButton() {
    // The homepage has its own full-width assistant entry point. The page
    // action is meaningful only when a documentation page supplies context.
    if (window.location.pathname === '/' || document.getElementById('home-assistant-entry')) return true;

    var main = document.querySelector('main');
    var title = main && main.querySelector('h1');
    if (!title) return false;
    if (title.parentNode && title.parentNode.id === PAGE_ROW_ID) return true;

    var row = document.createElement('div');
    row.id = PAGE_ROW_ID;
    title.parentNode.insertBefore(row, title);
    row.appendChild(title);
    row.appendChild(makeButton(PAGE_BTN_ID, 'Ask about this page', 'page-title', title.textContent.trim()));
    return true;
  }

  function enhanceHomeButton() {
    var homeButton = document.getElementById('home-assistant-entry');
    if (!homeButton || homeButton.dataset.chAskAiEnhanced) return Boolean(homeButton);

    // The homepage has a purpose-built hero CTA, rather than a conventional
    // document heading. Keep its existing appearance and context-free
    // behavior while using the shared analytics and Kapa flow.
    homeButton.dataset.chAskAiEnhanced = 'true';
    homeButton.removeAttribute('data-galaxy-event');
    bindPointerFocusBehavior(homeButton);
    homeButton.addEventListener('click', function (event) {
      event.preventDefault();
      event.stopPropagation();
      openDocsAi('home');
    });
    return true;
  }

  function injectMobileButton() {
    var mobileButton = document.getElementById(MOBILE_BTN_ID);
    if (document.getElementById(PAGE_BTN_ID)) {
      if (mobileButton) mobileButton.remove();
      return true;
    }
    if (mobileButton) return true;

    var mobileSearchButton = document.getElementById('search-bar-entry-mobile');
    if (!mobileSearchButton || !mobileSearchButton.parentNode) return false;

    mobileButton = makeButton(MOBILE_BTN_ID, '', 'mobile-header', '');
    mobileButton.classList.add('ch-ask-ai-mobile-button');
    mobileSearchButton.parentNode.insertBefore(mobileButton, mobileSearchButton.nextSibling);
    return true;
  }

  function bindShortcut() {
    if (document.documentElement.dataset.chAskAiShortcutBound) return;
    document.documentElement.dataset.chAskAiShortcutBound = 'true';
    document.addEventListener('keydown', function (event) {
      var hasShortcutModifier = (event.metaKey || event.ctrlKey) && !(event.metaKey && event.ctrlKey);
      var target = event.target;
      var isTyping = target && (target.tagName === 'INPUT' || target.tagName === 'TEXTAREA'
        || target.tagName === 'SELECT' || target.isContentEditable);
      if (event.defaultPrevented || event.altKey || event.shiftKey || !hasShortcutModifier
        || event.key.toLowerCase() !== 'i' || isTyping) return;
      event.preventDefault();
      openDocsAi('keyboard-shortcut');
    });
  }

  function init() {
    injectSidebarButton();
    injectPageButton();
    enhanceHomeButton();
    injectMobileButton();
    bindTaggedEntries();
    bindShortcut();

    var observer = new MutationObserver(function () {
      injectSidebarButton();
      injectPageButton();
      enhanceHomeButton();
      injectMobileButton();
    });
    observer.observe(document.documentElement, { childList: true, subtree: true });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
