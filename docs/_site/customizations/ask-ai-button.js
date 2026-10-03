(function () {
  'use strict';

  var BTN_ID = 'ch-ask-ai-btn';
  var MOBILE_BTN_ID = 'ch-ask-ai-btn-mobile';

  var sparkleSvg = '<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 18 18"'
    + ' class="ch-ai-icon size-4 shrink-0 text-gray-700">'
    + '<g fill="currentColor">'
    + '<path d="M5.658,2.99l-1.263-.421-.421-1.263c-.137-.408-.812-.408-.949,0l-.421,1.263-1.263,.421c-.204,.068-.342,.259-.342,.474s.138,.406,.342,.474l1.263,.421,.421,1.263c.068,.204,.26,.342,.475,.342s.406-.138,.475-.342l.421-1.263,1.263-.421c.204-.068,.342-.259,.342-.474s-.138-.406-.342-.474Z" fill="currentColor" data-stroke="none" stroke="none"></path>'
    + '<polygon points="9.5 2.75 11.412 7.587 16.25 9.5 11.412 11.413 9.5 16.25 7.587 11.413 2.75 9.5 7.587 7.587 9.5 2.75" fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5"></polygon>'
    + '</g></svg>';

  function injectStyles() {
    if (document.getElementById('ch-ask-ai-styles')) return;
    var style = document.createElement('style');
    style.id = 'ch-ask-ai-styles';
    style.textContent = '.dark .ch-ai-icon { color: #fdff75; }';
    document.head.appendChild(style);
  }

  // Wait briefly for Kapa to mount (it's loaded async), then open. Kapa's
  // open() accepts an optional query that's submitted immediately when
  // submit:true is set.
  function openKapa(query) {
    var opts = { mode: 'ai' };
    if (query) {
      opts.query = query;
      opts.submit = true;
    }
    var attempts = 0;
    var iv = setInterval(function () {
      attempts++;
      if (window.Kapa && typeof window.Kapa.open === 'function') {
        clearInterval(iv);
        window.Kapa.open(opts);
      } else if (attempts > 60) {
        clearInterval(iv);
      }
    }, 50);
  }

  function injectButton() {
    if (document.getElementById(BTN_ID)) return true;

    var searchBar = document.getElementById('search-bar-entry');

    if (!searchBar) return false;

    injectStyles();

    var btn = document.createElement('button');
    btn.id = BTN_ID;
    btn.type = 'button';
    btn.className = 'flex-none hidden lg:flex items-center justify-center gap-1.5 pl-3 pr-3.5 h-9 shadow-none bg-gray-950/[0.03] dark:bg-white/[0.03] hover:bg-gray-950/10 dark:hover:bg-white/10 border border-white/10 rounded ml-2';
    btn.setAttribute('aria-label', 'Ask AI');
    btn.innerHTML = sparkleSvg
      + '<span class="text-sm text-gray-500 dark:text-white/50 whitespace-nowrap">Ask</span>';
    btn.addEventListener('click', function (e) {
      e.stopPropagation();
      openKapa();
    });

    searchBar.parentNode.insertBefore(btn, searchBar.nextSibling);
    return true;
  }

  function injectMobileButton() {
    if (document.getElementById(MOBILE_BTN_ID)) return true;

    var mobileSearchBtn = document.getElementById('search-bar-entry-mobile');

    if (!mobileSearchBtn) return false;

    injectStyles();

    var btn = document.createElement('button');
    btn.id = MOBILE_BTN_ID;
    btn.type = 'button';
    btn.className = 'text-gray-500 w-8 h-8 flex items-center justify-center hover:text-gray-600 dark:text-gray-400 dark:hover:text-gray-300';
    btn.setAttribute('aria-label', 'Ask AI');
    btn.innerHTML = sparkleSvg;
    btn.addEventListener('click', function (e) {
      e.stopPropagation();
      openKapa();
    });

    mobileSearchBtn.parentNode.insertBefore(btn, mobileSearchBtn.nextSibling);
    return true;
  }

  function init() {
    injectButton();
    injectMobileButton();

    var observer = new MutationObserver(function () {
      injectButton();
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