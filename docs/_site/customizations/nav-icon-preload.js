(function () {
  'use strict';

  // Mintlify mounts desktop dropdown menus on demand, so their image assets are
  // otherwise discovered only when a user first opens a menu. Warm the small
  // SVGs at low priority as soon as the document is ready so every navbar
  // dropdown is prepared for its first interaction.
  var NAV_ICON_PATHS = [
    '/images/icons/icon-get-started.svg',
    '/images/icons/icon-concepts.svg',
    '/images/icons/icon-guides.svg',
    '/images/icons/icon-reference.svg',
    '/images/icons/icon-clickhouse-cloud.svg',
    '/images/icons/icon-clickhouse-observability.svg',
    '/images/icons/icon-clickhouse-observability-light.svg',
    '/images/icons/icon-postgres.svg',
    '/images/icons/icon-clickstack.svg',
    '/images/clickstack/clickstack-icon-dark.svg',
    '/images/clickstack/clickstack-icon-light.svg',
    '/images/icons/logo-langfuse.svg',
    '/images/icons/icon-agentic-data-stack.svg',
    '/images/icons/icon-chdb.svg',
    '/images/icons/icon-kubernetes-operator.svg',
    '/images/icons/icon-clickpipes.svg',
    '/images/icons/icon-language-clients.svg',
    '/images/icons/icon-connectors.svg',
    '/images/icons/icon-support-center.svg',
    '/images/icons/icon-contribute.svg',
    '/images/icons/icon-changelogs.svg',
    '/images/icons/icon-about.svg',
  ];

  var BASE = /^\/docs(\/|$)/.test(window.location.pathname) ? '/docs' : '';

  function warmNavIcons() {
    // Retain the Image objects for the page session. Besides keeping the SVGs
    // decoded, this avoids a second validation request in environments such as
    // `mint dev`, which serves static files with Cache-Control: max-age=0.
    window.__chNavIconCache = NAV_ICON_PATHS.map(function (path) {
      var image = new Image();
      image.fetchPriority = 'low';
      image.decoding = 'async';
      image.src = BASE + path;
      return image;
    });
  }

  // Nav assets are the dark-mode marks (white ClickHouse logo, white ClickStack
  // hexagon). On a light sidebar, point the img at the ink files by changing
  // `src`. A CSS `content: url()` swap paints a gray backdrop through the
  // transparent parts of the logo.
  var THEME_SWAPS = [
    ['icon-clickhouse-observability.svg', 'icon-clickhouse-observability-light.svg'],
    ['clickstack-icon-dark.svg', 'clickstack-icon-light.svg']
  ];

  function iconSrcHas(src, file) {
    return src.indexOf(file) !== -1;
  }

  function applyNavIconTheme(img) {
    var src = img.getAttribute('src');
    if (!src) return;
    var light = !document.documentElement.classList.contains('dark');
    for (var i = 0; i < THEME_SWAPS.length; i++) {
      var darkFile = THEME_SWAPS[i][0];
      var lightFile = THEME_SWAPS[i][1];
      if (light && iconSrcHas(src, darkFile) && !iconSrcHas(src, lightFile)) {
        img.setAttribute('src', src.replace(darkFile, lightFile));
        return;
      }
      if (!light && iconSrcHas(src, lightFile)) {
        img.setAttribute('src', src.replace(lightFile, darkFile));
        return;
      }
    }
  }

  function scanNavIcons(root) {
    if (!root || root.nodeType !== 1) return;
    if (root.tagName === 'IMG') applyNavIconTheme(root);
    if (!root.querySelectorAll) return;
    var imgs = root.querySelectorAll('img');
    for (var i = 0; i < imgs.length; i++) applyNavIconTheme(imgs[i]);
  }

  function watchNavIconTheme() {
    scanNavIcons(document.documentElement);
    new MutationObserver(function (mutations) {
      for (var i = 0; i < mutations.length; i++) {
        var mutation = mutations[i];
        if (mutation.type === 'attributes') {
          if (mutation.target === document.documentElement) scanNavIcons(document.documentElement);
          else if (mutation.target.tagName === 'IMG') applyNavIconTheme(mutation.target);
          continue;
        }
        for (var j = 0; j < mutation.addedNodes.length; j++) scanNavIcons(mutation.addedNodes[j]);
      }
    }).observe(document.documentElement, {
      subtree: true,
      childList: true,
      attributes: true,
      attributeFilter: ['src', 'class']
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', warmNavIcons, { once: true });
  } else {
    warmNavIcons();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', watchNavIconTheme, { once: true });
  } else {
    watchNavIconTheme();
  }
})();
