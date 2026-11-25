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
    '/images/icons/icon-postgres.svg',
    '/images/icons/icon-clickstack.svg',
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

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', warmNavIcons, { once: true });
  } else {
    warmNavIcons();
  }
})();
