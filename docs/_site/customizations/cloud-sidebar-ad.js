(function () {
  'use strict';

  var AD_SLOT_ID = 'ch-cloud-sidebar-ad-slot';
  var DISMISSED_KEY = 'ch-cloud-sidebar-ad-dismissed';
  var MCP_LINK_PATH = '/set-up-clickhouse-documentation-mcp-server';
  var SIGNUP_HREF = 'https://clickhouse.cloud/signUp?loc=docs-card-banner';
  var dismissedForPage = false;

  function isGetStartedPage() {
    var path = window.location.pathname.replace(/^\/docs(?=\/|$)/, '');
    return /^\/(?:(?:ar|es|fr|ja|ko|pt-BR|ru|zh)\/)?get-started(?:\/|$)/.test(path);
  }

  function storageGet(key) {
    try {
      return window.localStorage.getItem(key);
    } catch (e) {
      return null;
    }
  }

  function storageSet(key, value) {
    try {
      window.localStorage.setItem(key, value);
    } catch (e) { /* Dismiss for the current page even if storage is unavailable. */ }
  }

  function track(eventName, href) {
    if (!window.galaxy || typeof window.galaxy.track !== 'function') return;
    window.galaxy.track(eventName, {
      interaction: 'click',
      href: href,
    });
  }

  function findMcpControl() {
    var controls = document.querySelectorAll('#table-of-contents a[href], #table-of-contents button');
    for (var i = 0; i < controls.length; i++) {
      var href = controls[i].getAttribute('href') || '';
      var text = controls[i].textContent || '';
      if (href.indexOf(MCP_LINK_PATH) !== -1 || text.indexOf('ClickHouse documentation MCP server') !== -1) {
        return controls[i];
      }
    }
    return null;
  }

  function createAdSlot(tagName) {
    var slot = document.createElement(tagName);
    slot.id = AD_SLOT_ID;
    slot.className = 'ch-cloud-sidebar-ad-slot';

    var card = document.createElement('aside');
    card.className = 'ch-cloud-sidebar-ad';
    card.setAttribute('aria-label', 'ClickHouse Cloud');

    var dismissButton = document.createElement('button');
    dismissButton.className = 'ch-cloud-sidebar-ad-dismiss';
    dismissButton.type = 'button';
    dismissButton.setAttribute('aria-label', 'Dismiss ClickHouse Cloud advert permanently');
    dismissButton.textContent = '\u00d7';
    dismissButton.onclick = function () {
      dismissedForPage = true;
      storageSet(DISMISSED_KEY, 'true');
      track('docs.sidebarCloudAdvert.advertDismissed', SIGNUP_HREF);
      slot.remove();
    };

    var title = document.createElement('p');
    title.className = 'ch-cloud-sidebar-ad-title';
    title.textContent = 'Try ClickHouse Cloud for FREE';

    var description = document.createElement('p');
    description.className = 'ch-cloud-sidebar-ad-description';
    description.textContent = 'Separation of storage and compute, automatic scaling, built-in SQL console, and lots more. $300 in free credits when signing up.';

    var link = document.createElement('a');
    link.className = 'ch-cloud-sidebar-ad-link';
    link.href = SIGNUP_HREF;
    link.target = '_blank';
    link.rel = 'noopener noreferrer';
    link.textContent = 'Try it for Free';
    link.onclick = function () {
      track('docs.sidebarCloudAdvert.clickedThrough', SIGNUP_HREF);
    };

    card.appendChild(dismissButton);
    card.appendChild(title);
    card.appendChild(description);
    card.appendChild(link);
    slot.appendChild(card);
    return slot;
  }

  function injectAd() {
    var existing = document.getElementById(AD_SLOT_ID);
    if (!isGetStartedPage()) {
      if (existing) existing.remove();
      return true;
    }
    if (dismissedForPage || storageGet(DISMISSED_KEY) === 'true') {
      if (existing) existing.remove();
      return true;
    }
    if (existing) return true;

    var mcpControl = findMcpControl();
    if (!mcpControl) return false;

    var placement = mcpControl.closest('li') || mcpControl;
    var slotTagName = placement.tagName === 'LI' ? 'li' : 'div';
    placement.insertAdjacentElement('afterend', createAdSlot(slotTagName));
    return true;
  }

  function init() {
    injectAd();

    var scheduled = false;
    new MutationObserver(function () {
      if (scheduled) return;
      scheduled = true;
      requestAnimationFrame(function () {
        scheduled = false;
        injectAd();
      });
    }).observe(document.documentElement, { childList: true, subtree: true });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
