(function () {
  'use strict';

  function restorePreviousTitle() {
    var link = document.querySelector('#pagination > a[rel="prev"]');
    if (!link || link.querySelector('[data-component-part="pagination-title"]')) return;

    var label = link.querySelector('[data-component-part="pagination-label"]');
    var ariaLabel = link.getAttribute('aria-label');
    if (!label || !ariaLabel) return;

    var prefix = label.textContent.trim() + ':';
    if (!ariaLabel.startsWith(prefix)) return;

    var titleText = ariaLabel.slice(prefix.length).trim();
    if (!titleText) return;

    var title = document.createElement('div');
    title.setAttribute('data-component-part', 'pagination-title');
    title.textContent = titleText;
    link.appendChild(title);
  }

  function init() {
    restorePreviousTitle();

    var observer = new MutationObserver(restorePreviousTitle);
    observer.observe(document.documentElement, { childList: true, subtree: true });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
