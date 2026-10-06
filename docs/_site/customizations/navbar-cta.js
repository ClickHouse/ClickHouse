(function () {
  'use strict';

  var CTA_ID = 'ch-navbar-cta';
  var CTA_HREF = 'https://clickhouse.cloud/signUp?loc=docs-nav-signUp-cta';

  var githubSvg = '<svg width="16" height="16" viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">'
    + '<path fill-rule="evenodd" clip-rule="evenodd" d="M8 1.75C4.27 1.75 1.25 4.77 1.25 8.5c0 2.99 1.93 5.51 4.62 6.4.34.06.46-.14.46-.32 0-.16-.01-.69-.01-1.26-1.7.32-2.14-.6-2.27-.98-.08-.19-.41-.79-.7-.95-.24-.13-.58-.44-.01-.45.53-.01.91.49 1.04.69.61 1.02 1.58.73 1.97.56.06-.44.24-.74.43-.91-1.5-.17-3.07-.75-3.07-3.33 0-.73.26-1.34.69-1.81-.07-.17-.3-.86.07-1.79 0 0 .57-.18 1.86.69.54-.15 1.11-.23 1.69-.23.57 0 1.14.08 1.68.23 1.29-.88 1.86-.69 1.86-.69.37.93.14 1.62.07 1.79.43.47.69 1.07.69 1.81 0 2.59-1.58 3.16-3.08 3.33.25.21.46.62.46 1.25 0 .9-.01 1.63-.01 1.86 0 .18.13.39.47.32 2.67-.9 4.58-3.41 4.58-6.4 0-3.73-3.02-6.75-6.75-6.75Z" fill="currentColor"/>'
    + '</svg>';

  function formatStars(count) {
    if (count >= 1000) return (count / 1000).toFixed(1).replace(/\.0$/, '') + 'k';
    return String(count);
  }

  function injectStyles() {
    if (document.getElementById('ch-navbar-cta-styles')) return;
    var style = document.createElement('style');
    style.id = 'ch-navbar-cta-styles';
    style.textContent = ''
      // Hide mobile AI assistant button
      + '#assistant-entry-mobile { display: none !important; }'
      // Invert dark SVG logos so they're visible on dark backgrounds
      + '.dark img[src*="windsurf"], :is(.dark) img[src*="windsurf"] { filter: invert(1) !important; }'
      // CTA container
      + '#' + CTA_ID + ' { display: flex; align-items: center; gap: 16px; flex-shrink: 0; margin-left: 32px; }'
      // GitHub stars link
      + '#' + CTA_ID + ' .ch-gh-stars { display: inline-flex; align-items: center; gap: 6px; font-size: 13px; font-weight: 500; text-decoration: none; white-space: nowrap; transition: color 0.15s; }'
      + '#' + CTA_ID + ' .ch-gh-stars svg { flex-shrink: 0; }'
      // Get started button
      + '#' + CTA_ID + ' .ch-cta-btn { display: inline-flex; align-items: center; padding: 6px 16px; border-radius: 4px; font-size: 13px; font-weight: 600; text-decoration: none; white-space: nowrap; transition: background-color 0.15s, color 0.15s; }'
      // Light mode
      + '#' + CTA_ID + ' .ch-gh-stars { color: #374151; }'
      + '#' + CTA_ID + ' .ch-gh-stars:hover { color: #111; }'
      + '#' + CTA_ID + ' .ch-cta-btn { background: #1c1c1c; color: #fff; }'
      + '#' + CTA_ID + ' .ch-cta-btn:hover { background: #333; }'
      // Dark mode
      + '.dark #' + CTA_ID + ' .ch-gh-stars { color: #d1d5db; }'
      + '.dark #' + CTA_ID + ' .ch-gh-stars:hover { color: #fff; }'
      + '.dark #' + CTA_ID + ' .ch-cta-btn { background: #fdff75; color: #1c1c1c; }'
      + '.dark #' + CTA_ID + ' .ch-cta-btn:hover { background: #eaec6a; }';
    document.head.appendChild(style);
  }

  function injectCta() {
    if (document.getElementById(CTA_ID)) return true;

    var mapleNav = document.getElementById('navbar-transition-maple');
    if (!mapleNav) return false;

    injectStyles();

    // --- Right section: GitHub stars + Get started ---
    var container = document.createElement('div');
    container.id = CTA_ID;

    var ghLink = document.createElement('a');
    ghLink.className = 'ch-gh-stars';
    ghLink.href = 'https://github.com/ClickHouse/ClickHouse';
    ghLink.target = '_blank';
    ghLink.rel = 'noopener noreferrer';
    ghLink.innerHTML = githubSvg + '<span class="ch-gh-count"></span>';
    container.appendChild(ghLink);

    var ctaLink = document.createElement('a');
    ctaLink.className = 'ch-cta-btn';
    ctaLink.href = CTA_HREF;
    ctaLink.target = '_blank';
    ctaLink.rel = 'noopener noreferrer';
    ctaLink.textContent = 'Get Started';
    ctaLink.onclick = function () {
      if (window.galaxy && typeof window.galaxy.track === 'function') {
        window.galaxy.track('docs.navbar.get-started', {
          interaction: 'click',
          href: CTA_HREF,
        });
      }
    };
    container.appendChild(ctaLink);

    mapleNav.appendChild(container);

    fetchStarCount(ghLink.querySelector('.ch-gh-count'));

    return true;
  }

  function fetchStarCount(el) {
    if (!el) return;
    el.textContent = '44.4k';

    try {
      var xhr = new XMLHttpRequest();
      xhr.open('GET', 'https://api.github.com/repos/ClickHouse/ClickHouse', true);
      xhr.timeout = 5000;
      xhr.onload = function () {
        if (xhr.status === 200) {
          try {
            var data = JSON.parse(xhr.responseText);
            if (data.stargazers_count) {
              el.textContent = formatStars(data.stargazers_count);
            }
          } catch (e) { /* keep fallback */ }
        }
      };
      xhr.send();
    } catch (e) { /* keep fallback */ }
  }

  function init() {
    injectCta();

    var observer = new MutationObserver(function () {
      injectCta();
    });
    observer.observe(document.documentElement, { childList: true, subtree: true });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
