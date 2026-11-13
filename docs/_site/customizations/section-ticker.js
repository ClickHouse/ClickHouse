(function () {
  'use strict';

  // ── Configuration ─────────────────────────────────────────────────────────
  // Section tickers: a slim announcement strip shown directly beneath the top
  // navbar on every page of a documentation section, and nowhere else.
  //
  // One entry per ticker:
  //   id          Stable identifier. Used for analytics and to namespace the
  //               dismissal key, so a ticker for a different section never
  //               inherits another's dismissed state.
  //   enabled     Set to false to switch the ticker off without deleting it.
  //   paths       Section roots, matched against the page path with the /docs
  //               base and the locale prefix removed: '/clickstack' matches
  //               /clickstack, /clickstack/overview and /ja/clickstack/faq.
  //   content     Markdown copy. Supported: [text](url), **bold**, *italic*,
  //               `code`. Either a string, or an object keyed by locale with
  //               `en` as the fallback, e.g. { en: '…', ja: '…' }. Links that
  //               start with `/` are docs paths and stay within the current
  //               locale; links to another host open in a new tab.
  //   dismissible Render a close button. Dismissal is remembered per
  //               `id` + `content`, so changing the copy shows the ticker
  //               again to readers who dismissed the previous copy.
  //   dismissal   'session' hides it for the rest of the browser session (the
  //               behavior of the clickhouse.com announcement bar);
  //               'permanent' hides it until the copy changes.
  var TICKERS = [
    {
      id: 'clickstack',
      enabled: true,
      paths: ['/clickstack'],
      content: 'ClickStack Cloud is coming: turnkey observability, powered by ClickHouse. [Join the waitlist](https://clickhouse.com/cloud/clickstack-cloud-waitlist-turnkey?loc=docs-clickstack-ticker)',
      dismissible: true,
      dismissal: 'session',
    },
  ];
  // ── End of configuration ──────────────────────────────────────────────────

  var TICKER_ID = 'ch-section-ticker';
  var STYLE_ID = 'ch-section-ticker-styles';
  var ACTIVE_CLASS = 'ch-section-ticker-active';
  var HEIGHT_VAR = '--ch-section-ticker-height';
  var STORAGE_PREFIX = 'ch-section-ticker:';
  var LOCALES = ['ar', 'es', 'fr', 'ja', 'ko', 'pt-BR', 'ru', 'zh'];
  var DISMISS_LABELS = {
    en: 'Dismiss announcement',
    ar: 'إغلاق الإعلان',
    es: 'Descartar el anuncio',
    fr: 'Fermer l’annonce',
    ja: 'お知らせを閉じる',
    ko: '공지 닫기',
    'pt-BR': 'Dispensar o anúncio',
    ru: 'Скрыть объявление',
    zh: '关闭公告',
  };

  // '' at root (.mintlify.app previews, mint dev); '/docs' on the subpath deploy.
  var BASE = /^\/docs(\/|$)/.test(window.location.pathname) ? '/docs' : '';
  var dismissedInMemory = {};

  function stripBase(path) {
    return (BASE && path.indexOf(BASE) === 0) ? (path.slice(BASE.length) || '/') : path;
  }

  function currentLocale() {
    var segment = stripBase(window.location.pathname).split('/')[1] || '';
    return LOCALES.indexOf(segment) !== -1 ? segment : '';
  }

  // Page path without the base path, locale prefix, or trailing slash.
  function sectionPath() {
    var path = stripBase(window.location.pathname);
    var locale = currentLocale();
    if (locale) path = path.slice(locale.length + 1) || '/';
    return path.replace(/\/+$/, '') || '/';
  }

  function localizeUrl(url) {
    var locale = currentLocale();
    var firstSegment = url.split('/')[1] || '';
    if (!locale || LOCALES.indexOf(firstSegment) !== -1) return BASE + url;
    return BASE + '/' + locale + url;
  }

  function matchesPath(ticker) {
    var path = sectionPath();
    var roots = ticker.paths || [];
    for (var i = 0; i < roots.length; i++) {
      var root = String(roots[i]).replace(/\/+$/, '');
      if (root && (path === root || path.indexOf(root + '/') === 0)) return true;
    }
    return false;
  }

  function resolveContent(ticker) {
    var content = ticker.content;
    if (typeof content === 'string') return content;
    if (content && typeof content === 'object') {
      return content[currentLocale()] || content.en || '';
    }
    return '';
  }

  function hash(str) {
    var h = 5381;
    for (var i = 0; i < str.length; i++) h = ((h << 5) + h + str.charCodeAt(i)) | 0;
    return (h >>> 0).toString(36);
  }

  function dismissalKey(ticker) {
    return STORAGE_PREFIX + ticker.id + ':' + hash(JSON.stringify(ticker.content));
  }

  function storage(ticker) {
    return ticker.dismissal === 'permanent' ? window.localStorage : window.sessionStorage;
  }

  function isDismissed(ticker) {
    var key = dismissalKey(ticker);
    if (dismissedInMemory[key]) return true;
    try {
      return storage(ticker).getItem(key) !== null;
    } catch (e) {
      return false;
    }
  }

  function dismiss(ticker) {
    var key = dismissalKey(ticker);
    dismissedInMemory[key] = true;
    try {
      storage(ticker).setItem(key, String(Date.now()));
    } catch (e) { /* Dismiss for the current page even if storage is unavailable. */ }
  }

  function activeTicker() {
    for (var i = 0; i < TICKERS.length; i++) {
      var ticker = TICKERS[i];
      if (ticker.enabled === false) continue;
      if (!matchesPath(ticker)) continue;
      if (!resolveContent(ticker)) continue;
      if (ticker.dismissible && isDismissed(ticker)) continue;
      return ticker;
    }
    return null;
  }

  function track(eventName, ticker, href) {
    if (!window.galaxy || typeof window.galaxy.track !== 'function') return;
    window.galaxy.track(eventName, {
      interaction: 'click',
      ticker: ticker.id,
      href: href || null,
    });
  }

  // ── Markdown ──────────────────────────────────────────────────────────────
  // Renders the supported inline subset into DOM nodes. Text always goes
  // through textContent, so the copy can never inject markup.
  var INLINE_PATTERN = /\[([^\]]+)\]\(([^)\s]+)\)|\*\*([^*]+)\*\*|\*([^*]+)\*|`([^`]+)`/.source;

  function resolveHref(url) {
    if (/^https?:\/\//i.test(url) || /^mailto:/i.test(url)) return url;
    if (url.charAt(0) === '/') return localizeUrl(url);
    if (url.charAt(0) === '#') return url;
    return null;
  }

  function isExternal(href) {
    var match = href.match(/^https?:\/\/([^/?#]+)/i);
    return !!match && match[1].toLowerCase() !== window.location.host.toLowerCase();
  }

  function renderInline(markdown, parent, ticker, allowLinks) {
    // A fresh RegExp per call: renderInline recurses for link and emphasis
    // text, and a shared global regex would have its lastIndex clobbered.
    var pattern = new RegExp(INLINE_PATTERN, 'g');
    var lastIndex = 0;
    var match;
    while ((match = pattern.exec(markdown)) !== null) {
      if (match.index > lastIndex) {
        parent.appendChild(document.createTextNode(markdown.slice(lastIndex, match.index)));
      }
      if (match[1] !== undefined) {
        var href = allowLinks ? resolveHref(match[2]) : null;
        if (href) {
          var link = document.createElement('a');
          link.href = href;
          if (isExternal(href)) {
            link.target = '_blank';
            link.rel = 'noopener noreferrer';
          }
          link.onclick = function () {
            track('docs.sectionTicker.clickedThrough', ticker, this.href);
          };
          renderInline(match[1], link, ticker, false);
          parent.appendChild(link);
        } else {
          renderInline(match[1], parent, ticker, false);
        }
      } else if (match[3] !== undefined) {
        var strong = document.createElement('strong');
        renderInline(match[3], strong, ticker, allowLinks);
        parent.appendChild(strong);
      } else if (match[4] !== undefined) {
        var em = document.createElement('em');
        renderInline(match[4], em, ticker, allowLinks);
        parent.appendChild(em);
      } else if (match[5] !== undefined) {
        var code = document.createElement('code');
        code.textContent = match[5];
        parent.appendChild(code);
      }
      lastIndex = pattern.lastIndex;
    }
    if (lastIndex < markdown.length) {
      parent.appendChild(document.createTextNode(markdown.slice(lastIndex)));
    }
  }

  // ── Rendering ─────────────────────────────────────────────────────────────
  function injectStyles() {
    if (document.getElementById(STYLE_ID)) return;
    var style = document.createElement('style');
    style.id = STYLE_ID;
    style.textContent = ''
      // Strip. Same palette as the clickhouse.com announcement bar: brand
      // yellow with near-black text, in both color schemes.
      + '#' + TICKER_ID + ' { position: relative; z-index: 20; box-sizing: border-box; display: flex; align-items: center; justify-content: center; width: 100%; min-height: 2.25rem; margin: 0 0 1.5rem; padding: 0.375rem 1rem; border-radius: 0.375rem; background: #faff69; color: #161600; font-size: 0.875rem; line-height: 1.25rem; font-weight: 500; text-align: center; }'
      + '#' + TICKER_ID + ' p { flex: 1 1 auto; margin: 0; padding: 0 1.75rem; color: inherit; }'
      + '#' + TICKER_ID + ' strong { font-weight: 700; color: inherit; }'
      + '#' + TICKER_ID + ' code { padding: 0 0.3em; border-radius: 0.25rem; background: rgba(22, 22, 0, 0.09); color: inherit; font-size: 0.8125rem; }'
      + '#' + TICKER_ID + ' a { color: inherit; font-weight: 700; text-decoration: underline; text-decoration-thickness: 1px; text-underline-offset: 3px; white-space: nowrap; }'
      + '#' + TICKER_ID + ' a:hover { text-decoration-thickness: 2px; }'
      + '#' + TICKER_ID + ' a:focus-visible, #' + TICKER_ID + ' button:focus-visible { outline: 2px solid #161600; outline-offset: 2px; }'
      // Dismiss button, pinned to the end of the strip.
      + '#' + TICKER_ID + ' button { position: absolute; top: 50%; right: 0.5rem; display: inline-flex; align-items: center; justify-content: center; width: 1.5rem; height: 1.5rem; margin: 0; padding: 0; border: 0; border-radius: 0.25rem; background: transparent; color: inherit; cursor: pointer; transform: translateY(-50%); }'
      + '#' + TICKER_ID + ' button:hover { background: rgba(22, 22, 0, 0.1); }'
      + '#' + TICKER_ID + ' button svg { width: 1rem; height: 1rem; }'
      // Desktop: pin the strip to the bottom edge of the fixed top navbar
      // (3rem tall, offset by Mintlify's own banner when one is configured)
      // and push the content column and the sticky table of contents down by
      // the strip's measured height, so the page keeps its usual spacing.
      + '@media (min-width: 1024px) {'
      + '#' + TICKER_ID + ' { position: fixed; top: calc(var(--banner-height, 0px) + 3rem); right: 0; left: 19rem; width: auto; margin: 0; border-radius: 0; }'
      + 'html.' + ACTIVE_CLASS + ' #content-area { margin-top: calc(3rem + var(' + HEIGHT_VAR + ', 2.25rem)) !important; }'
      + 'html.' + ACTIVE_CLASS + ' #content-side-layout { top: calc(6rem + var(' + HEIGHT_VAR + ', 2.25rem)) !important; height: calc(100vh - 6rem - var(' + HEIGHT_VAR + ', 2.25rem)) !important; }'
      // Keep anchored headings clear of the strip when following #links.
      + 'html.' + ACTIVE_CLASS + ' { scroll-padding-top: var(' + HEIGHT_VAR + ', 2.25rem); }'
      + '}';
    document.head.appendChild(style);
  }

  function dismissLabel() {
    return DISMISS_LABELS[currentLocale()] || DISMISS_LABELS.en;
  }

  function createTicker(ticker) {
    var element = document.createElement('aside');
    element.id = TICKER_ID;
    element.setAttribute('data-ticker', ticker.id);
    element.setAttribute('data-ticker-key', dismissalKey(ticker) + ':' + currentLocale());

    var text = document.createElement('p');
    renderInline(resolveContent(ticker), text, ticker, true);
    element.appendChild(text);

    if (ticker.dismissible) {
      var button = document.createElement('button');
      button.type = 'button';
      button.setAttribute('aria-label', dismissLabel());
      button.innerHTML = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M18 6 6 18"/><path d="m6 6 12 12"/></svg>';
      button.onclick = function () {
        dismiss(ticker);
        track('docs.sectionTicker.dismissed', ticker);
        sync();
      };
      element.appendChild(button);
    }
    return element;
  }

  function publishHeight(element) {
    var height = element ? element.offsetHeight : 0;
    if (height > 0) {
      document.documentElement.style.setProperty(HEIGHT_VAR, height + 'px');
    } else {
      document.documentElement.style.removeProperty(HEIGHT_VAR);
    }
  }

  var resizeObserver = null;

  function observeHeight(element) {
    if (typeof ResizeObserver !== 'function') {
      publishHeight(element);
      return;
    }
    if (resizeObserver) resizeObserver.disconnect();
    resizeObserver = new ResizeObserver(function () {
      publishHeight(element);
    });
    resizeObserver.observe(element);
    publishHeight(element);
  }

  function setActive(active) {
    var html = document.documentElement;
    if (active) {
      html.classList.add(ACTIVE_CLASS);
    } else {
      html.classList.remove(ACTIVE_CLASS);
      html.style.removeProperty(HEIGHT_VAR);
      if (resizeObserver) {
        resizeObserver.disconnect();
        resizeObserver = null;
      }
    }
  }

  function sync() {
    var ticker = activeTicker();
    var existing = document.getElementById(TICKER_ID);
    if (!ticker) {
      if (existing) existing.remove();
      setActive(false);
      return;
    }

    // `#content-area` is the content column beneath the top navbar. Pages
    // without it (custom layouts) get no ticker.
    var target = document.getElementById('content-area');
    if (!target) {
      if (existing) existing.remove();
      setActive(false);
      return;
    }

    var key = dismissalKey(ticker) + ':' + currentLocale();
    if (existing && existing.getAttribute('data-ticker-key') === key && existing.parentElement === target && target.firstElementChild === existing) {
      setActive(true);
      return;
    }

    if (existing) existing.remove();
    injectStyles();
    var element = createTicker(ticker);
    target.insertBefore(element, target.firstChild);
    setActive(true);
    observeHeight(element);
  }

  function init() {
    sync();

    var scheduled = false;
    new MutationObserver(function () {
      if (scheduled) return;
      scheduled = true;
      requestAnimationFrame(function () {
        scheduled = false;
        sync();
      });
    }).observe(document.documentElement, { childList: true, subtree: true });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
