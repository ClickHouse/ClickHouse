(function () {
  'use strict';

  // Inkeep search integration (official @inkeep/cxkit-mintlify package).
  //
  // Replaces Mintlify's native search with Inkeep's search-only modal. The
  // cxkit-mintlify bundle automatically wires itself to Mintlify's search
  // entry points — it opens the modal on:
  //   • clicks on #search-bar-entry / #search-bar-entry-mobile (navbar), and
  //     #home-search-entry (the home page hero button, see index.mdx)
  //   • the ⌘K / Ctrl+K hotkey
  //
  // We use Inkeep.ModalSearchAndChat — it is the ONLY component that installs
  // the native-search auto-hook (ModalSearch/SearchBar do not) — and force it
  // search-only with `defaultView: 'search'` + `canToggleView: false`. That
  // hides the chat toggle and the "Ask AI" card, so Inkeep does search only;
  // "Ask AI" is handled by Kapa (see kapa-init.js).

  // Inkeep integration API keys. These are client-side/public keys (they ship
  // in the browser bundle), so committing them is expected. Staging covers any
  // *.mintlify.app host (preview/staging deploys); every other host (localhost,
  // mint dev, a future prod domain) uses the local-dev key.
  var INKEEP_API_KEY_STAGING = 'd3e2792740610240ff7bcf2c2a78a33012812eb4f3e34d54';
  var INKEEP_API_KEY_LOCAL = 'b25e5cf856ec9da60d250578b59dace8417359feeedcbc6b';
  var INKEEP_API_KEY = /\.mintlify\.app$/.test(window.location.hostname)
    ? INKEEP_API_KEY_STAGING
    : INKEEP_API_KEY_LOCAL;

  // cxkit-mintlify CDN bundle. @0.5 resolves to the latest 0.5.x; pin a full
  // version (e.g. @0.5.119) when deploying for reproducible builds.
  var INKEEP_SCRIPT_URL = 'https://cdn.jsdelivr.net/npm/@inkeep/cxkit-mintlify@0.5/dist/index.js';

  // ── Search tabs ──────────────────────────────────────────────────────────
  // Catch-all (default) tab. Deliberately NOT named 'All': Inkeep reserves the
  // literal 'All' as a built-in tab that force-pushes EVERY result into it
  // (`tabs.includes("All") && bucket.All.push(item)` in the cxkit bundle),
  // which can't be filtered — so changelogs could never be excluded from it.
  // We use our own catch-all tag instead and apply it in transformSource to
  // every source except changelogs.
  var CATCH_ALL_TAB = 'All results';
  // Row 1 (always visible): the top-level tabs.
  var ROW1_TABS = [CATCH_ALL_TAB, 'Docs', 'Changelogs', 'Blogs', 'Website', 'GitHub'];
  // Row 2 (docs sub-areas): shown only while a docs-context tab is active. A
  // docs source is tagged with BOTH 'Docs' and its sub-area, so selecting
  // 'Docs' shows everything and selecting a sub-area narrows it.
  var DOCS_SUBAREAS = [
    'Get started', 'Concepts', 'Guides', 'Reference',
    'Cloud', 'ClickHouse Private', 'Managed Postgres', 'ClickStack',
    'Agentic Data Stack', 'chDB', 'Kubernetes Operator',
    'ClickPipes', 'Connectors', 'Language clients', 'Ecosystem',
  ];
  var SEARCH_TABS = ROW1_TABS.concat(DOCS_SUBAREAS);

  // Maps a docs path (segment after /docs/) to its row-2 sub-area. Ordered:
  // first matching prefix wins, so specific paths precede their parent.
  var DOCS_SUBAREA_RULES = [
    ['get-started', 'Get started'],
    ['concepts', 'Concepts'],
    ['guides', 'Guides'],
    ['reference', 'Reference'],
    ['products/cloud', 'Cloud'],
    ['products/bring-your-own-cloud', 'Cloud'],
    ['products/clickhouse-private', 'ClickHouse Private'],
    ['products/managed-postgres', 'Managed Postgres'],
    ['products/agentic-data-stack', 'Agentic Data Stack'],
    ['products/chdb', 'chDB'],
    ['products/kubernetes-operator', 'Kubernetes Operator'],
    ['clickstack', 'ClickStack'],
    ['integrations/clickpipes', 'ClickPipes'],
    ['integrations/connectors', 'Connectors'],
    ['integrations/language-clients', 'Language clients'],
    ['integrations', 'Ecosystem'],
  ];

  function docsSubAreaTab(canonicalDocsUrl) {
    var path = canonicalDocsUrl
      .replace(/^https?:\/\/clickhouse\.com\/docs\//, '')
      .replace(/[?#].*$/, '');
    for (var i = 0; i < DOCS_SUBAREA_RULES.length; i++) {
      var prefix = DOCS_SUBAREA_RULES[i][0];
      if (path === prefix || path.indexOf(prefix + '/') === 0) return DOCS_SUBAREA_RULES[i][1];
    }
    return null;
  }

  // CSS (injected into the Inkeep shadow root via theme.styles) that turns the
  // flat tab row into two rows: row 1 always visible, the docs sub-areas only
  // while 'Docs' or one of those sub-areas is the active tab. Tabs are matched
  // by their stable id suffix (`...-trigger-<TabName>`); a sub-area tab is
  // simply "any tab that is not a row-1 tab", so adding sub-areas needs no CSS
  // change. data-state + :has drive the conditional visibility (both verified
  // supported in the cxkit shadow root).
  function buildTwoRowTabCss() {
    var LIST = '.ikp-ai-search-results__tab-list';
    var TAB = '.ikp-ai-search-results__tab';
    var notRow1 = ROW1_TABS.map(function (n) { return ':not([id$="-trigger-' + n + '"])'; }).join('');
    var SUB = TAB + notRow1;            // any docs sub-area tab
    var row1 = TAB + ':not(' + SUB.slice(TAB.length) + ')'; // its complement (row-1 tabs)
    var docsActive = LIST + ':has(' + TAB + '[id$="-trigger-Docs"][data-state="active"])';
    var subActive = LIST + ':has(' + SUB + '[data-state="active"])';
    return [
      // Let the row wrap and stop the single-line horizontal scroll.
      LIST + ' { flex-wrap: wrap !important; overflow-x: visible !important; row-gap: 0.375rem; }',
      // Force ordering: row-1 tabs, then a break, then the sub-area tabs.
      row1 + ' { order: 0; }',
      SUB + ' { order: 2; display: none !important; }',
      // Reveal the sub-area tabs only when Docs (or a sub-area) is active.
      docsActive + ' ' + SUB + ', ' + subActive + ' ' + SUB + ' { display: inline-flex !important; }',
      // A full-width pseudo-element (a flex item of the tab-list) ordered
      // between row-1 and the sub-areas forces the sub-areas onto their own
      // second line — only while that row is showing. No DOM injection, so it
      // survives Inkeep's re-renders.
      docsActive + '::before, ' + subActive + '::before' +
        ' { content: ""; order: 1; flex: 0 0 100%; height: 0; }',
    ].join('');
  }

  function loadScript(url, callback) {
    if (document.getElementById('inkeep-cxkit-script')) {
      callback();
      return;
    }
    var script = document.createElement('script');
    script.id = 'inkeep-cxkit-script';
    script.src = url;
    script.type = 'text/javascript';
    script.onload = callback;
    document.head.appendChild(script);
  }

  // When the Inkeep modal opens, its scroll lock (react-remove-scroll) marks
  // the body with [data-scroll-locked] and adds `padding-right` equal to the
  // scrollbar width to "compensate" for hiding the scrollbar. On systems with
  // classic (non-overlay) scrollbars that padding shrinks the body and shifts
  // centered page content sideways. We neutralize it — the selector is more
  // specific than react-remove-scroll's own rule so it wins regardless of
  // injection order.
  function injectNoShiftStyle() {
    if (document.getElementById('inkeep-no-shift-style')) return;
    var style = document.createElement('style');
    style.id = 'inkeep-no-shift-style';
    style.textContent =
      'html body[data-scroll-locked] { padding-right: 0 !important; margin-right: 0 !important; }';
    document.head.appendChild(style);
  }

  function initInkeep() {
    if (typeof Inkeep === 'undefined' || !Inkeep || typeof Inkeep.ModalSearchAndChat !== 'function') {
      console.log('Inkeep: cxkit-mintlify did not expose ModalSearchAndChat.');
      return;
    }

    var settings = {
      // Open straight to the search view. (canToggleView is honored by
      // SearchBar/ChatButton but NOT by ModalSearchAndChat, so we hide the
      // chat affordances via CSS below instead.)
      defaultView: 'search',
      baseSettings: {
        apiKey: INKEEP_API_KEY,
        primaryBrandColor: '#fdff75',
        organizationDisplayName: 'ClickHouse',
        // Route each search result into a custom tab by its URL. The cxkit
        // `tabs` array below (in searchSettings) controls which tabs render and
        // their order. Docs results are served from the Mintlify preview host,
        // so we also rewrite those links to the canonical clickhouse.com/docs.
        // IMPORTANT: this must be idempotent. Inkeep re-invokes transformSource
        // on already-transformed sources (the incoming source.tabs/url are what
        // we returned last time), so we must NOT append to source.tabs or
        // re-derive from a URL we already rewrote. We rebuild the tabs array
        // from scratch off the URL each call — recognizing both the raw preview
        // host and the already rewritten clickhouse.com/docs form — and
        // overwrite tabs with the result.
        transformSource: function (source) {
          var url = source.url || '';
          var isPreview = url.indexOf('private-7c7dfe99.mintlify.app') !== -1;
          if (isPreview) {
            // Rewrite preview links to the canonical clickhouse.com/docs domain.
            url = url.replace(/^https?:\/\/private-7c7dfe99\.mintlify\.app\//, 'https://clickhouse.com/docs/');
          }
          // Do NOT tag sources with 'All' — 'All' is Inkeep's reserved built-in
          // tab (shows every result automatically); tagging sources with it
          // corrupts per-tab filtering. Docs sources are multi-tab (Docs + the
          // row-2 sub-area) so 'Docs' shows all docs and a sub-area narrows.
          var tabs = [];
          if (isPreview || /clickhouse\.com\/docs(\/|$)/.test(url)) {
            if (/\/resources\/changelogs(\/|$)/.test(url)) {
              tabs.push('Changelogs');
            } else {
              tabs.push('Docs');
              var sub = docsSubAreaTab(url);
              if (sub) tabs.push(sub);
            }
          } else if (url.indexOf('github.com') !== -1 && /\/issues(\/|$)/.test(url)) {
            tabs.push('GitHub'); // GitHub issues only — exclude repos, PRs, etc.
          } else if (/\/blog(\/|$)/.test(url)) {
            tabs.push('Blogs');
          } else if (url.indexOf('clickhouse.com') !== -1) {
            tabs.push('Website'); // marketing site (the /docs case is handled above)
          }
          // Every non-changelog result also belongs to the catch-all tab.
          // Changelogs are excluded so they only surface under their own tab.
          // This also captures results that matched none of the branches above
          // (tabs still empty) — they would otherwise appear in no tab now that
          // Inkeep's reserved 'All' tab is gone.
          if (tabs.indexOf('Changelogs') === -1) tabs.unshift(CATCH_ALL_TAB);
          return Object.assign({}, source, { tabs: tabs, url: url });
        },
        // Follow Mintlify's `.dark` class on <html>.
        colorMode: {
          sync: {
            target: document.documentElement,
            attributes: ['class'],
            isDarkMode: function (attributes) {
              return ((attributes && attributes.class) || '').indexOf('dark') !== -1;
            },
          },
        },
        // Make the modal search-only: hide the Search/Ask AI view toggle and
        // the "Ask AI — Start conversation" card. Inkeep renders inside a
        // shadow root, so we inject CSS via theme.styles (which mounts inside
        // that shadow root). "Ask AI" stays exclusively on Kapa.
        theme: {
          styles: [
            {
              key: 'hide-inkeep-ai-chat',
              type: 'style',
              value: '.ikp-view_toggle, .ikp-ai-ask-ai-trigger { display: none !important; }',
            },
            {
              key: 'dark-search-overlay',
              type: 'style',
              value: '.dark\\:bg-overlay-dark { background-color: rgba(0, 0, 0, 0.75) !important; }',
            },
            {
              key: 'two-row-docs-tabs',
              type: 'style',
              value: buildTwoRowTabCss(),
            },
          ],
        },
      },
      searchSettings: {
        placeholder: 'Search ClickHouse docs...',
        // Wait 300ms after the last keystroke before firing a search request,
        // so fast typing issues one query instead of one per character.
        debounceTimeMs: 300,
        // Return at most 20 results per query (down from Inkeep's default 40).
        maxResults: 20,
        shouldShowContentSnippets: true,
        contentSnippetLength: 200,
        shouldHighlightMatches: true,
        // Row-1 tabs followed by the docs sub-areas (see SEARCH_TABS). The
        // two-row-docs-tabs CSS above keeps the sub-areas hidden until 'Docs'
        // (or one of them) is active. All tabs are populated by transformSource.
        tabs: SEARCH_TABS,
      },
    };

    // cxkit-mintlify auto-binds this to the Mintlify search bar entries
    // (#search-bar-entry / -mobile / #home-search-entry) and the ⌘K hotkey,
    // intercepting them (capture-phase preventDefault) so the native search
    // never opens.
    Inkeep.ModalSearchAndChat(settings);
  }

  function boot() {
    try {
      injectNoShiftStyle();
      loadScript(INKEEP_SCRIPT_URL, initInkeep);
    } catch (e) {
      console.log('Inkeep: failed to load widget:', e);
    }
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', boot);
  } else {
    boot();
  }
})();