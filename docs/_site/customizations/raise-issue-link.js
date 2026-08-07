(function () {
  'use strict';

  // Mintlify renders a default "Raise issue" feedback link that opens a blank
  // issue (issues/new?title=Issue on docs&body=Path: <page>). Point it at the
  // repository's Documentation issue form instead, carrying the page path
  // through so the reporter does not have to type it. The path lands in the
  // form's `page` field (matched by its `id` in
  // .github/ISSUE_TEMPLATE/60_documentation-issue.yaml).
  var FORM = 'https://github.com/ClickHouse/ClickHouse/issues/new';
  var TEMPLATE = '60_documentation-issue.yaml';

  function pagePathFrom(href) {
    // Prefer the path Mintlify already encoded in the default link's body
    // ("Path: /foo"); fall back to the current location.
    try {
      var body = new URL(href, window.location.origin).searchParams.get('body') || '';
      var m = body.match(/Path:\s*(\S+)/);
      if (m) return m[1];
    } catch (e) {}
    return window.location.pathname;
  }

  function targetHref(path) {
    return FORM + '?template=' + encodeURIComponent(TEMPLATE) +
      '&page=' + encodeURIComponent(path);
  }

  function isDefaultRaiseIssue(href) {
    // Only touch Mintlify's default "Raise issue" link; leave any other
    // issues/new links (e.g. in page content) alone. Spaces in the title may be
    // encoded as %20 or +, or left literal in the attribute.
    return /\/issues\/new\?/.test(href) &&
      /title=Issue(%20|\+|\s)on(%20|\+|\s)docs/i.test(href);
  }

  function rewrite() {
    var links = document.querySelectorAll('a[href*="/issues/new"]');
    for (var i = 0; i < links.length; i++) {
      var href = links[i].getAttribute('href') || '';
      if (isDefaultRaiseIssue(href)) {
        links[i].setAttribute('href', targetHref(pagePathFrom(href)));
      }
    }
  }

  // Run now and keep watching: Mintlify is a SPA, so the feedback link is
  // re-rendered (with a fresh default href) on every client-side navigation.
  // Rewriting is idempotent -- after the rewrite the href no longer matches
  // isDefaultRaiseIssue -- so re-running on our own attribute change is a no-op
  // and cannot loop.
  function start() {
    rewrite();
    new MutationObserver(rewrite).observe(document.body, {
      childList: true,
      subtree: true,
      attributes: true,
      attributeFilter: ['href'],
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', start);
  } else {
    start();
  }
})();