(function () {
  'use strict';

  // With an announcement banner configured, Next.js skips its scroll-to-top
  // on client-side navigation: the banner is position:fixed at the top of the
  // re-rendered segment, so the router considers the new page "already in
  // viewport" and leaves the scroll position where it was. (Banner-less
  // Mintlify sites scroll to top as expected; dismissing the banner makes the
  // bug disappear.) Restore the expected behavior by scrolling to the top
  // whenever a forward navigation changes the path.
  //
  // The path is watched from a rAF loop rather than by wrapping
  // history.pushState — the router can hold a reference to the original
  // pushState from before this script runs, which would bypass a wrapper.
  //
  // Back/forward (popstate) is deliberately left alone so the browser and
  // router can restore the previous scroll position. Cross-page hash links
  // (/page#anchor) scroll to the anchor once the new page has rendered it,
  // since the banner bug breaks that scroll too.
  var lastPath = window.location.pathname;
  var traversed = false;

  window.addEventListener('popstate', function () {
    if (window.location.pathname !== lastPath) {
      traversed = true;
    }
  });

  function findAnchor(id) {
    var exact = document.getElementById(id);
    if (exact) return exact;

    // Legacy Docusaurus anchors were lowercase, while some Mintlify anchors
    // preserve identifier casing. Accept casing-only changes in page content,
    // but fail closed if more than one element could be the intended target.
    var main = document.querySelector('main');
    if (!main) return null;

    var normalizedId = id.toLowerCase();
    var elements = main.querySelectorAll('[id]');
    var match = null;
    for (var index = 0; index < elements.length; index += 1) {
      if (elements[index].id.toLowerCase() !== normalizedId) continue;
      if (match) return null;
      match = elements[index];
    }
    return match;
  }

  // The new page renders some frames after the path changes, so poll for the
  // anchor target before scrolling to it; if it never appears (bad anchor),
  // fall back to the top rather than keeping the old page's position.
  function scrollToAnchor(hash, framesLeft) {
    var id;
    try { id = decodeURIComponent(hash.slice(1)); } catch (e) { id = hash.slice(1); }
    var el = findAnchor(id);
    if (el) {
      el.scrollIntoView();
      return;
    }
    if (framesLeft > 0 && window.location.hash === hash) {
      window.requestAnimationFrame(function () { scrollToAnchor(hash, framesLeft - 1); });
    } else {
      window.scrollTo(0, 0);
    }
  }

  // Let the browser handle exact anchors on initial load and same-page hash
  // changes. Only intervene when a legacy casing variant needs compatibility.
  function scrollToLegacyAnchor(hash, framesLeft) {
    var id;
    try { id = decodeURIComponent(hash.slice(1)); } catch (e) { id = hash.slice(1); }
    if (document.getElementById(id)) return;

    var el = findAnchor(id);
    if (el) {
      el.scrollIntoView();
      return;
    }
    if (framesLeft > 0 && window.location.hash === hash) {
      window.requestAnimationFrame(function () { scrollToLegacyAnchor(hash, framesLeft - 1); });
    }
  }

  window.addEventListener('hashchange', function () {
    if (window.location.hash) {
      scrollToLegacyAnchor(window.location.hash, 180);
    }
  });

  function watch() {
    var path = window.location.pathname;
    if (path !== lastPath) {
      lastPath = path;
      if (traversed) {
        traversed = false;
      } else if (window.location.hash) {
        scrollToAnchor(window.location.hash, 180);
      } else {
        window.scrollTo(0, 0);
      }
    }
    window.requestAnimationFrame(watch);
  }

  if (window.location.hash) {
    scrollToLegacyAnchor(window.location.hash, 180);
  }
  window.requestAnimationFrame(watch);
})();
