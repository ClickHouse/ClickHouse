(function () {
  'use strict';

  var routeFiles = {
    '/reference/settings/session-settings':
      '/_site/customizations/settings-legacy-routes/session-settings.js',
    '/reference/settings/server-settings/settings':
      '/_site/customizations/settings-legacy-routes/server-settings.js',
    '/reference/settings/merge-tree-settings':
      '/_site/customizations/settings-legacy-routes/mergetree-settings.js',
    '/reference/settings/formats':
      '/_site/customizations/settings-legacy-routes/format-settings.js',
  };
  var BASE = /^\/docs(\/|$)/.test(window.location.pathname) ? '/docs' : '';
  var loadingRoutes = {};
  var lastLocation = '';
  var framesRemaining = 180;

  function canonicalAnchor(anchorRoutes, value) {
    var candidates = [
      value,
      value.replace(/[?,;:!'"()[\]{}]/g, ''),
    ];
    for (var index = 0; index < candidates.length; index += 1) {
      var candidate = candidates[index];
      if (anchorRoutes[candidate]) return candidate;
      var lowerValue = candidate.toLowerCase();
      if (anchorRoutes[lowerValue]) return lowerValue;
    }
    return null;
  }

  function redirectLegacySettingAnchor() {
    var rawHash = window.location.hash.slice(1);
    if (!rawHash) return false;

    var decodedHash;
    try {
      decodedHash = decodeURIComponent(rawHash);
    } catch (error) {
      return false;
    }

    var routeFamilies = window.clickhouseSettingsLegacyRoutes || {};
    var baseRoutes = Object.keys(routeFiles);
    for (var index = 0; index < baseRoutes.length; index += 1) {
      var baseRoute = baseRoutes[index];
      var markerIndex = window.location.pathname.indexOf(baseRoute);
      if (markerIndex < 0) continue;
      var routeSuffix = window.location.pathname.slice(
        markerIndex + baseRoute.length
      ).replace(/\/$/, '');
      if (routeSuffix) continue;

      if (!routeFamilies[baseRoute]) {
        if (!loadingRoutes[baseRoute]) {
          loadingRoutes[baseRoute] = true;
          var script = document.createElement('script');
          script.src = BASE + routeFiles[baseRoute];
          script.onload = redirectLegacySettingAnchor;
          script.onerror = function () {
            loadingRoutes[baseRoute] = false;
          };
          document.head.appendChild(script);
        }
        return false;
      }

      var anchorRoutes = routeFamilies[baseRoute];
      var directAnchor = canonicalAnchor(anchorRoutes, decodedHash);
      var baseAnchor = directAnchor || canonicalAnchor(
        anchorRoutes,
        decodedHash.split('-', 1)[0]
      );
      if (!baseAnchor) return false;

      var basePath = window.location.pathname.slice(0, markerIndex);
      window.location.replace(
        basePath + anchorRoutes[baseAnchor] + window.location.search +
        '#' + (directAnchor || rawHash)
      );
      return true;
    }
    return false;
  }

  function watchLocation() {
    var location = (
      window.location.pathname + window.location.search + window.location.hash
    );
    if (location !== lastLocation) {
      lastLocation = location;
      framesRemaining = 180;
    }

    if (framesRemaining > 0) {
      framesRemaining -= 1;
      if (redirectLegacySettingAnchor()) return;
    }
    window.requestAnimationFrame(watchLocation);
  }

  watchLocation();
})();
