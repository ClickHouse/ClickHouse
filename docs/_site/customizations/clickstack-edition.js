(function () {
  'use strict';

  var PILL_ID = 'ch-clickstack-edition';
  // Commercial-only pages retain their original URLs. Keep this list aligned
  // with clickstack/navigation.json; shared commercial pages use /managed/.
  var COMMERCIAL_PAGES = [
    '/clickstack/getting-started/managed',
    '/clickstack/deployment/managed',
    '/clickstack/notebooks',
    '/clickstack/managing/estimating-resources',
    '/clickstack/managing/rbac'
  ];

  // Shared guide counterparts use identical content. Applies in either sidebar.
  var SHARED_GUIDES = [
    "/api-reference",
    "/architecture",
    "/demo-days/2026/2026-04-03",
    "/demo-days/2026/2026-04-10",
    "/demo-days/2026/2026-04-17",
    "/demo-days/2026/2026-05-08",
    "/demo-days/2026/2026-05-15",
    "/demo-days/2026/2026-05-22",
    "/demo-days/2026/2026-05-29",
    "/demo-days/2026/2026-06-05",
    "/demo-days/2026/2026-06-12",
    "/demo-days/2026/2026-06-18",
    "/demo-days/2026/2026-06-26",
    "/demo-days/2026/2026-07-02",
    "/demo-days/2026/2026-07-10",
    "/demo-days/2026/2026-07-17",
    "/demo-days/2026/2026-07-24",
    "/demo-days/2026/2026-07-31",
    "/demo-days/2026/2026-08-07",
    "/demo-days/2026/2026-08-15",
    "/demo-days/2026/2026-08-21",
    "/demo-days/2026/2026-08-28",
    "/demo-days",
    "/example-datasets/chrome-extension",
    "/example-datasets",
    "/example-datasets/instrument-application",
    "/example-datasets/kubernetes",
    "/example-datasets/local-data",
    "/example-datasets/otelgen",
    "/example-datasets/sample-data",
    "/example-datasets/session-replay",
    "/example-datasets/telemetrygen",
    "/faq",
    "/features/alerts",
    "/features/dashboards/dashboard-templates",
    "/features/dashboards/overview",
    "/features/dashboards/release-markers",
    "/features/dashboards/row-click-drilldowns",
    "/features/dashboards/sql-visualizations",
    "/features/event-deltas",
    "/features/event-patterns",
    "/features/search",
    "/features/session-replay",
    "/ingesting-data/collector",
    "/ingesting-data/opentelemetry",
    "/ingesting-data/overview",
    "/ingesting-data/schema/map-vs-json",
    "/ingesting-data/schemas",
    "/ingesting-data/sdks/aws-lambda",
    "/ingesting-data/sdks/browser",
    "/ingesting-data/sdks/deno",
    "/ingesting-data/sdks/elixir",
    "/ingesting-data/sdks/golang",
    "/ingesting-data/sdks",
    "/ingesting-data/sdks/java",
    "/ingesting-data/sdks/nestjs",
    "/ingesting-data/sdks/nextjs",
    "/ingesting-data/sdks/nodejs",
    "/ingesting-data/sdks/python",
    "/ingesting-data/sdks/react-native",
    "/ingesting-data/sdks/ruby",
    "/ingesting-data/trace-sampling",
    "/ingesting-data/vector",
    "/integration-examples/aws-lambda",
    "/integration-examples/cloudflare",
    "/integration-examples/cloudwatch",
    "/integration-examples/host-logs/ec2",
    "/integration-examples/host-logs",
    "/integration-examples",
    "/integration-examples/jvm-metrics",
    "/integration-examples/kafka-logs",
    "/integration-examples/kafka-metrics",
    "/integration-examples/kubernetes",
    "/integration-examples/mongodb-logs",
    "/integration-examples/mysql",
    "/integration-examples/nginx-logs",
    "/integration-examples/nginx-traces",
    "/integration-examples/nodejs-traces",
    "/integration-examples/postgres-logs",
    "/integration-examples/postgres-metrics",
    "/integration-examples/redis-logs",
    "/integration-examples/redis-metrics",
    "/integration-examples/systemd",
    "/integration-examples/temporal",
    "/integration-partners/bindplane",
    "/integration-partners/bitdrift",
    "/integration-partners",
    "/integration-partners/odigos",
    "/integration-partners/telflo",
    "/managing/materialized-views",
    "/managing/performance-tuning",
    "/managing/ttl",
    "/mcp",
    "/migration/datadog",
    "/migration/elastic/concepts",
    "/migration/elastic",
    "/migration/elastic/intro",
    "/migration/elastic/migrating-agents",
    "/migration/elastic/migrating-data",
    "/migration/elastic/migrating-sdks",
    "/migration/elastic/search",
    "/migration/elastic/types",
    "/migration",
    "/service-maps",
    "/text-to-chart",
    "/url-parameters"
  ];

  function editionForPath(path) {
    path = path.replace(/^\/docs(?=\/|$)/, '').replace(/\/+$/, '').replace(/\/index$/, '');
    if (path !== '/clickstack' && path.indexOf('/clickstack/') !== 0) return [];
    var guide = path.replace(/^\/clickstack(?:\/managed)?/, '');
    if (SHARED_GUIDES.indexOf(guide) !== -1) return ['ClickHouse Observability', 'OSS'];
    if (path.indexOf('/clickstack/managed/') === 0
        || path.indexOf('/clickstack/managed-onboarding/') === 0
        || COMMERCIAL_PAGES.indexOf(path) !== -1) {
      return ['ClickHouse Observability'];
    }
    return ['OSS'];
  }

  function updateEditionPill() {
    var labels = editionForPath(window.location.pathname);
    var pill = document.getElementById(PILL_ID);
    var title = document.getElementById('page-title');
    if (!labels.length || !title) {
      if (pill) pill.remove();
      return;
    }
    if (!pill) {
      pill = document.createElement('span');
      pill.id = PILL_ID;
      pill.className = 'ch-clickstack-edition-pills';
    }
    var identity = labels.join('|');
    if (pill.getAttribute('data-editions') !== identity) {
      pill.replaceChildren();
      labels.forEach(function (label) {
        var badge = document.createElement('span');
        badge.className = 'ch-clickstack-edition-pill';
        badge.textContent = label;
        pill.appendChild(badge);
      });
      pill.setAttribute('data-editions', identity);
    }
    if (pill.nextSibling !== title) title.parentNode.insertBefore(pill, title);
  }

  function init() {
    updateEditionPill();
    var scheduled = false;
    new MutationObserver(function () {
      if (scheduled) return;
      scheduled = true;
      requestAnimationFrame(function () {
        scheduled = false;
        updateEditionPill();
      });
    }).observe(document.documentElement, { childList: true, subtree: true });
    window.addEventListener('popstate', updateEditionPill);
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
