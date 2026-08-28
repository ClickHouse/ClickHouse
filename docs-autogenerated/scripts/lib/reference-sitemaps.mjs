const siteOrigin = 'https://clickhouse.com';
const sitemapEntryLimit = 50_000;

function escapeXml(value) {
  return value
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&apos;');
}

function absoluteUrl(route) {
  return new URL(route, siteOrigin).href;
}

export function referenceSitemapRoute(channel) {
  return channel === 'latest'
    ? '/docs/reference/sitemap.xml'
    : `/docs/reference/versions/${channel}/sitemap.xml`;
}

export function renderSitemapIndex(sitemapRoutes) {
  if (sitemapRoutes.length > sitemapEntryLimit) {
    throw new Error(
      `Reference sitemap index contains ${sitemapRoutes.length} entries; `
        + `the limit is ${sitemapEntryLimit}`,
    );
  }
  const entries = sitemapRoutes.map((route) => (
    `  <sitemap><loc>${escapeXml(absoluteUrl(route))}</loc></sitemap>`
  ));
  return [
    '<?xml version="1.0" encoding="UTF-8"?>',
    '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
    ...entries,
    '</sitemapindex>',
    '',
  ].join('\n');
}

export function renderReferenceSitemap(routes) {
  if (routes.length > sitemapEntryLimit) {
    throw new Error(
      `Reference sitemap contains ${routes.length} URLs; the limit is ${sitemapEntryLimit}`,
    );
  }
  const entries = routes.map((route) => (
    `  <url><loc>${escapeXml(absoluteUrl(route))}</loc></url>`
  ));
  return [
    '<?xml version="1.0" encoding="UTF-8"?>',
    '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
    ...entries,
    '</urlset>',
    '',
  ].join('\n');
}

function createSitemapNode() {
  return { routes: [], children: new Map() };
}

function addRouteToSitemapTree(root, baseRoute, route) {
  if (route === baseRoute) {
    root.routes.push(route);
    return;
  }
  const prefix = `${baseRoute}/`;
  if (!route.startsWith(prefix)) {
    throw new Error(`Reference route ${route} is outside sitemap root ${baseRoute}`);
  }
  const segments = route.slice(prefix.length).split('/');
  if (segments.some((segment) => !segment || segment === '.' || segment === '..')) {
    throw new Error(`Reference route ${route} contains an invalid path segment`);
  }

  let node = root;
  for (const segment of segments.slice(0, -1)) {
    if (!node.children.has(segment)) node.children.set(segment, createSitemapNode());
    node = node.children.get(segment);
  }
  node.routes.push(route);
}

function addSitemapFile(filesByRoute, route, content) {
  if (filesByRoute.has(route)) {
    throw new Error(`Reference sitemap route ${route} was generated more than once`);
  }
  filesByRoute.set(route, content);
}

function emitSitemapTree(node, nodeRoute, filesByRoute) {
  const childSitemapRoutes = [];
  for (const [segment, child] of [...node.children.entries()].sort(([left], [right]) => (
    left.localeCompare(right)
  ))) {
    const childRoute = `${nodeRoute}/${segment}`;
    emitSitemapTree(child, childRoute, filesByRoute);
    childSitemapRoutes.push(`${childRoute}/sitemap.xml`);
  }

  const sortedRoutes = [...new Set(node.routes)].sort((left, right) => left.localeCompare(right));
  const sitemapRoute = `${nodeRoute}/sitemap.xml`;
  if (childSitemapRoutes.length === 0) {
    addSitemapFile(filesByRoute, sitemapRoute, renderReferenceSitemap(sortedRoutes));
    return;
  }

  const indexEntries = [];
  if (sortedRoutes.length > 0) {
    const pagesSitemapRoute = `${nodeRoute}/pages-sitemap.xml`;
    addSitemapFile(filesByRoute, pagesSitemapRoute, renderReferenceSitemap(sortedRoutes));
    indexEntries.push(pagesSitemapRoute);
  }
  indexEntries.push(...childSitemapRoutes);
  addSitemapFile(filesByRoute, sitemapRoute, renderSitemapIndex(indexEntries));
}

function hierarchicalReferenceSitemapFiles(baseRoute, routes) {
  const root = createSitemapNode();
  for (const route of [baseRoute, ...routes]) {
    addRouteToSitemapTree(root, baseRoute, route);
  }
  const filesByRoute = new Map();
  emitSitemapTree(root, baseRoute, filesByRoute);
  return [...filesByRoute.entries()]
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([route, content]) => ({ route, content }));
}

export function referenceSitemapFiles(manifest, routes, versions) {
  const baseRoute = manifest.channel === 'latest'
    ? '/docs/reference'
    : `/docs/reference/versions/${manifest.channel}`;
  const files = hierarchicalReferenceSitemapFiles(
    baseRoute,
    routes.routes.map(({ route }) => route),
  );
  if (manifest.channel === 'latest') {
    files.unshift({
      route: '/sitemap.xml',
      content: renderSitemapIndex(
        versions.versions.map(({ id }) => referenceSitemapRoute(id)),
      ),
    });
  }
  return files;
}
