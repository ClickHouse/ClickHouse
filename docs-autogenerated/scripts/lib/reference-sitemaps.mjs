const siteOrigin = 'https://clickhouse.com';

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

export function renderSitemapIndex(versions) {
  const entries = versions.map(({ id }) => (
    `  <sitemap><loc>${escapeXml(absoluteUrl(referenceSitemapRoute(id)))}</loc></sitemap>`
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
  if (routes.length > 50_000) {
    throw new Error(`Reference sitemap contains ${routes.length} URLs; the limit is 50000`);
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

export function referenceSitemapFiles(manifest, routes, versions) {
  const baseRoute = manifest.channel === 'latest'
    ? '/docs/reference'
    : `/docs/reference/versions/${manifest.channel}`;
  const sitemapRoute = referenceSitemapRoute(manifest.channel);
  const files = [{
    route: sitemapRoute,
    content: renderReferenceSitemap([baseRoute, ...routes.routes.map(({ route }) => route)]),
  }];
  if (manifest.channel === 'latest') {
    files.unshift({ route: '/sitemap.xml', content: renderSitemapIndex(versions.versions) });
  }
  return files;
}
