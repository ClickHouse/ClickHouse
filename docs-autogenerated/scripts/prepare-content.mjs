#!/usr/bin/env node

import { createHash } from 'node:crypto';
import { copyFile, mkdir, readFile, rename, rm, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDirectory = path.dirname(fileURLToPath(import.meta.url));
const projectDirectory = path.resolve(scriptDirectory, '..');
const repositoryDirectory = path.resolve(projectDirectory, '..');
const sourceDocsDirectory = path.join(repositoryDirectory, 'docs');
const defaultArtifactDirectory = path.join(projectDirectory, '.artifacts/latest');
const generatedDirectory = path.join(projectDirectory, 'src/generated');
const stagingDirectory = path.join(projectDirectory, 'src/generated.next');

const componentNames = new Set([
  'Accordion',
  'BetaBadge',
  'Card',
  'CloudNotSupportedBadge',
  'CloudOnlyBadge',
  'Danger',
  'ExperimentalBadge',
  'Frame',
  'Info',
  'Note',
  'ScalePlanFeatureBadge',
  'SettingsInfoBlock',
  'Tab',
  'Tabs',
  'Tip',
  'VersionHistory',
  'Warning',
]);

function pagePathFromRoute(route) {
  const pagePath = route.replace(/^\/+/, '');
  if (!pagePath || pagePath.split('/').includes('..')) {
    throw new Error(`Invalid generated route: ${route}`);
  }
  return pagePath;
}

function mintlifyDocsConfig(documents, sourceSiteConfig, navigation) {
  if (navigation.schemaVersion !== 2 || navigation.root?.type !== 'group') {
    throw new Error(`Unsupported reference navigation schema: ${navigation.schemaVersion}`);
  }

  const documentById = new Map(documents.map((document) => [document.id, document]));
  function convertNode(node) {
    if (node.type === 'document') {
      const document = documentById.get(node.documentId);
      if (!document) throw new Error(`Navigation references unknown document ${node.documentId}`);
      if (document.route !== node.route) {
        throw new Error(`Navigation route mismatch for ${node.documentId}`);
      }
      return pagePathFromRoute(document.route);
    }
    if (node.type !== 'group' || !Array.isArray(node.children)) {
      throw new Error(`Invalid navigation node in ${node.id ?? 'reference navigation'}`);
    }
    return {
      group: node.label,
      pages: node.children.map(convertNode),
      expandable: true,
      expanded: node.expanded === true,
    };
  }

  const groups = navigation.root.children.map(convertNode);

  return {
    $schema: sourceSiteConfig.$schema,
    name: sourceSiteConfig.name,
    theme: sourceSiteConfig.theme,
    appearance: sourceSiteConfig.appearance,
    background: sourceSiteConfig.background,
    colors: sourceSiteConfig.colors,
    fonts: sourceSiteConfig.fonts,
    logo: sourceSiteConfig.logo,
    favicon: sourceSiteConfig.favicon,
    styling: sourceSiteConfig.styling,
    navigation: {
      tabs: [
        {
          tab: 'Reference',
          groups,
        },
      ],
    },
  };
}

function exactVersionLabel(sourceVersion) {
  return sourceVersion.match(/^v?(\d+\.\d+\.\d+(?:\.\d+)?)/)?.[1]
    ?? sourceVersion.replace(/^v/, '');
}

function versionsConfig(manifest, routes) {
  const exactVersion = exactVersionLabel(manifest.sourceVersion);
  const currentRoutes = Object.fromEntries(
    routes.routes.map((route) => [route.id, route.route]),
  );
  const currentChannel = manifest.channel === 'latest' || manifest.channel === 'head'
    ? manifest.channel
    : null;

  const versions = [
    {
      id: 'latest',
      kind: 'channel',
      label: 'Latest',
      exactVersion: currentChannel === 'latest' ? exactVersion : undefined,
      description: 'Current release',
      available: currentChannel === 'latest',
      indexable: true,
      fallbackRoute: '/docs/reference',
      routes: currentChannel === 'latest' ? currentRoutes : undefined,
    },
    {
      id: 'head',
      kind: 'channel',
      label: 'Head',
      exactVersion: currentChannel === 'head' ? exactVersion : undefined,
      description: 'Master preview',
      available: currentChannel === 'head',
      indexable: false,
      fallbackRoute: '/docs/reference/head',
      routes: currentChannel === 'head' ? currentRoutes : undefined,
    },
  ];

  if (manifest.channel !== 'latest' && manifest.channel !== 'head') {
    versions.push({
      id: manifest.channel,
      kind: 'release',
      label: exactVersion,
      exactVersion,
      description: 'Pinned release',
      available: true,
      indexable: false,
      fallbackRoute: `/docs/reference/versions/${exactVersion}`,
      routes: currentRoutes,
    });
  }

  return {
    schemaVersion: 1,
    currentId: manifest.channel,
    generatedAt: manifest.generatedAt,
    sourceRevision: manifest.sourceRevision,
    versions,
  };
}

const headerMenuMetadata = new Map([
  ['get-started/navigation.json', { label: 'Get started', icon: '/images/icons/icon-get-started.svg' }],
  ['concepts/navigation.json', { label: 'Concepts', icon: '/images/icons/icon-concepts.svg' }],
  ['guides/navigation.json', { label: 'Guides', icon: '/images/icons/icon-guides.svg' }],
  ['reference/navigation.json', { label: 'Reference', icon: '/images/icons/icon-reference.svg', href: '/docs/reference' }],
  ['products/cloud/navigation.json', { label: 'ClickHouse Cloud', icon: '/images/icons/icon-clickhouse-cloud.svg' }],
  ['products/managed-postgres/navigation.json', { label: 'Managed Postgres', icon: '/images/icons/icon-postgres.svg' }],
  ['clickstack/navigation.json', { label: 'ClickStack', icon: '/images/icons/icon-clickstack.svg' }],
  ['products/agentic-data-stack/navigation.json', { label: 'Agentic Data Stack', icon: '/images/icons/icon-agentic-data-stack.svg' }],
  ['chdb/navigation.json', { label: 'chDB', icon: '/images/icons/icon-chdb.svg' }],
  ['products/kubernetes-operator/navigation.json', { label: 'Kubernetes Operator', icon: '/images/icons/icon-kubernetes-operator.svg' }],
  ['integrations/clickpipes/navigation.json', { label: 'ClickPipes', icon: '/images/icons/icon-clickpipes.svg' }],
  ['integrations/language-clients/navigation.json', { label: 'Language clients', icon: '/images/icons/icon-language-clients.svg' }],
  ['integrations/connectors/navigation.json', { label: 'Connectors', icon: '/images/icons/icon-connectors.svg' }],
  ['resources/support-center/navigation.json', { label: 'Support', icon: '/images/icons/icon-support-center.svg' }],
  ['resources/develop-contribute/navigation.json', { label: 'Develop and contribute', icon: '/images/icons/icon-contribute.svg' }],
  ['resources/changelogs/navigation.json', { label: 'Changelogs', icon: '/images/icons/icon-changelogs.svg' }],
  ['resources/about/navigation.json', { label: 'About', icon: '/images/icons/icon-about.svg' }],
]);

function firstNavigationPage(entry) {
  if (typeof entry === 'string') return entry;
  if (!entry || typeof entry !== 'object') return null;
  if (typeof entry.href === 'string' && entry.href !== '#') return entry.href;

  for (const key of ['pages', 'groups', 'dropdowns']) {
    if (!Array.isArray(entry[key])) continue;
    for (const child of entry[key]) {
      const page = firstNavigationPage(child);
      if (page) return page;
    }
  }
  return null;
}

function publishedDocsHref(page) {
  if (!page) return 'https://clickhouse.com/docs';
  if (/^https?:\/\//.test(page)) return page;
  const normalizedPage = page.replace(/^\/+/, '');
  const normalized = normalizedPage === 'index' ? '' : normalizedPage.replace(/\/index$/, '');
  return normalized ? `https://clickhouse.com/docs/${normalized}` : 'https://clickhouse.com/docs';
}

async function headerNavigationConfig(sourceSiteConfig) {
  const englishNavigation = sourceSiteConfig.navigation.languages.find(
    (navigation) => navigation.language === 'en',
  );
  if (!englishNavigation) throw new Error('English navigation is missing from docs.json');

  const tabs = [];
  for (const tab of englishNavigation.tabs) {
    if (!Array.isArray(tab.menu)) {
      tabs.push({
        label: tab.tab,
        href: publishedDocsHref(firstNavigationPage(tab)),
        active: false,
      });
      continue;
    }

    const items = [];
    for (const rawEntry of tab.menu) {
      let entry = rawEntry;
      let metadata = {};

      if (typeof rawEntry.$ref === 'string') {
        const reference = rawEntry.$ref.replace(/^\.\//, '');
        if (reference.split('/').includes('..')) {
          throw new Error(`Invalid header navigation reference: ${rawEntry.$ref}`);
        }
        metadata = headerMenuMetadata.get(reference) ?? {};
        if (!metadata.href) entry = await readJson(path.join(sourceDocsDirectory, reference));
      }

      if (rawEntry.href === '#') {
        items.push({ kind: 'heading', label: rawEntry.item });
        continue;
      }

      const page = metadata.href ?? firstNavigationPage(rawEntry) ?? firstNavigationPage(entry);
      const label = rawEntry.item
        ?? entry.item
        ?? metadata.label
        ?? entry.group
        ?? entry.groups?.[0]?.group;
      if (!label || !page) {
        throw new Error(`Header navigation item in ${tab.tab} is missing a label or page`);
      }

      const href = metadata.href ?? publishedDocsHref(page);
      items.push({
        kind: 'link',
        label,
        icon: rawEntry.icon ?? entry.icon ?? metadata.icon,
        href,
        external: /^https?:\/\//.test(href),
      });
    }

    tabs.push({ label: tab.tab, active: tab.tab === 'Database', items });
  }

  return { schemaVersion: 1, tabs };
}

function parseArguments(argv) {
  if (argv.length === 0) return defaultArtifactDirectory;
  if (argv.length === 2 && argv[0] === '--artifact') return path.resolve(argv[1]);
  throw new Error('Usage: node scripts/prepare-content.mjs [--artifact <directory>]');
}

function hash(value) {
  return createHash('sha256').update(value).digest('hex');
}

async function readJson(sourcePath) {
  return JSON.parse(await readFile(sourcePath, 'utf8'));
}

async function writeJson(destination, value) {
  await mkdir(path.dirname(destination), { recursive: true });
  await writeFile(destination, `${JSON.stringify(value, null, 2)}\n`, 'utf8');
}

function escapeRegularExpression(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function rewriteReferenceLinks(content, routeRewrites) {
  const docsPrefixes = [
    'changelogs',
    'chdb',
    'clickstack',
    'concepts',
    'get-started',
    'guides',
    'integrations',
    'interfaces',
    'operations',
    'products',
    'reference',
    'resources',
    'sql-reference',
  ].join('|');
  const markdownLink = new RegExp(`\\]\\(/(${docsPrefixes})(?=[/#)])`, 'g');
  const componentLink = new RegExp(`href=(['"])\/(${docsPrefixes})(?=[/#])`, 'g');

  let rewritten = content
    .replace(markdownLink, '](/docs/$1')
    .replace(componentLink, 'href=$1/docs/$2');

  for (const [oldRoute, newRoute] of routeRewrites) {
    const oldRoutePattern = escapeRegularExpression(oldRoute);
    if (oldRoute.includes('#')) {
      rewritten = rewritten
        .replace(new RegExp(`(\\]\\()${oldRoutePattern}(?=\\))`, 'g'), `$1${newRoute}`)
        .replace(new RegExp(`(href=['"])${oldRoutePattern}(?=['"])`, 'g'), `$1${newRoute}`);
      continue;
    }
    rewritten = rewritten
      .replace(new RegExp(`\\]\\(${oldRoutePattern}(?=[#)])`, 'g'), `](${newRoute}`)
      .replace(
        new RegExp(`href=(['"])${oldRoutePattern}(?=[#'\"])`, 'g'),
        `href=$1${newRoute}`,
      );
  }

  return rewritten;
}

function headingSlug(title) {
  return title
    .replace(/\[([^\]]+)\]\([^)]+\)/g, '$1')
    .replace(/<[^>]+>/g, '')
    .replace(/[`*_~]/g, '')
    .replace(/&[A-Za-z0-9#]+;/g, '')
    .toLocaleLowerCase()
    .replace(/[^\p{Letter}\p{Number}]+/gu, '-')
    .replace(/^-|-$/g, '');
}

function rewriteExplicitHeadingAnchors(content) {
  return content.replace(
    /^(#{1,6})\s+(.+?)\s*\{#([^}\n]+)\}\s*$/gm,
    (_line, level, title, anchor) => {
      const markdownHeading = `${level} ${title}`;
      return headingSlug(title) === anchor
        ? markdownHeading
        : `<span id="${anchor}" className="legacy-anchor"></span>\n\n${markdownHeading}`;
    },
  );
}

function rewriteInlineCodeBraces(content) {
  let fence = null;
  let mathBlock = false;

  function escapePlainBraces(value) {
    return value.replace(/\{/g, '&#123;').replace(/\}/g, '&#125;');
  }

  return content
    .split('\n')
    .map((line) => {
      const fenceMatch = line.match(/^\s*(`{3,}|~{3,})/);
      if (fenceMatch) {
        if (!fence) fence = fenceMatch[1];
        else if (fence[0] === fenceMatch[1][0] && fenceMatch[1].length >= fence.length) fence = null;
        return line;
      }
      if (fence) return line;
      if (line.trim() === '$$') {
        mathBlock = !mathBlock;
        return line;
      }
      if (mathBlock || line.includes('<VersionHistory rows={')) return line;
      if (/^\s*<[^>]+>\s*$/.test(line) || /^\s*\{\/\*/.test(line)) return line;

      let result = '';
      let cursor = 0;
      for (const match of line.matchAll(/(`+)([^`\n]*?)\1/g)) {
        result += escapePlainBraces(line.slice(cursor, match.index));
        result += /[{}]/.test(match[2])
          ? `<code>{${JSON.stringify(match[2])}}</code>`
          : match[0];
        cursor = match.index + match[0].length;
      }
      result += escapePlainBraces(line.slice(cursor));
      return result;
    })
    .join('\n');
}

function prepareMdx(content, sourcePath, routeRewrites = new Map()) {
  const importedComponents = new Set();
  let prepared = content.replace(
    /^import\s+.+?\s+from\s+['"]\/snippets\/components\/[^'"]+['"];?\s*$/gm,
    '',
  );

  prepared = prepared.replace(
    /^import\s+(\w+)\s+from\s+['"]\/snippets\/(?!components\/)([^'"]+\.mdx)['"];?\s*$/gm,
    (_statement, componentName, snippetPath) => {
      importedComponents.add(componentName);
      return `import ${componentName} from '/snippets/${snippetPath}';\n`;
    },
  );
  prepared = prepared.replace(/\{\/\*[\s\S]*?\*\/\}/g, '');

  for (const importStatement of prepared.match(/^import\s+.+?\s+from\s+['"]\/snippets\/[^'"]+['"];?\s*$/gm) ?? []) {
    if (!/^import\s+\w+\s+from\s+['"]\/snippets\/(?!components\/)[^'"]+\.mdx['"];?\s*$/.test(importStatement)) {
      throw new Error(`Unsupported snippet import in ${sourcePath}: ${importStatement}`);
    }
  }

  const withoutCode = prepared
    .replace(/```[\s\S]*?```/g, '')
    .replace(/`[^`\n]*`/g, '')
    .replace(/^import\s+.+$/gm, '');
  const unknownComponents = new Set();
  for (const match of withoutCode.matchAll(/<([A-Z][A-Za-z0-9.]*)\b/g)) {
    if (!componentNames.has(match[1]) && !importedComponents.has(match[1])) {
      unknownComponents.add(match[1]);
    }
  }

  if (unknownComponents.size > 0) {
    throw new Error(
      `Unsupported MDX components in ${sourcePath}: ${[...unknownComponents].sort().join(', ')}`,
    );
  }

  return rewriteInlineCodeBraces(
    rewriteExplicitHeadingAnchors(rewriteReferenceLinks(prepared, routeRewrites)),
  ).trim();
}

function frontmatter(document) {
  return [
    '---',
    `title: ${JSON.stringify(document.title)}`,
    `description: ${JSON.stringify(document.description)}`,
    `route: ${JSON.stringify(document.route)}`,
    `stableId: ${JSON.stringify(document.id)}`,
    `entityKind: ${JSON.stringify(document.entityKind)}`,
    `sourcePath: ${JSON.stringify(document.sourcePath)}`,
    `contentHash: ${JSON.stringify(document.contentHash)}`,
    `aliases: ${JSON.stringify(document.aliases)}`,
    `legacyRoutes: ${JSON.stringify(document.legacyRoutes)}`,
    '---',
  ].join('\n');
}

async function main() {
  const artifactDirectory = parseArguments(process.argv.slice(2));
  const [
    manifest,
    documentPayload,
    navigation,
    routes,
    search,
    snippetPayload,
    sourceSiteConfig,
  ] =
    await Promise.all(
      [
        ...['manifest.json', 'documents.json', 'navigation.json', 'routes.json', 'search.json', 'snippets.json'].map(
          (fileName) => readJson(path.join(artifactDirectory, fileName)),
        ),
        readJson(path.join(sourceDocsDirectory, 'docs.json')),
      ],
    );

  if (
    manifest.schemaVersion !== 1
    || documentPayload.schemaVersion !== 1
    || navigation.schemaVersion !== 2
  ) {
    throw new Error(`Unsupported artifact schema version: ${manifest.schemaVersion}`);
  }
  if (manifest.documentCount !== documentPayload.documents.length) {
    throw new Error('Artifact document count does not match its manifest');
  }
  const expectedSearchRecordCount = manifest.searchRecordCount ?? manifest.documentCount;
  if (routes.routes.length !== manifest.documentCount || search.records.length !== expectedSearchRecordCount) {
    throw new Error('Artifact routes or search records do not match its manifest');
  }

  const expectedBundleHash = hash(
    `${JSON.stringify(documentPayload)}\n${JSON.stringify(snippetPayload.snippets)}\n${JSON.stringify(navigation)}\n${JSON.stringify(routes)}\n${JSON.stringify(search)}`,
  );
  if (manifest.bundleHash !== expectedBundleHash) {
    throw new Error('Artifact bundle hash does not match its contents');
  }

  const routeRewrites = new Map();
  function addRouteRewrite(oldRoute, newRoute) {
    const existing = routeRewrites.get(oldRoute);
    if (existing && existing !== newRoute) {
      throw new Error(`Legacy route ${oldRoute} maps to both ${existing} and ${newRoute}`);
    }
    if (oldRoute !== newRoute) routeRewrites.set(oldRoute, newRoute);
  }
  for (const document of documentPayload.documents) {
    if (/(^|\/)index\.mdx?$/.test(document.sourcePath)) {
      addRouteRewrite(`${document.route}/index`, document.route);
    }
    for (const legacyRoute of document.legacyRoutes ?? []) {
      if (legacyRoute.includes('#')) addRouteRewrite(legacyRoute, document.route);
    }
  }

  await rm(stagingDirectory, { recursive: true, force: true });
  await mkdir(path.join(stagingDirectory, 'mintlify/snippets'), { recursive: true });

  const docsConfig = mintlifyDocsConfig(
    documentPayload.documents,
    sourceSiteConfig,
    navigation,
  );
  const headerNavigation = await headerNavigationConfig(sourceSiteConfig);
  const versionConfig = versionsConfig(manifest, routes);
  const configuredPages = new Set();
  const collectConfiguredPages = (entries) => {
    for (const entry of entries) {
      if (typeof entry === 'string') configuredPages.add(entry);
      else collectConfiguredPages(entry.pages);
    }
  };
  collectConfiguredPages(docsConfig.navigation.tabs[0].groups);
  if (configuredPages.size !== manifest.documentCount) {
    throw new Error(
      `Mintlify navigation contains ${configuredPages.size} pages; expected ${manifest.documentCount}`,
    );
  }

  for (const document of documentPayload.documents) {
    if (hash(document.content) !== document.contentHash) {
      throw new Error(`Content hash mismatch for ${document.sourcePath}`);
    }

    const destination = path.join(
      stagingDirectory,
      'mintlify',
      `${pagePathFromRoute(document.route)}.mdx`,
    );
    const content = `${frontmatter(document)}\n\n${prepareMdx(document.content, document.sourcePath, routeRewrites)}\n`;
    await mkdir(path.dirname(destination), { recursive: true });
    await writeFile(destination, content, 'utf8');
  }

  for (const snippet of snippetPayload.snippets) {
    if (hash(snippet.content) !== snippet.contentHash) {
      throw new Error(`Content hash mismatch for snippet ${snippet.path}`);
    }

    const destination = path.join(stagingDirectory, 'mintlify/snippets', snippet.path);
    await mkdir(path.dirname(destination), { recursive: true });
    const preparedSnippet = prepareMdx(snippet.content, `snippets/${snippet.path}`, routeRewrites);
    await writeFile(destination, `${preparedSnippet}\n`, 'utf8');
  }

  await Promise.all([
    writeJson(path.join(stagingDirectory, 'data/manifest.json'), manifest),
    writeJson(path.join(stagingDirectory, 'data/header-navigation.json'), headerNavigation),
    writeJson(path.join(stagingDirectory, 'data/versions.json'), versionConfig),
    writeJson(path.join(stagingDirectory, 'mintlify/docs.json'), docsConfig),
    writeJson(path.join(stagingDirectory, 'public/docs/reference/_search/latest.json'), search),
    writeJson(
      path.join(stagingDirectory, 'public/docs/reference/_versions/versions.json'),
      versionConfig,
    ),
  ]);

  await Promise.all([
    mkdir(path.join(stagingDirectory, 'public/_site/logo'), { recursive: true }),
    mkdir(path.join(stagingDirectory, 'public/_site/customizations'), { recursive: true }),
    mkdir(path.join(stagingDirectory, 'public/images/icons'), { recursive: true }),
  ]);
  const headerIcons = [...new Set(
    headerNavigation.tabs.flatMap((tab) => tab.items?.map((item) => item.icon).filter(Boolean) ?? []),
  )];
  await Promise.all([
    copyFile(
      path.join(sourceDocsDirectory, '_site/favicon.svg'),
      path.join(stagingDirectory, 'public/_site/favicon.svg'),
    ),
    copyFile(
      path.join(sourceDocsDirectory, '_site/logo/dark.svg'),
      path.join(stagingDirectory, 'public/_site/logo/dark.svg'),
    ),
    copyFile(
      path.join(sourceDocsDirectory, '_site/logo/light.svg'),
      path.join(stagingDirectory, 'public/_site/logo/light.svg'),
    ),
    copyFile(
      path.join(sourceDocsDirectory, '_site/customizations/navbar-cta.js'),
      path.join(stagingDirectory, 'public/_site/customizations/navbar-cta.js'),
    ),
    ...headerIcons.map((icon) => copyFile(
      path.join(sourceDocsDirectory, icon.replace(/^\//, '')),
      path.join(stagingDirectory, 'public', icon.replace(/^\//, '')),
    )),
  ]);

  await rm(generatedDirectory, { recursive: true, force: true });
  await rename(stagingDirectory, generatedDirectory);
  console.log(
    `Prepared ${manifest.documentCount} Mintlify pages for Astro from bundle ${manifest.bundleHash}`,
  );
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exitCode = 1;
});
