#!/usr/bin/env node

import { execFileSync } from 'node:child_process';
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { rewriteGeneratedCodeBlocksInSource } from '../src/markdown/rewrite-generated-code-blocks.mjs';
import {
  entityAliases,
  referenceDescription,
  splitEntitySections,
} from './export-reference-docs.mjs';
import { resolveStatementPages } from './lib/statement-metadata.mjs';
import { loadStatementRegistrations } from './lib/statement-source.mjs';
import { findVersionSnapshotFile } from './lib/version-snapshot-files.mjs';

const scriptDirectory = path.dirname(fileURLToPath(import.meta.url));
const projectDirectory = path.resolve(scriptDirectory, '..');
const repositoryDirectory = path.resolve(projectDirectory, '..');
const temporaryRoot = path.join(repositoryDirectory, 'tmp');

function run(script, argumentsList) {
  execFileSync(process.execPath, [path.join(scriptDirectory, script), ...argumentsList], {
    cwd: projectDirectory,
    stdio: 'inherit',
  });
}

function requireFailure(script, argumentsList, expectedMessage) {
  try {
    execFileSync(process.execPath, [path.join(scriptDirectory, script), ...argumentsList], {
      cwd: projectDirectory,
      encoding: 'utf8',
      stdio: 'pipe',
    });
  } catch (error) {
    const output = `${error?.stdout ?? ''}${error?.stderr ?? ''}`;
    requireValue(output.includes(expectedMessage), `Unexpected failure from ${script}: ${output}`);
    return;
  }
  throw new Error(`${script} unexpectedly succeeded`);
}

function requireValue(condition, message) {
  if (!condition) throw new Error(message);
}

async function readJson(sourcePath) {
  return JSON.parse(await readFile(sourcePath, 'utf8'));
}

function findNavigationNode(node, id) {
  if (!node) return null;
  if (Array.isArray(node)) {
    for (const child of node) {
      const match = findNavigationNode(child, id);
      if (match) return match;
    }
    return null;
  }
  if (node.id === id || node.documentId === id) return node;
  return findNavigationNode(node.children, id);
}

function flattenNavigationNodes(node) {
  if (!node) return [];
  if (Array.isArray(node)) return node.flatMap(flattenNavigationNodes);
  return [node, ...flattenNavigationNodes(node.children)];
}

async function main() {
  requireValue(
    referenceDescription('statement', 'SELECT')
      === 'Reference documentation for the `SELECT` statement in ClickHouse.',
    'Statement descriptions are not deterministic',
  );
  requireValue(
    referenceDescription('system-table', 'system.query_log')
      === 'Reference documentation for the `system.query_log` system table in ClickHouse.',
    'System table descriptions are not deterministic',
  );

  const splitSections = splitEntitySections(
    [
      'Grouped aggregate functions.',
      '',
      '## sequenceMatch {#sequencematch}',
      '',
      'First function.',
      '',
      '## GROUPING SETS {#grouping-sets}',
      '',
      'Second function.',
    ].join('\n'),
    'aggregate-function',
    'functions/aggregate-functions/grouped.mdx',
    '/docs/reference/functions/aggregate-functions/grouped',
  );
  requireValue(
    splitSections.map(({ title }) => title).join(',') === 'sequenceMatch,GROUPING SETS',
    'Case-normalized entity headings were not split',
  );
  requireValue(
    splitEntitySections(
      '## Syntax {#syntax}\n\nExample.\n\n## Arguments {#arguments}\n\nExample.',
      'function',
      'functions/table-functions/example.mdx',
      '/docs/reference/functions/table-functions/example',
    ).length === 0,
    'Structural headings were incorrectly treated as entities',
  );
  const systemStatements = splitEntitySections(
    [
      'import { Badge } from "/snippets/components/Badge/Badge.jsx";',
      '',
      '## SYSTEM RELOAD DICTIONARIES {#reload-dictionaries}',
      '',
      'Reloads dictionaries. See [instrumentation](#instrument).',
      '',
      '## SYSTEM INSTRUMENT {#instrument}',
      '',
      'Controls instrumentation.',
      '',
      '### SYSTEM INSTRUMENT ADD {#instrument-add}',
      '',
      'Adds instrumentation.',
      '',
      '#### LOG {#instrument-add-log}',
      '',
      'Logs an event.',
      '',
      '### SYSTEM INSTRUMENT REMOVE {#instrument-remove}',
      '',
      'Removes instrumentation.',
      '',
      '## Managing Distributed Tables {#managing-distributed-tables}',
      '',
      'Distributed table controls.',
      '',
      '### SYSTEM FLUSH DISTRIBUTED {#flush-distributed}',
      '',
      'Flushes distributed data.',
      '',
      '### Privileges {#distributed-privileges}',
      '',
      'Required privileges.',
    ].join('\n'),
    'statement',
    'statements/system.mdx',
    '/docs/reference/statements/system',
  );
  requireValue(
    systemStatements.map(({ title }) => title).join(',')
      === 'SYSTEM RELOAD DICTIONARIES,SYSTEM INSTRUMENT,SYSTEM INSTRUMENT ADD,SYSTEM INSTRUMENT REMOVE,SYSTEM FLUSH DISTRIBUTED',
    '`SYSTEM` statements were not split from their generated source page',
  );
  requireValue(
    systemStatements.find(({ anchor }) => anchor === 'instrument-add')?.content.includes('## LOG {#instrument-add-log}'),
    'Nested `SYSTEM` statement headings were not promoted correctly',
  );
  requireValue(
    systemStatements.find(({ anchor }) => anchor === 'flush-distributed')?.navigationGroup.label
      === 'Managing Distributed Tables',
    '`SYSTEM` statement navigation groups were not retained',
  );
  requireValue(
    entityAliases(
      '## Aliases {#aliases}\n\nThe `Paimon` engine dispatches to `PaimonS3`, `PaimonAzure`, or `PaimonLocal` using the `disk` setting.',
      'Paimon',
    ).join(',') === 'PaimonS3,PaimonAzure,PaimonLocal',
    'Aliases section was not parsed',
  );

  const compoundCodeBlock = '<CodeBlock title="Query" className="language-sql"><_components.pre><_components.code className="language-sql">{"SELECT 1"}</_components.code></_components.pre></CodeBlock>';
  const rewrittenCodeBlock = await rewriteGeneratedCodeBlocksInSource(
    compoundCodeBlock,
    'smoke-test component',
  );
  requireValue(rewrittenCodeBlock.count === 1, 'Compound code block metadata prevented rewriting');
  requireValue(
    rewrittenCodeBlock.source.includes('code-block-with-header')
      && rewrittenCodeBlock.source.includes('{"Query"}'),
    'Compound code block title was not preserved',
  );

  await mkdir(temporaryRoot, { recursive: true });
  const temporaryDirectory = await mkdtemp(path.join(temporaryRoot, 'reference-pipeline-'));
  const artifactsDirectory = path.join(temporaryDirectory, 'artifacts');
  const artifactDirectory = path.join(artifactsDirectory, 'latest');
  const testArtifactDirectory = path.join(artifactsDirectory, '26.8');
  const generatedDirectory = path.join(temporaryDirectory, 'generated');
  const versionedGeneratedDirectory = path.join(temporaryDirectory, 'generated-26.8');

  try {
    const snapshotFixtureDirectory = path.join(temporaryDirectory, 'snapshots');
    const snapshotVersionDirectory = path.join(snapshotFixtureDirectory, '26.8');
    const snapshotHomepagePath = path.join(
      snapshotVersionDirectory,
      'docs/reference/versions/26.8/index.html',
    );
    const snapshotPagePath = path.join(
      snapshotVersionDirectory,
      'docs/reference/versions/26.8/formats/Parquet/Parquet/index.html',
    );
    const snapshotSearchPath = path.join(
      snapshotVersionDirectory,
      'docs/reference/_search/26.8.json',
    );
    const snapshotSitemapPath = path.join(
      snapshotVersionDirectory,
      'docs/reference/versions/26.8/sitemap.xml',
    );
    const snapshotAssetPath = path.join(snapshotVersionDirectory, '_astro/frozen.js');
    await Promise.all([
      mkdir(path.dirname(snapshotHomepagePath), { recursive: true }),
      mkdir(path.dirname(snapshotPagePath), { recursive: true }),
      mkdir(path.dirname(snapshotSearchPath), { recursive: true }),
      mkdir(path.dirname(snapshotSitemapPath), { recursive: true }),
      mkdir(path.dirname(snapshotAssetPath), { recursive: true }),
    ]);
    await Promise.all([
      writeFile(snapshotHomepagePath, '<h1>Frozen 26.8 reference</h1>'),
      writeFile(snapshotPagePath, '<h1>Frozen Parquet</h1>'),
      writeFile(snapshotSearchPath, '{"records":[]}'),
      writeFile(snapshotSitemapPath, '<urlset></urlset>'),
      writeFile(snapshotAssetPath, 'export const frozen = true;'),
    ]);
    for (const [requestUrl, expectedPath] of [
      ['/docs/reference/versions/26.8', snapshotHomepagePath],
      ['/docs/reference/versions/26.8/formats/Parquet/Parquet', snapshotPagePath],
      ['/docs/reference/_search/26.8.json', snapshotSearchPath],
      ['/docs/reference/versions/26.8/sitemap.xml', snapshotSitemapPath],
      ['/_astro/frozen.js', snapshotAssetPath],
    ]) {
      requireValue(
        (await findVersionSnapshotFile(requestUrl, snapshotFixtureDirectory))?.filePath
          === expectedPath,
        `Development server did not resolve snapshot request ${requestUrl}`,
      );
    }
    requireValue(
      await findVersionSnapshotFile(
        '/docs/reference/formats/Parquet/Parquet',
        snapshotFixtureDirectory,
      ) === null,
      'Development snapshot serving intercepted the mutable latest route',
    );

    run('export-reference-docs.mjs', ['--output', artifactDirectory]);
    const snapshotArguments = [
      '26.8',
      '--source', artifactDirectory,
      '--output', testArtifactDirectory,
      '--test-fixture',
    ];
    run('snapshot-reference-bundle.mjs', snapshotArguments);
    requireFailure(
      'snapshot-reference-bundle.mjs',
      snapshotArguments,
      'Reference bundle 26.8 already exists and is immutable',
    );
    run('prepare-content.mjs', [
      '--artifacts', artifactsDirectory,
      '--artifact', artifactDirectory,
      '--generated', generatedDirectory,
    ]);
    run('prepare-content.mjs', [
      '--artifacts', artifactsDirectory,
      '--artifact', testArtifactDirectory,
      '--generated', versionedGeneratedDirectory,
    ]);

    const documents = (await readJson(path.join(artifactDirectory, 'documents.json'))).documents;
    const search = (await readJson(path.join(artifactDirectory, 'search.json'))).records;
    const navigation = await readJson(path.join(artifactDirectory, 'navigation.json'));
    const versionedNavigation = await readJson(
      path.join(testArtifactDirectory, 'navigation.json'),
    );
    const legacyStatementRoutes = await readJson(
      path.join(projectDirectory, 'legacy-statement-routes.json'),
    );
    const statementRegistrations = await loadStatementRegistrations(repositoryDirectory);
    const generatedStatementPages = resolveStatementPages(
      statementRegistrations,
      legacyStatementRoutes,
    );
    const cleanSlateStatementPages = resolveStatementPages(
      statementRegistrations,
      { schemaVersion: 1, routes: {} },
    );
    requireValue(
      generatedStatementPages.every((page, index) => {
        const { legacyRoutes: _legacyRoutes, ...metadata } = page;
        const { legacyRoutes: _cleanLegacyRoutes, ...cleanMetadata }
          = cleanSlateStatementPages[index];
        return JSON.stringify(metadata) === JSON.stringify(cleanMetadata);
      }),
      'Legacy URL compatibility data changed generated statement metadata',
    );
    const redirects = (await readJson(path.join(generatedDirectory, 'data/redirects.json'))).redirects;
    const versions = await readJson(path.join(generatedDirectory, 'data/versions.json'));
    const selectPage = await readFile(
      path.join(generatedDirectory, 'mintlify/docs/reference/statements/select.mdx'),
      'utf8',
    );
    const systemTablePage = await readFile(
      path.join(
        generatedDirectory,
        'mintlify/docs/reference/system-tables/background_schedule_pool_log.mdx',
      ),
      'utf8',
    );
    const latestManifest = await readJson(path.join(artifactDirectory, 'manifest.json'));
    const testManifest = await readJson(path.join(testArtifactDirectory, 'manifest.json'));
    const versionedVersions = await readJson(
      path.join(versionedGeneratedDirectory, 'data/versions.json'),
    );
    const versionedRedirects = (
      await readJson(path.join(versionedGeneratedDirectory, 'data/redirects.json'))
    ).redirects;
    const versionedSelectPage = await readFile(
      path.join(
        versionedGeneratedDirectory,
        'mintlify/docs/reference/versions/26.8/statements/select.mdx',
      ),
      'utf8',
    );
    const versionedSearch = (
      await readJson(
        path.join(versionedGeneratedDirectory, 'public/docs/reference/_search/26.8.json'),
      )
    ).records;
    const sitemapIndex = await readFile(
      path.join(generatedDirectory, 'public/sitemap.xml'),
      'utf8',
    );
    const latestSitemap = await readFile(
      path.join(generatedDirectory, 'public/docs/reference/sitemap.xml'),
      'utf8',
    );
    const versionedSitemap = await readFile(
      path.join(
        versionedGeneratedDirectory,
        'public/docs/reference/versions/26.8/sitemap.xml',
      ),
      'utf8',
    );
    const docsLayout = await readFile(
      path.join(projectDirectory, 'src/layouts/DocsLayout.astro'),
      'utf8',
    );
    const inkeepTools = await readFile(
      path.join(projectDirectory, 'src/components/InkeepTools.astro'),
      'utf8',
    );
    const header = await readFile(
      path.join(projectDirectory, 'src/components/Header.astro'),
      'utf8',
    );
    const sidebar = await readFile(
      path.join(projectDirectory, 'src/components/Sidebar.astro'),
      'utf8',
    );
    const referenceIndex = await readFile(
      path.join(projectDirectory, 'src/components/ReferenceIndex.astro'),
      'utf8',
    );
    const referenceSearch = await readFile(
      path.join(projectDirectory, 'src/components/ReferenceSearch.astro'),
      'utf8',
    );
    const alterModifyQueryPage = await readFile(
      path.join(
        generatedDirectory,
        'mintlify/docs/reference/statements/alter/table/modify-query.mdx',
      ),
      'utf8',
    );
    const routes = new Set(documents.map((document) => document.route));
    const searchTitles = new Set(search.map((record) => record.title));
    const redirectMap = new Map(redirects.map(({ from, to }) => [from, to]));
    const systemTableDocument = documents.find(
      ({ id }) => id === 'reference:system-tables/background_schedule_pool_log',
    );
    const systemTableNavigation = findNavigationNode(
      navigation.root,
      'reference:system-tables/background_schedule_pool_log',
    );

    for (const fileName of ['kapa-init.js', 'ask-ai-button.js', 'inkeep-init.js']) {
      const [sourceCustomization, preparedCustomization] = await Promise.all([
        readFile(
          path.join(repositoryDirectory, 'docs/_site/customizations', fileName),
          'utf8',
        ),
        readFile(
          path.join(generatedDirectory, 'public/_site/customizations', fileName),
          'utf8',
        ),
      ]);
      requireValue(
        preparedCustomization === sourceCustomization,
        `Prepared customization ${fileName} differs from the docs-wide integration`,
      );
    }
    requireValue(
      inkeepTools.includes('id="search-bar-entry"')
        && inkeepTools.includes('Search...')
        && !inkeepTools.includes('Search reference')
        && docsLayout.indexOf('/_site/customizations/kapa-init.js')
          < docsLayout.indexOf('/_site/customizations/ask-ai-button.js')
        && docsLayout.indexOf('/_site/customizations/ask-ai-button.js')
          < docsLayout.indexOf('/_site/customizations/inkeep-init.js'),
      'The sidebar does not use the docs-wide Inkeep search and Kapa Ask AI entry points',
    );
    requireValue(
      referenceIndex.includes('<ReferenceSearch />')
        && referenceIndex.includes('wideContent')
        && !referenceIndex.includes('reference-stats')
        && referenceSearch.includes(
          'data-search-index={`/docs/reference/_search/${manifest.channel}.json`}',
        ),
      'The reference homepage does not use the selected artifact search index',
    );
    requireValue(
      header.includes('href={referenceBaseRoute}')
        && sidebar.includes('class="sidebar-home"')
        && sidebar.includes('href={referenceBaseRoute}')
        && sidebar.includes("currentRoute === referenceBaseRoute ? 'page' : undefined"),
      'The shell does not link to the homepage for the selected reference version',
    );

    requireValue(
      systemTableDocument?.title === 'system.background_schedule_pool_log'
        && systemTableNavigation?.label === 'background_schedule_pool_log'
        && systemTablePage.includes('title: "system.background_schedule_pool_log"')
        && systemTablePage.includes('sidebarTitle: "background_schedule_pool_log"')
        && searchTitles.has('system.background_schedule_pool_log'),
      'System table navigation labels do not omit only the `system.` prefix',
    );
    for (const [channel, channelNavigation] of [
      ['latest', navigation],
      ['26.8', versionedNavigation],
    ]) {
      const systemTables = findNavigationNode(
        channelNavigation.root,
        'reference.system-tables',
      );
      requireValue(
        flattenNavigationNodes(systemTables?.children).every((node) => (
          node.type !== 'document' || !String(node.label).startsWith('system.')
        )),
        `The ${channel} sidebar contains a system table label with the redundant prefix`,
      );
    }

    for (const [id, label] of [
      ['reference.functions.regular', 'Regular'],
      ['reference.functions.aggregate', 'Aggregate'],
      ['reference.functions.table', 'Table'],
      ['reference.functions.window', 'Window'],
      ['reference.source.functions.regular-functions.ai-functions', 'AI'],
      ['reference.source.functions.regular-functions.bit-functions', 'Bit'],
      [
        'reference.source.functions.regular-functions.string-search-functions',
        'Searching in Strings',
      ],
      ['reference.source.functions.regular-functions.string-functions', 'Strings'],
      ['reference.source.functions.regular-functions.time-series-functions', 'Time series'],
      ['reference.source.functions.regular-functions.ulid-functions', 'ULIDs'],
    ]) {
      requireValue(
        findNavigationNode(navigation.root, id)?.label === label,
        `Function navigation group ${id} does not use the concise label ${label}`,
      );
    }
    const functionNavigation = findNavigationNode(navigation.root, 'reference.functions');
    requireValue(
      flattenNavigationNodes(functionNavigation?.children).every((node) => (
        node.type !== 'group'
        || !/\bFunctions\b/i.test(String(node.label))
      )),
      'A nested function navigation group still includes the redundant word `Functions`',
    );

    requireValue(
      versions.schemaVersion === 2
        && versions.versions.map(({ id }) => id).join(',') === 'latest,26.8'
        && versions.versions.every((version) => (
          !('available' in version)
          && !('kind' in version)
          && !('description' in version)
        )),
      'Version selector catalog contains speculative or grouped entries',
    );
    requireValue(
      versions.versions.find(({ id }) => id === '26.8')
        ?.routes['reference:statement:select']
        === '/docs/reference/versions/26.8/statements/select',
      'The test bundle does not map stable document IDs to pinned routes',
    );
    requireValue(
      testManifest.channel === '26.8'
        && testManifest.testFixture === true
        && testManifest.snapshotOf === latestManifest.sourceRevision
        && testManifest.bundleHash === latestManifest.bundleHash,
      'The second bundle is not an explicit duplicate test fixture',
    );
    requireValue(
      JSON.stringify(versionedVersions) === JSON.stringify(versions)
        && versionedSelectPage.includes(
          'route: "/docs/reference/versions/26.8/statements/select"',
        )
        && versionedSearch.every((record) => (
          record.route.startsWith('/docs/reference/versions/26.8/')
        ))
        && versionedRedirects.every(({ from, to }) => (
          from.startsWith('/docs/reference/versions/26.8/')
          && to.startsWith('/docs/reference/versions/26.8/')
        )),
      'The second bundle was not prepared as an isolated versioned site',
    );
    requireValue(
      sitemapIndex.includes('<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">')
        && sitemapIndex.includes(
          '<loc>https://clickhouse.com/docs/reference/sitemap.xml</loc>',
        )
        && sitemapIndex.includes(
          '<loc>https://clickhouse.com/docs/reference/versions/26.8/sitemap.xml</loc>',
        ),
      'The root sitemap index does not point to latest and immutable version sitemaps',
    );
    requireValue(
      latestSitemap.includes('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">')
        && latestSitemap.includes('<loc>https://clickhouse.com/docs/reference</loc>')
        && latestSitemap.includes(
          '<loc>https://clickhouse.com/docs/reference/statements/select</loc>',
        )
        && !latestSitemap.includes('/docs/reference/versions/26.8/'),
      'The latest sitemap does not contain only current canonical reference routes',
    );
    requireValue(
      versionedSitemap.includes('<loc>https://clickhouse.com/docs/reference/versions/26.8</loc>')
        && versionedSitemap.includes(
          '<loc>https://clickhouse.com/docs/reference/versions/26.8/statements/select</loc>',
        )
        && docsLayout.includes(
          `manifest.channel !== 'latest' && <meta name="robots" content="noindex,follow" />`,
        ),
      'The immutable version is missing its sitemap or page-level `noindex` directive',
    );

    requireValue(
      documents.every((document) => (
        document.description === referenceDescription(document.entityKind, document.title)
      )),
      'A generated page description still depends on source prose or handwritten frontmatter',
    );

    requireValue(searchTitles.has('openSSL.client.requireTLSv1'), 'Dotted setting headings were not split');
    requireValue(searchTitles.has('PaimonS3'), 'Aliases from an unsplit page were not indexed');
    requireValue(
      redirectMap.has('/docs/reference/functions/regular-functions/uuid-functions'),
      'A split source route was not materialized',
    );
    requireValue(
      redirectMap.has('/docs/operations/system-tables/delta_lake_metadata_log'),
      'A legacy frontmatter route was not materialized',
    );
    requireValue(
      redirectMap.get('/docs/reference/statements/select/index')
        === '/docs/reference/statements/select',
      'The legacy `SELECT` index route was not retained after removing its Markdown source',
    );
    requireValue(
      redirectMap.get('/docs/reference/statements/alter/view')
        === '/docs/reference/statements/alter/table/modify-query',
      'A pre-generation statement route was not retained as a compatibility redirect',
    );

    const generatedStatements = documents.filter(
      (document) => document.sourcePath.startsWith('statements/'),
    );
    requireValue(
      generatedStatements.every((document) => routes.has(document.route)),
      'Autogenerated statement pages are missing from the artifact',
    );
    requireValue(
      routes.has('/docs/reference/statements/system')
        && routes.has('/docs/reference/statements/system/reload-dictionaries'),
      'The `SYSTEM` overview or individual statement pages are missing from the artifact',
    );
    requireValue(
      searchTitles.has('SYSTEM RELOAD DICTIONARIES'),
      'Individual `SYSTEM` statements are missing from search',
    );
    const documentById = new Map(documents.map((document) => [document.id, document]));
    const sourceStatementDocuments = generatedStatementPages
      .map(({ id }) => documentById.get(id));
    requireValue(
      sourceStatementDocuments.length === statementRegistrations.length
        && sourceStatementDocuments.length >= 100
        && sourceStatementDocuments.every((document) => (
          document?.sourcePath.startsWith('src/Parsers/')
          && document.content.length > 0
        )),
      'Statement pages still depend on generated Markdown under `docs/reference/statements`',
    );
    requireValue(
      documents.every((document) => (
        !document.sourcePath.startsWith('statements/')
        || document.sourcePath.startsWith('statements/create/dictionary/')
      )),
      'A source-owned statement Markdown page leaked into the clean-slate artifact',
    );
    requireValue(
      selectPage.includes('title: "SELECT"')
        && selectPage.includes('sidebarTitle: "Overview"')
        && selectPage.includes('description: "Reference documentation for the `SELECT` statement in ClickHouse."')
        && selectPage.includes('keywords: []')
        && selectPage.includes('doc_type: "reference"')
        && selectPage.includes('stableId: "reference:statement:select"')
        && selectPage.includes('parent: null')
        && selectPage.includes('sourcePath: "src/Parsers/ParserSelectQuery.cpp"'),
      'The source-generated `SELECT` page is missing generated frontmatter or provenance',
    );
    requireValue(
      findNavigationNode(navigation.root, 'reference.statements.system')?.children.length > 1,
      'Individual `SYSTEM` statements are missing from navigation',
    );
    const alterNavigation = findNavigationNode(navigation.root, 'reference.statements.alter');
    const alterTableNavigation = findNavigationNode(
      navigation.root,
      'reference.statements.alter.table',
    );
    requireValue(
      alterNavigation?.children[0]?.documentId === 'reference:statement:alter'
        && alterNavigation.children[1]?.id === 'reference.statements.alter.table',
      '`ALTER` navigation does not start with its overview and `ALTER TABLE` primitive',
    );
    requireValue(
      alterTableNavigation?.children.some((child) => child.label === '... COLUMN')
        && alterTableNavigation.children.some((child) => child.label === '... CONSTRAINT'),
      '`ALTER TABLE` variants are missing from their primitive navigation group',
    );
    requireValue(
      alterModifyQueryPage.includes('title: "ALTER TABLE ... MODIFY QUERY"')
        && alterModifyQueryPage.includes('sidebarTitle: "... MODIFY QUERY"'),
      '`ALTER TABLE` sidebar labels are not shortened independently of page titles',
    );
    requireValue(
      searchTitles.has('ALTER TABLE ... MODIFY QUERY'),
      '`ALTER TABLE ... MODIFY QUERY` lost its fully qualified search title',
    );
    const createNavigation = findNavigationNode(navigation.root, 'reference.statements.create');
    requireValue(
      createNavigation?.children[0]?.documentId === 'reference:statement:create'
        && createNavigation.children[1]?.id === 'reference.statements.create-dictionary'
        && createNavigation.children[2]?.id === 'reference.statements.create-table',
      '`CREATE` navigation does not follow its primitive hierarchy',
    );
    for (const title of ['sequenceMatch', 'GROUPING']) {
      const sourceContainsTitle = documents.some(
        (document) => document.sourcePath.includes('aggregate-functions')
          && document.content.includes(`## ${title} `),
      );
      requireValue(
        !sourceContainsTitle || searchTitles.has(title),
        `${title} was not split from its grouped source page`,
      );
    }
  } finally {
    await rm(temporaryDirectory, { recursive: true, force: true });
  }

  console.log('Reference artifact export and preparation smoke test passed');
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exitCode = 1;
});
