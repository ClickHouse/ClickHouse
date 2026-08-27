#!/usr/bin/env node

import { execFileSync } from 'node:child_process';
import { mkdir, mkdtemp, readFile, rm } from 'node:fs/promises';
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
  if (node.id === id) return node;
  return findNavigationNode(node.children, id);
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
