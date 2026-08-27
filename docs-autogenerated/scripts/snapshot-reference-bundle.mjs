#!/usr/bin/env node

import { access, cp, mkdir, readFile, rename, rm, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDirectory = path.dirname(fileURLToPath(import.meta.url));
const projectDirectory = path.resolve(scriptDirectory, '..');

function parseArguments(argv) {
  const version = argv[0];
  if (
    !version
    || !/^[a-z0-9][a-z0-9.-]*$/.test(version)
    || version === 'latest'
  ) {
    throw new Error(
      'Usage: node scripts/snapshot-reference-bundle.mjs <version> '
        + '[--source <directory>] [--output <directory>] [--test-fixture]',
    );
  }

  const options = {
    version,
    source: path.join(projectDirectory, '.artifacts/latest'),
    output: path.join(projectDirectory, `.artifacts/${version}`),
    testFixture: false,
  };
  const names = new Map([
    ['--source', 'source'],
    ['--output', 'output'],
  ]);
  for (let index = 1; index < argv.length; index += 1) {
    if (argv[index] === '--test-fixture') {
      options.testFixture = true;
      continue;
    }
    const name = names.get(argv[index]);
    if (!name || index + 1 >= argv.length) {
      throw new Error(`Unknown or incomplete argument: ${argv[index]}`);
    }
    options[name] = argv[index + 1];
    index += 1;
  }
  options.source = path.resolve(options.source);
  options.output = path.resolve(options.output);
  return options;
}

async function exists(target) {
  try {
    await access(target);
    return true;
  } catch (error) {
    if (error?.code === 'ENOENT') return false;
    throw error;
  }
}

async function main() {
  const options = parseArguments(process.argv.slice(2));
  if (await exists(options.output)) {
    throw new Error(`Reference bundle ${options.version} already exists and is immutable`);
  }

  const stagingDirectory = `${options.output}.next`;
  const manifest = JSON.parse(
    await readFile(path.join(options.source, 'manifest.json'), 'utf8'),
  );
  if (manifest.channel !== 'latest') {
    throw new Error(`Snapshot source must be latest, received ${manifest.channel}`);
  }

  await rm(stagingDirectory, { recursive: true, force: true });
  await mkdir(path.dirname(options.output), { recursive: true });
  await cp(options.source, stagingDirectory, { recursive: true });
  const snapshotManifest = {
    ...manifest,
    channel: options.version,
    snapshotOf: manifest.sourceRevision,
    ...(options.testFixture ? { testFixture: true } : {}),
  };
  await writeFile(
    path.join(stagingDirectory, 'manifest.json'),
    `${JSON.stringify(snapshotManifest, null, 2)}\n`,
    'utf8',
  );
  await rename(stagingDirectory, options.output);
  console.log(`Snapshotted latest reference bundle as immutable version ${options.version}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exitCode = 1;
});
