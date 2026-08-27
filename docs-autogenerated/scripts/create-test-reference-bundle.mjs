#!/usr/bin/env node

import { cp, readFile, rename, rm, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDirectory = path.dirname(fileURLToPath(import.meta.url));
const projectDirectory = path.resolve(scriptDirectory, '..');

function parseArguments(argv) {
  const options = {
    source: path.join(projectDirectory, '.artifacts/latest'),
    output: path.join(projectDirectory, '.artifacts/26.8'),
    channel: '26.8',
  };
  const names = new Map([
    ['--source', 'source'],
    ['--output', 'output'],
    ['--channel', 'channel'],
  ]);

  for (let index = 0; index < argv.length; index += 2) {
    const name = names.get(argv[index]);
    if (!name || index + 1 >= argv.length) {
      throw new Error(
        'Usage: node scripts/create-test-reference-bundle.mjs [--source <directory>] [--output <directory>] [--channel <version>]',
      );
    }
    options[name] = argv[index + 1];
  }

  if (!/^[a-z0-9][a-z0-9.-]*$/.test(options.channel) || options.channel === 'latest') {
    throw new Error(`Invalid test bundle version: ${options.channel}`);
  }
  options.source = path.resolve(options.source);
  options.output = path.resolve(options.output);
  return options;
}

async function main() {
  const options = parseArguments(process.argv.slice(2));
  const stagingDirectory = `${options.output}.next`;
  const manifest = JSON.parse(await readFile(path.join(options.source, 'manifest.json'), 'utf8'));
  if (manifest.channel !== 'latest') {
    throw new Error(`Test bundle source must be latest, received ${manifest.channel}`);
  }

  await rm(stagingDirectory, { recursive: true, force: true });
  await cp(options.source, stagingDirectory, { recursive: true });
  await writeFile(
    path.join(stagingDirectory, 'manifest.json'),
    `${JSON.stringify({ ...manifest, channel: options.channel, testFixture: true }, null, 2)}\n`,
    'utf8',
  );
  await rm(options.output, { recursive: true, force: true });
  await rename(stagingDirectory, options.output);
  console.log(`Created test reference bundle ${options.channel} from latest`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exitCode = 1;
});
