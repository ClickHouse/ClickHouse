#!/usr/bin/env node

import { spawn } from 'node:child_process';
import { readFile, rm } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDirectory = path.dirname(fileURLToPath(import.meta.url));
const projectDirectory = path.resolve(scriptDirectory, '..');
const artifactsDirectory = path.join(projectDirectory, '.artifacts');
const generatedDirectory = path.join(projectDirectory, 'src/generated');
const astroBinary = path.join(projectDirectory, 'node_modules/astro/bin/astro.mjs');

function parseArguments(argv) {
  const options = {
    artifactDirectory: path.join(artifactsDirectory, 'latest'),
    artifactsDirectory,
    outputDirectory: path.join(projectDirectory, 'dist'),
  };
  const names = new Map([
    ['--artifact', 'artifactDirectory'],
    ['--artifacts', 'artifactsDirectory'],
    ['--output', 'outputDirectory'],
  ]);

  for (let index = 0; index < argv.length; index += 2) {
    const name = names.get(argv[index]);
    if (!name || index + 1 >= argv.length) {
      throw new Error(
        'Usage: node scripts/build-reference-site.mjs '
          + '[--artifact <directory>] [--artifacts <directory>] [--output <directory>]',
      );
    }
    options[name] = path.resolve(argv[index + 1]);
  }
  return options;
}

function runNode(argumentsList) {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, argumentsList, {
      cwd: projectDirectory,
      stdio: 'inherit',
    });
    child.once('error', reject);
    child.once('exit', (code, signal) => {
      if (code === 0) resolve();
      else reject(new Error(`Command failed with ${signal ? `signal ${signal}` : `exit code ${code}`}`));
    });
  });
}

async function main() {
  const options = parseArguments(process.argv.slice(2));
  const manifest = JSON.parse(
    await readFile(path.join(options.artifactDirectory, 'manifest.json'), 'utf8'),
  );
  console.log(`\nBuilding only reference bundle ${manifest.channel}`);
  await runNode([
    path.join(scriptDirectory, 'prepare-content.mjs'),
    '--artifact', options.artifactDirectory,
    '--artifacts', options.artifactsDirectory,
    '--generated', generatedDirectory,
  ]);
  await rm(path.join(projectDirectory, '.astro'), { recursive: true, force: true });
  await rm(path.join(projectDirectory, '.mintlify'), { recursive: true, force: true });
  await runNode([
    '--max-old-space-size=8192',
    astroBinary,
    'build',
    '--outDir', options.outputDirectory,
  ]);
  console.log(`\nBuilt reference bundle ${manifest.channel} in ${options.outputDirectory}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exitCode = 1;
});
