#!/usr/bin/env node

import { access, mkdir, mkdtemp, readFile, rename, rm } from 'node:fs/promises';
import { spawn } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDirectory = path.dirname(fileURLToPath(import.meta.url));
const projectDirectory = path.resolve(scriptDirectory, '..');
const artifactsDirectory = path.join(projectDirectory, '.artifacts');
const snapshotsDirectory = path.join(projectDirectory, '.snapshots');

function parseArguments(argv) {
  const version = argv[0];
  if (
    !version
    || !/^[a-z0-9][a-z0-9.-]*$/.test(version)
    || version === 'latest'
  ) {
    throw new Error(
      'Usage: node scripts/snapshot-reference-release.mjs <version> [--test-fixture]',
    );
  }
  const flags = new Set(argv.slice(1));
  if ([...flags].some((flag) => flag !== '--test-fixture')) {
    const unknown = [...flags].find((flag) => flag !== '--test-fixture');
    throw new Error(`Unknown argument: ${unknown}`);
  }
  return { version, testFixture: flags.has('--test-fixture') };
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
  const artifactDirectory = path.join(artifactsDirectory, options.version);
  const snapshotDirectory = path.join(snapshotsDirectory, options.version);
  if (await exists(snapshotDirectory)) {
    throw new Error(`Static reference snapshot ${options.version} already exists and is immutable`);
  }

  if (!(await exists(artifactDirectory))) {
    const snapshotArguments = [
      path.join(scriptDirectory, 'snapshot-reference-bundle.mjs'),
      options.version,
    ];
    if (options.testFixture) snapshotArguments.push('--test-fixture');
    await runNode(snapshotArguments);
  }
  const manifest = JSON.parse(
    await readFile(path.join(artifactDirectory, 'manifest.json'), 'utf8'),
  );
  if (manifest.channel !== options.version) {
    throw new Error(`Reference bundle channel ${manifest.channel} does not match ${options.version}`);
  }
  if (options.testFixture && manifest.testFixture !== true) {
    throw new Error(`Reference bundle ${options.version} is not marked as a test fixture`);
  }

  await mkdir(snapshotsDirectory, { recursive: true });
  const stagingDirectory = await mkdtemp(
    path.join(snapshotsDirectory, `.${options.version}-`),
  );
  try {
    await runNode([
      path.join(scriptDirectory, 'build-reference-site.mjs'),
      '--artifact', artifactDirectory,
      '--artifacts', artifactsDirectory,
      '--output', stagingDirectory,
    ]);
    await rename(stagingDirectory, snapshotDirectory);
  } catch (error) {
    await rm(stagingDirectory, { recursive: true, force: true });
    throw error;
  }
  console.log(`Published immutable static reference snapshot ${options.version}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exitCode = 1;
});
