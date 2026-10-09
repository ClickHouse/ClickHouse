#!/usr/bin/env node

import { createServer } from 'node:http';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import {
  candidatesWithin,
  firstReadableFile,
  snapshotRoots,
  staticContentType,
} from './lib/version-snapshot-files.mjs';

const projectDirectory = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const distributionDirectory = path.join(projectDirectory, 'dist');
const snapshotsDirectory = path.join(projectDirectory, '.snapshots');

function option(name, fallback) {
  const index = process.argv.indexOf(name);
  return index === -1 ? fallback : process.argv[index + 1];
}

const host = option('--host', '127.0.0.1');
const port = Number(option('--port', '4321'));
if (!Number.isInteger(port) || port < 1 || port > 65535) {
  throw new Error(`Invalid preview port: ${port}`);
}

async function requestCandidates(requestUrl) {
  const pathname = decodeURIComponent(new URL(requestUrl, `http://${host}:${port}`).pathname);
  const relativePath = pathname.replace(/^\/+/, '');
  if (relativePath.split('/').includes('..')) return [];

  const snapshots = await snapshotRoots(snapshotsDirectory);
  const requestedVersion = relativePath.match(
    /^docs\/reference\/versions\/([^/]+)(?:\/|$)/,
  )?.[1];
  const orderedRoots = requestedVersion
    ? [
      ...snapshots
        .filter(({ version }) => version === requestedVersion)
        .map(({ directory }) => directory),
      distributionDirectory,
    ]
    : [distributionDirectory, ...snapshots.map(({ directory }) => directory)];
  return orderedRoots.flatMap((root) => candidatesWithin(root, relativePath));
}

const server = createServer(async (request, response) => {
  try {
    if (request.method !== 'GET' && request.method !== 'HEAD') {
      response.writeHead(405, { Allow: 'GET, HEAD' });
      response.end();
      return;
    }

    const file = await firstReadableFile(await requestCandidates(request.url ?? '/'));
    if (!file) {
      response.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
      response.end('Not found\n');
      return;
    }

    response.writeHead(200, {
      'Cache-Control': 'no-store',
      'Content-Type': staticContentType(file.filePath),
    });
    response.end(request.method === 'HEAD' ? undefined : file.content);
  } catch (error) {
    response.writeHead(500, { 'Content-Type': 'text/plain; charset=utf-8' });
    response.end(`${error instanceof Error ? error.message : error}\n`);
  }
});

server.listen(port, host, () => {
  console.log(`Serving latest plus immutable version snapshots at http://${host}:${port}`);
});
