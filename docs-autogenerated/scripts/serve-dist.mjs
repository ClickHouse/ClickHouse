#!/usr/bin/env node

import { createServer } from 'node:http';
import { readFile, readdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

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

const contentTypes = new Map([
  ['.css', 'text/css; charset=utf-8'],
  ['.html', 'text/html; charset=utf-8'],
  ['.ico', 'image/x-icon'],
  ['.js', 'text/javascript; charset=utf-8'],
  ['.json', 'application/json; charset=utf-8'],
  ['.map', 'application/json; charset=utf-8'],
  ['.png', 'image/png'],
  ['.svg', 'image/svg+xml'],
  ['.wasm', 'application/wasm'],
  ['.webp', 'image/webp'],
  ['.woff', 'font/woff'],
  ['.woff2', 'font/woff2'],
]);

async function snapshotRoots() {
  let entries;
  try {
    entries = await readdir(snapshotsDirectory, { withFileTypes: true });
  } catch (error) {
    if (error?.code === 'ENOENT') return [];
    throw error;
  }
  return entries
    .filter((entry) => entry.isDirectory() && !entry.name.startsWith('.'))
    .map((entry) => ({
      version: entry.name,
      directory: path.join(snapshotsDirectory, entry.name),
    }));
}

function candidatesWithin(root, relativePath) {
  if (!relativePath) return [path.join(root, 'index.html')];
  const resolved = path.resolve(root, relativePath);
  if (!resolved.startsWith(`${root}${path.sep}`)) return [];
  if (path.extname(relativePath)) return [resolved];
  return [path.join(resolved, 'index.html'), resolved];
}

async function requestCandidates(requestUrl) {
  const pathname = decodeURIComponent(new URL(requestUrl, `http://${host}:${port}`).pathname);
  const relativePath = pathname.replace(/^\/+/, '');
  if (relativePath.split('/').includes('..')) return [];

  const snapshots = await snapshotRoots();
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

async function firstReadableFile(candidates) {
  for (const candidate of candidates) {
    try {
      return { content: await readFile(candidate), filePath: candidate };
    } catch (error) {
      if (error?.code !== 'ENOENT' && error?.code !== 'EISDIR') throw error;
    }
  }
  return null;
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
      'Content-Type': contentTypes.get(path.extname(file.filePath)) ?? 'application/octet-stream',
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
