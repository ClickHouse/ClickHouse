import { readFile, readdir } from 'node:fs/promises';
import path from 'node:path';

const contentTypes = new Map([
  ['.css', 'text/css; charset=utf-8'],
  ['.html', 'text/html; charset=utf-8'],
  ['.ico', 'image/x-icon'],
  ['.js', 'text/javascript; charset=utf-8'],
  ['.json', 'application/json; charset=utf-8'],
  ['.map', 'application/json; charset=utf-8'],
  ['.md', 'text/markdown; charset=utf-8'],
  ['.png', 'image/png'],
  ['.svg', 'image/svg+xml'],
  ['.wasm', 'application/wasm'],
  ['.webp', 'image/webp'],
  ['.woff', 'font/woff'],
  ['.woff2', 'font/woff2'],
  ['.xml', 'application/xml; charset=utf-8'],
]);

export function staticContentType(filePath) {
  return contentTypes.get(path.extname(filePath)) ?? 'application/octet-stream';
}

export async function snapshotRoots(snapshotsDirectory) {
  let entries;
  try {
    entries = await readdir(snapshotsDirectory, { withFileTypes: true });
  } catch (error) {
    if (error?.code === 'ENOENT') return [];
    throw error;
  }
  return entries
    .filter((entry) => entry.isDirectory() && !entry.name.startsWith('.'))
    .sort((left, right) => left.name.localeCompare(right.name))
    .map((entry) => ({
      version: entry.name,
      directory: path.join(snapshotsDirectory, entry.name),
    }));
}

export function candidatesWithin(root, relativePath) {
  if (!relativePath) return [path.join(root, 'index.html')];
  const resolvedRoot = path.resolve(root);
  const resolved = path.resolve(resolvedRoot, relativePath);
  if (!resolved.startsWith(`${resolvedRoot}${path.sep}`)) return [];
  if (contentTypes.has(path.extname(relativePath))) return [resolved];
  return [path.join(resolved, 'index.html'), resolved];
}

export async function firstReadableFile(candidates) {
  for (const candidate of candidates) {
    try {
      return { content: await readFile(candidate), filePath: candidate };
    } catch (error) {
      if (error?.code !== 'ENOENT' && error?.code !== 'EISDIR') throw error;
    }
  }
  return null;
}

function requestedSnapshotVersion(relativePath) {
  return relativePath.match(/^docs\/reference\/versions\/([^/]+)(?:\/|$)/)?.[1]
    ?? relativePath.match(/^docs\/reference\/_search\/([^/]+)\.json$/)?.[1]
    ?? null;
}

export async function findVersionSnapshotFile(requestUrl, snapshotsDirectory) {
  const pathname = decodeURIComponent(new URL(requestUrl, 'http://localhost').pathname);
  const relativePath = pathname.replace(/^\/+/, '');
  if (relativePath.split('/').includes('..')) return null;

  const requestedVersion = requestedSnapshotVersion(relativePath);
  const isSnapshotAsset = relativePath.startsWith('_astro/');
  if (!requestedVersion && !isSnapshotAsset) return null;

  const snapshots = await snapshotRoots(snapshotsDirectory);
  const roots = requestedVersion
    ? snapshots
      .filter(({ version }) => version === requestedVersion)
      .map(({ directory }) => directory)
    : snapshots.map(({ directory }) => directory);
  return firstReadableFile(roots.flatMap((root) => candidatesWithin(root, relativePath)));
}

export default function versionSnapshotFiles({ snapshotsDirectory }) {
  return {
    name: 'clickhouse-reference-version-snapshots',
    configureServer(server) {
      server.middlewares.use(async (request, response, next) => {
        if (request.method !== 'GET' && request.method !== 'HEAD') {
          next();
          return;
        }

        try {
          const file = await findVersionSnapshotFile(request.url ?? '/', snapshotsDirectory);
          if (!file) {
            next();
            return;
          }
          response.writeHead(200, {
            'Cache-Control': 'no-store',
            'Content-Type': staticContentType(file.filePath),
          });
          response.end(request.method === 'HEAD' ? undefined : file.content);
        } catch (error) {
          next(error);
        }
      });
    },
  };
}
