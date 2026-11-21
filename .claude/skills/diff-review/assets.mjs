// The UI's dependencies, pinned by sha256 in vendor/manifest.json.
//
// The node polyfills are a few KB each and ship in vendor/. The @pierre/diffs
// bundle is 11 MB of build output, too much to carry in git history, so it is
// fetched once from its pinned URL into a cache under ~/.cache/diff-review and
// reused from there. Where an asset comes from changes nothing about what
// reaches the browser: its sha256 is checked against the manifest on every
// start — for a cache hit as much as for a fresh download — and a mismatch or a
// failed fetch stops the server instead of serving whatever arrived. So a
// compromised CDN cannot reach the diff under review; it can only take the
// review offline.
//
// Resolution order per asset: vendor/, then the cache, then the network. The
// first step is what makes a machine work offline — drop an audited copy into
// vendor/, or run `server.mjs --prefetch` once while there is a network.

import { createHash } from 'node:crypto';
import { gzipSync, gunzipSync } from 'node:zlib';
import { mkdirSync, readFileSync, renameSync, unlinkSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { homedir } from 'node:os';

const __dirname = dirname(fileURLToPath(import.meta.url));
export const VENDOR_DIR = join(__dirname, 'vendor');
const FETCH_TIMEOUT_MS = 120_000;

const sha256 = (buf) => createHash('sha256').update(buf).digest('hex');

export function cacheDir() {
  if (process.env.DIFF_REVIEW_CACHE) return resolve(process.env.DIFF_REVIEW_CACHE);
  const xdg = process.env.XDG_CACHE_HOME;
  return join(xdg?.startsWith('/') ? xdg : join(homedir(), '.cache'), 'diff-review');
}

/// Where an asset may be found. The cache name carries its hash, so upgrading a
/// pin lands on a different file rather than on a stale one, and downgrading
/// finds the old file still there.
const vendorName = (a) => `${a.name}.mjs${a.gzip ? '.gz' : ''}`;
const cacheName = (a) => `${a.name}-${a.sha256.slice(0, 16)}.mjs${a.gzip ? '.gz' : ''}`;

function assets() {
  return JSON.parse(readFileSync(join(VENDOR_DIR, 'manifest.json'), 'utf8')).assets;
}

/// The stored bytes of `path` if they hold exactly the module the manifest pins,
/// else null. Gzipped assets are verified through the decompression, so the hash
/// checked is always the audited one and never our own compressor's output.
function readIfVerified(path, asset) {
  try {
    const stored = readFileSync(path);
    const raw = asset.gzip ? gunzipSync(stored) : stored;
    return raw.length === asset.bytes && sha256(raw) === asset.sha256 ? stored : null;
  } catch {
    return null;
  }
}

/// The audited bytes from the first URL that serves them, plus a reason per URL
/// that did not. A hash mismatch is reported, never retried: either the pinned
/// build changed under the URL or something answered in its place, and both need
/// a human to look before the code runs.
async function download(asset, log) {
  const errors = [];
  for (const url of asset.urls ?? []) {
    log(`diff-review: fetching ${asset.name} (${(asset.bytes / 1e6).toFixed(1)} MB) from ${url}\n`);
    let body;
    try {
      const res = await fetch(url, { signal: AbortSignal.timeout(FETCH_TIMEOUT_MS) });
      if (!res.ok) {
        errors.push(`${url}: HTTP ${res.status} ${res.statusText}`);
        continue;
      }
      body = Buffer.from(await res.arrayBuffer());
    } catch (err) {
      errors.push(`${url}: ${err.message}`);
      continue;
    }
    if (body.length !== asset.bytes || sha256(body) !== asset.sha256) {
      errors.push(
        `${url}: got sha256 ${sha256(body)} (${body.length} bytes), ` +
          `manifest pins ${asset.sha256} (${asset.bytes} bytes)`
      );
      continue;
    }
    return { raw: body, errors };
  }
  return { raw: null, errors };
}

/// Renamed into place, so a torn or partial write is never picked up as a cache
/// hit by the next run.
function store(dir, name, buf) {
  const path = join(dir, name);
  const tmp = join(dir, `.${name}.${process.pid}.tmp`);
  mkdirSync(dir, { recursive: true });
  try {
    writeFileSync(tmp, buf);
    renameSync(tmp, path);
  } catch (err) {
    try {
      unlinkSync(tmp);
    } catch {}
    throw err;
  }
  return path;
}

export class AssetError extends Error {}

/// Every asset the UI imports, keyed by the path it is served at, with the bytes
/// to serve — compressed where `gzip` says the browser can take them that way.
/// Throws AssetError naming the asset and every failed source: the server has no
/// business opening a review it cannot render.
export async function resolveAssets({ allowFetch = true, log = () => {} } = {}) {
  const cache = cacheDir();
  const resolved = new Map();
  for (const asset of assets()) {
    const local = join(VENDOR_DIR, vendorName(asset));
    let body = readIfVerified(local, asset);
    let from = body != null ? `vendor/${vendorName(asset)}` : null;
    if (body == null && asset.urls != null) {
      body = readIfVerified(join(cache, cacheName(asset)), asset);
      from = body != null ? join(cache, cacheName(asset)) : null;
    }
    if (body == null && asset.urls == null) {
      throw new AssetError(
        `vendor/${vendorName(asset)} is missing or does not match its pinned ` +
          `sha256 ${asset.sha256}`
      );
    }
    if (body == null && !allowFetch) {
      throw new AssetError(`${asset.name} is not in vendor/ or ${cache}, and fetching is off`);
    }
    if (body == null) {
      const { raw, errors } = await download(asset, log);
      if (raw == null) {
        throw new AssetError(
          `cannot obtain ${asset.name}:\n` + errors.map((e) => `    - ${e}`).join('\n')
        );
      }
      body = asset.gzip ? gzipSync(raw, { level: 9 }) : raw;
      // A cache we cannot write costs the next run a download, nothing more: the
      // bytes in hand are already verified, so the review goes ahead either way.
      try {
        from = store(cache, cacheName(asset), body);
        log(`diff-review: verified and cached at ${from}\n`);
      } catch (err) {
        from = '(uncached)';
        log(`diff-review: cannot cache ${asset.name}: ${err.message}; will fetch again\n`);
      }
    }
    resolved.set(asset.servePath, {
      body,
      gzip: asset.gzip === true,
      type: 'text/javascript; charset=utf-8',
      from,
    });
  }
  return resolved;
}
