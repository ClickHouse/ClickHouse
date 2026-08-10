#!/usr/bin/env node
// diff-review: serve pending git changes for in-browser review and collect the
// user's comments. Zero npm dependencies; the UI renders with a vendored
// @pierre/diffs bundle (see vendor/README.md) — nothing is loaded from a CDN.
//
// Usage:
//   node server.mjs [--repo <path>] [--base <ref>] [--committed] [--port 3000] [--out <file>] [--no-open] [--force]
//
// By default the working tree (staged + unstaged + untracked) is diffed against
// --base. With --committed, <base>..HEAD is diffed instead and the working tree
// is ignored — use it to review already-committed branch work.
//
// Exits 0 after the user submits their review (comments written to --out),
// exits 3 when there is nothing to review, exits 4 when this machine has no
// browser that could reach the server, exits 1 on errors (e.g. port busy).

import { createServer } from 'node:http';
import { spawnSync, spawn } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { gunzipSync } from 'node:zlib';
import { readFileSync, writeFileSync, lstatSync, readlinkSync, existsSync } from 'node:fs';
import { join, dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { tmpdir } from 'node:os';

const __dirname = dirname(fileURLToPath(import.meta.url));
const args = process.argv.slice(2);
const argVal = (name, dflt) => {
  const i = args.indexOf(name);
  return i >= 0 && args[i + 1] != null ? args[i + 1] : dflt;
};

const PORT = Number(argVal('--port', '3000'));
const BASE = argVal('--base', 'HEAD');
const REPO = resolve(argVal('--repo', process.cwd()));
const OUT = resolve(argVal('--out', join(tmpdir(), `diff-review-${Date.now()}.json`)));
const NO_OPEN = args.includes('--no-open');
const COMMITTED = args.includes('--committed');
const FORCE = args.includes('--force') || process.env.DIFF_REVIEW_FORCE === '1';
const MAX_FILE_BYTES = 2_000_000;

// ── Refuse to serve where nobody can look ────────────────────────────────────
// The server binds to loopback, so the review UI is only reachable from a
// browser running on this very host. On an isolated VM — a cloud instance we
// reach over SSH, a headless container — there is no such browser. Starting a
// server there helps nobody: it binds a port and leaves a background task
// waiting for a review that can never be submitted. Decide this before doing
// any work at all, so we neither shell out to git nor read the diff.
function detectNoReachableBrowser() {
  const reasons = [];

  // A remote shell: the user's browser runs on the machine at the other end of
  // the connection, and that machine cannot reach our loopback socket.
  if (process.env.SSH_CONNECTION || process.env.SSH_CLIENT || process.env.SSH_TTY)
    reasons.push('this is an SSH session (SSH_CONNECTION/SSH_CLIENT/SSH_TTY is set)');

  // A cloud instance. cloud-init state is the cheapest reliable marker, and the
  // DMI vendor strings cover images that clean it up. Both are plain file
  // reads. Deliberately not IMDS: querying 169.254.169.254 costs a network
  // round trip (two, under IMDSv2) and hangs where the link-local route is
  // firewalled off, to report what the DMI strings already say for free.
  const cloudInitMarker = ['/var/lib/cloud/instance', '/run/cloud-init/instance-data.json'].find(
    (p) => existsSync(p)
  );
  if (cloudInitMarker) {
    reasons.push(`cloud-init state is present (${cloudInitMarker})`);
  } else {
    const CLOUD_VENDORS =
      /amazon ec2|google|microsoft corporation|digitalocean|hetzner|openstack|alibaba cloud|oraclecloud|scaleway|vultr/i;
    for (const field of ['sys_vendor', 'chassis_asset_tag', 'board_vendor']) {
      let value;
      try {
        value = readFileSync(`/sys/class/dmi/id/${field}`, 'utf8').trim();
      } catch {
        continue;
      }
      if (CLOUD_VENDORS.test(value)) {
        reasons.push(`DMI ${field} reads "${value}", so this is a cloud instance`);
        break;
      }
    }
  }

  // No graphical session: nothing here can open a browser even locally. macOS
  // and Windows always have a window server, so this only applies to Linux.
  if (process.platform === 'linux' && !process.env.DISPLAY && !process.env.WAYLAND_DISPLAY)
    reasons.push('there is no graphical session (DISPLAY and WAYLAND_DISPLAY are both unset)');

  return reasons;
}

const noBrowserReasons = FORCE ? [] : detectNoReachableBrowser();
if (noBrowserReasons.length > 0) {
  process.stderr.write(
    'diff-review: not starting a server, because no browser on this machine could reach it:\n' +
      noBrowserReasons.map((r) => `  - ${r}\n`).join('') +
      'diff-review: show the diff in the terminal instead, with git diff or git show.\n' +
      'diff-review: --force overrides this, and is only for a user who forwarded the port themselves.\n'
  );
  process.exit(4);
}

// Per-session secret: embedded into the served page and required on /submit, so
// that a submission can only come from the UI this server handed out — not from
// a random cross-origin tab or a blind POST to localhost.
const NONCE = randomBytes(16).toString('hex');

function git(gitArgs, opts = {}) {
  return spawnSync('git', ['-C', ROOT ?? REPO, ...gitArgs], {
    maxBuffer: 256 * 1024 * 1024,
    ...opts,
  });
}
function gitOrDie(gitArgs) {
  const r = git(gitArgs);
  if (r.status !== 0) {
    process.stderr.write(`diff-review: git ${gitArgs.join(' ')} failed:\n${r.stderr}`);
    process.exit(1);
  }
  return r.stdout;
}

let ROOT = null;
ROOT = gitOrDie(['rev-parse', '--show-toplevel']).toString('utf8').trim();

// ── Collect changed files ────────────────────────────────────────────────────
// Default: worktree vs BASE, plus untracked. With --committed: BASE..HEAD only,
// so dirty/untracked local edits never leak into a review of committed work.
function collectFiles() {
  const entries = []; // { path, oldPath, status }
  const diffArgs = COMMITTED
    ? ['diff', '--name-status', '-z', BASE, 'HEAD']
    : ['diff', '--name-status', '-z', BASE];
  const tokens = gitOrDie(diffArgs)
    .toString('utf8')
    .split('\0')
    .filter((t) => t.length > 0);
  for (let i = 0; i < tokens.length; ) {
    const status = tokens[i++];
    if (status.startsWith('R') || status.startsWith('C')) {
      const oldPath = tokens[i++];
      const newPath = tokens[i++];
      entries.push({ path: newPath, oldPath, status: status[0] });
    } else {
      entries.push({ path: tokens[i++], oldPath: null, status: status[0] });
    }
  }
  if (!COMMITTED) {
    const untracked = gitOrDie(['ls-files', '--others', '--exclude-standard', '-z'])
      .toString('utf8')
      .split('\0')
      .filter((t) => t.length > 0);
    for (const p of untracked) entries.push({ path: p, oldPath: null, status: '?' });
  }

  const files = [];
  const skipped = [];
  const looksBinary = (buf) => buf.subarray(0, 8192).includes(0);
  for (const e of entries) {
    let oldBuf = Buffer.alloc(0);
    if (e.status !== 'A' && e.status !== '?') {
      const r = git(['show', `${BASE}:${e.oldPath ?? e.path}`]);
      if (r.status === 0) oldBuf = r.stdout;
    }
    let newBuf = Buffer.alloc(0);
    if (e.status !== 'D') {
      if (COMMITTED) {
        const r = git(['show', `HEAD:${e.path}`]);
        if (r.status === 0) newBuf = r.stdout;
      } else {
        try {
          // Git stores a symlink blob as the link target path; following the
          // link would show the target file's bytes instead — the wrong diff,
          // and a way to expose unrelated local files in the review.
          const abs = join(ROOT, e.path);
          newBuf = lstatSync(abs).isSymbolicLink()
            ? Buffer.from(readlinkSync(abs))
            : readFileSync(abs);
        } catch {
          // vanished between diff and read; treat as deleted
        }
      }
    }
    if (looksBinary(oldBuf) || looksBinary(newBuf)) {
      skipped.push({ path: e.path, reason: 'binary' });
      continue;
    }
    if (oldBuf.length > MAX_FILE_BYTES || newBuf.length > MAX_FILE_BYTES) {
      skipped.push({ path: e.path, reason: `larger than ${MAX_FILE_BYTES} bytes` });
      continue;
    }
    files.push({
      path: e.path,
      oldPath: e.oldPath,
      status: e.status === '?' ? 'A' : e.status,
      old: oldBuf.toString('utf8'),
      new: newBuf.toString('utf8'),
    });
  }
  files.sort((a, b) => (a.path < b.path ? -1 : 1));
  return { files, skipped };
}

const { files, skipped } = collectFiles();
if (files.length === 0 && skipped.length === 0) {
  process.stderr.write(`diff-review: no changes to review vs ${BASE} in ${ROOT}\n`);
  process.exit(3);
}

const DATA = JSON.stringify({
  repo: ROOT,
  base: BASE,
  files,
  skipped,
});

// Vendored UI dependencies (see vendor/README.md). The @pierre/diffs bundle was
// built by esm.sh and imports its node polyfills by absolute path, so those are
// served at the exact /node/*.mjs specifiers the bundle expects. Fixed
// allowlist — nothing else under vendor/ or elsewhere is ever served.
const VENDOR_FILES = new Map([
  ['/vendor/pierre-diffs.mjs', { file: 'pierre-diffs-1.2.12.mjs.gz', gzipped: true }],
  ['/node/process.mjs', { file: 'node_process.mjs' }],
  ['/node/buffer.mjs', { file: 'node_buffer.mjs' }],
  ['/node/events.mjs', { file: 'node_events.mjs' }],
  ['/node/tty.mjs', { file: 'node_tty.mjs' }],
  ['/node/async_hooks.mjs', { file: 'node_async_hooks.mjs' }],
]);

// ── HTTP server ──────────────────────────────────────────────────────────────
const server = createServer((req, res) => {
  const url = new URL(req.url, `http://localhost:${PORT}`);
  if (req.method === 'GET' && (url.pathname === '/' || url.pathname === '/index.html')) {
    res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
    res.end(
      readFileSync(join(__dirname, 'ui.html'), 'utf8').replace('__DIFF_REVIEW_NONCE__', NONCE)
    );
  } else if (req.method === 'GET' && url.pathname === '/slow') {
    // Test helper: responds after ?ms= milliseconds (used by ?waitload=).
    const ms = Math.min(Number(url.searchParams.get('ms')) || 0, 60_000);
    setTimeout(() => {
      res.writeHead(200, { 'content-type': 'text/plain' });
      res.end('ok');
    }, ms);
  } else if (req.method === 'GET' && VENDOR_FILES.has(url.pathname)) {
    const { file, gzipped } = VENDOR_FILES.get(url.pathname);
    const buf = readFileSync(join(__dirname, 'vendor', file));
    if (!gzipped) {
      res.writeHead(200, { 'content-type': 'text/javascript; charset=utf-8' });
      res.end(buf);
    } else if ((req.headers['accept-encoding'] ?? '').includes('gzip')) {
      res.writeHead(200, {
        'content-type': 'text/javascript; charset=utf-8',
        'content-encoding': 'gzip',
      });
      res.end(buf);
    } else {
      res.writeHead(200, { 'content-type': 'text/javascript; charset=utf-8' });
      res.end(gunzipSync(buf));
    }
  } else if (req.method === 'GET' && url.pathname === '/data') {
    res.writeHead(200, { 'content-type': 'application/json; charset=utf-8' });
    res.end(DATA);
  } else if (req.method === 'POST' && url.pathname === '/submit') {
    // Only the page this server served knows the nonce; a cross-origin tab or a
    // blind local POST must not be able to complete the review on the user's
    // behalf. Cross-origin requests also carry a foreign Origin — reject those.
    const origin = req.headers.origin;
    const allowedOrigins = [`http://localhost:${PORT}`, `http://127.0.0.1:${PORT}`];
    if (
      req.headers['x-diff-review-nonce'] !== NONCE ||
      (origin != null && !allowedOrigins.includes(origin))
    ) {
      res.writeHead(403, { 'content-type': 'application/json' });
      res.end('{"ok":false,"error":"bad or missing review session nonce"}');
      return;
    }
    let body = '';
    req.on('data', (c) => (body += c));
    req.on('end', () => {
      let payload;
      try {
        payload = JSON.parse(body);
      } catch {
        res.writeHead(400, { 'content-type': 'application/json' });
        res.end('{"ok":false,"error":"invalid JSON"}');
        return;
      }
      const result = {
        submittedAt: new Date().toISOString(),
        repo: ROOT,
        base: BASE,
        ...payload,
      };
      writeFileSync(OUT, JSON.stringify(result, null, 2) + '\n');
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end('{"ok":true}');
      process.stdout.write(
        `diff-review: review submitted (verdict: ${payload.verdict}, ` +
          `${(payload.comments ?? []).length} comment(s)). Written to ${OUT}\n`
      );
      setTimeout(() => {
        server.close();
        process.exit(0);
      }, 300);
    });
  } else {
    res.writeHead(404);
    res.end('not found');
  }
});

server.on('error', (err) => {
  if (err.code === 'EADDRINUSE') {
    process.stderr.write(
      `diff-review: port ${PORT} is already in use. ` +
        `Kill a stale server (pkill -f "diff-review/server.mjs") or pass --port <other>.\n`
    );
  } else {
    process.stderr.write(`diff-review: server error: ${err.message}\n`);
  }
  process.exit(1);
});

server.listen(PORT, '127.0.0.1', () => {
  const url = `http://localhost:${PORT}`;
  process.stdout.write(
    `diff-review: ${files.length} file(s) ready for review at ${url}\n` +
      `diff-review: waiting for the user to submit their review; comments will be written to ${OUT}\n`
  );
  if (!NO_OPEN) {
    const opener =
      process.platform === 'darwin'
        ? ['open', [url]]
        : process.platform === 'win32'
          ? ['cmd', ['/c', 'start', '', url]]
          : ['xdg-open', [url]];
    try {
      spawn(opener[0], opener[1], { detached: true, stdio: 'ignore' }).on('error', () => {
        process.stdout.write(`diff-review: could not open a browser; open ${url} manually\n`);
      });
    } catch {
      process.stdout.write(`diff-review: could not open a browser; open ${url} manually\n`);
    }
  }
});

for (const sig of ['SIGINT', 'SIGTERM']) {
  process.on(sig, () => {
    process.stderr.write(`diff-review: ${sig} received, exiting without a review\n`);
    process.exit(130);
  });
}
