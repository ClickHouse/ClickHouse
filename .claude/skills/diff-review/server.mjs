#!/usr/bin/env node
// diff-review: serve pending git changes for in-browser review and collect the
// user's comments. Node 18+, zero npm dependencies; the UI renders with the
// @pierre/diffs bundle, which this server serves itself, hash-pinned (see
// assets.mjs and vendor/manifest.json) — the browser never loads code from a
// CDN.
//
// Usage:
//   node server.mjs [--repo <path>] [--base <ref>] [--head <ref> | --staged]
//                     [--committed] [--port 3000] [--out <file>] [--no-open]
//                     [--force]
//   node server.mjs --prefetch      # download the pinned assets, then exit
//
// --base is one end of the review; the other is the working tree by default, the
// index with --staged, or a commit with --head (--committed is --head HEAD).
// Between them that covers uncommitted work, what is about to be committed, one
// commit, and a whole branch:
//
//   (default)                              uncommitted work, untracked included
//   --staged                               exactly what `git commit` would record
//   --base HEAD~1 --committed              the last commit
//   --base <sha>^ --head <sha>             one commit, anywhere in history
//   --base $(git merge-base origin/master HEAD) --committed     a whole branch
//
// The server only lists what changed; the UI asks for both sides of a file when
// it displays it (/file), and builds the diff itself, so the reviewer can always
// expand unchanged regions out to the whole file.
//
// The --out file is durable state, not just a report: every comment is written
// to it as soon as it is made, and stays in it across runs until something marks
// it `"resolved": true`. Reopening a review therefore starts where the last one
// left off, and killing the server loses nothing. It defaults to
// diff-review-comments.json in the repository's git directory — one per
// repository, and out of the working tree, where it would otherwise turn up as a
// file to review. Pass --out to keep two reviews of one repository apart.
//
// Submitting does not end the review: the round is written out and announced on
// stdout, and the server stays up. The session goes to work on the round it was
// handed while the reviewer keeps reading and writes the next one; the review
// ends when the reviewer closes it, or when the server is signalled.
//
// stdout therefore carries only what a watching session reacts to — the review
// is open, a round was submitted, the review is closed — one line each, with
// everything else on stderr.
//
// Exits 0 once the review is closed (or signalled after a round was submitted),
// exits 3 when there is nothing to review, exits 4 when the machine looks remote
// and no browser reached the server in time, exits 1 on errors (e.g. port busy).

import { createServer } from 'node:http';
import { spawnSync, spawn } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { gunzipSync } from 'node:zlib';
import { existsSync, readFileSync, writeFileSync, lstatSync, readlinkSync } from 'node:fs';
import { join, dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { userInfo } from 'node:os';

import { reanchor, splitLines } from './anchor.mjs';
import { AssetError, cacheDir, resolveAssets, VENDOR_DIR } from './assets.mjs';

// Node 18 is the floor: global fetch and AbortSignal.timeout, which fetch the
// pinned UI assets. Older majors parse every file here, so without this they run
// until the first machine with a cold cache and fail there with
// `fetch is not defined` — a floor stated once beats that.
const NODE_MAJOR = Number(process.versions.node.split('.')[0]);
if (!(NODE_MAJOR >= 18)) {
  process.stderr.write(
    `diff-review: needs node 18 or newer, this is ${process.version}. The pinned UI assets are ` +
      `downloaded with global fetch, which older versions do not have.\n`
  );
  process.exit(1);
}

const __dirname = dirname(fileURLToPath(import.meta.url));
const args = process.argv.slice(2);
const argVal = (name, dflt) => {
  const i = args.indexOf(name);
  return i >= 0 && args[i + 1] != null ? args[i + 1] : dflt;
};

/// What the UI cannot render without, and why the server refuses to start rather
/// than open a review that would come up blank.
async function assetsOrDie() {
  try {
    return await resolveAssets({ log: (m) => process.stderr.write(m) });
  } catch (err) {
    if (!(err instanceof AssetError)) throw err;
    process.stderr.write(
      `diff-review: ${err.message}\n` +
        `diff-review: the review UI cannot render without it. With a network, ` +
        `\`node ${join(__dirname, 'server.mjs')} --prefetch\` fills ${cacheDir()}; ` +
        `offline, copy an audited copy into ${VENDOR_DIR} (see vendor/README.md).\n`
    );
    process.exit(1);
  }
}

// Filling the cache is a job of its own: no repository, no review, no server.
if (args.includes('--prefetch')) {
  for (const [servePath, asset] of await assetsOrDie())
    process.stderr.write(`diff-review: ${servePath} <- ${asset.from}\n`);
  process.exit(0);
}

const PORT = Number(argVal('--port', '3000'));
const BASE = argVal('--base', 'HEAD');
const REPO = resolve(argVal('--repo', process.cwd()));
const OUT_ARG = argVal('--out', null);
const NO_OPEN = args.includes('--no-open');
const FORCE = args.includes('--force') || process.env.DIFF_REVIEW_FORCE === '1';

/// The far end of the review: the working tree, the index, or a commit. Every
/// difference between the three modes is a `TARGET.kind` away, so nothing else
/// has to know which one is running.
const HEAD_REF = argVal('--head', args.includes('--committed') ? 'HEAD' : null);
const TARGET =
  HEAD_REF != null
    ? { kind: 'ref', ref: HEAD_REF, label: HEAD_REF }
    : args.includes('--staged')
      ? { kind: 'index', label: 'the index (staged)' }
      : { kind: 'worktree', label: 'the working tree' };
if (HEAD_REF != null && args.includes('--staged')) {
  process.stderr.write('diff-review: --staged and --head are two different ends; pick one\n');
  process.exit(1);
}

const MAX_FILE_BYTES = 2_000_000;
// Git's own heuristic: a NUL byte near the start means "binary". Used for
// untracked files, which have no numstat entry to read it off.
const NUL_SCAN_BYTES = 8000;

// ── Signals that the user's browser is probably elsewhere ────────────────────
// The server binds to loopback, so the review UI is reachable from a browser on
// this very host — or from any machine, once the user forwards the port
// (ssh -L 3000:localhost:3000). None of the signals below prove the page is
// unreachable: a cloud desktop has a local browser, `ssh -X` opens one over the
// wire, and a forwarded port reaches loopback from anywhere. So the signals
// refuse nothing on their own. They decide two things: whether to print the
// port-forwarding hint, and whether to arm a deadline so that on an isolated VM
// — where a review can never be submitted — the server exits instead of leaving
// a background task waiting forever. The refusal itself rests on the one
// observation that does prove nobody is looking: no request arrived in time.
function detectRemoteSignals() {
  const reasons = [];

  // A remote shell: the user's browser usually runs on the machine at the other
  // end of the connection, and reaches our loopback only through a forwarded
  // port.
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

  // No graphical session: no browser can be auto-opened here (the user may
  // still bring their own through a forwarded port). macOS and Windows always
  // have a window server, so this only applies to Linux.
  if (process.platform === 'linux' && !process.env.DISPLAY && !process.env.WAYLAND_DISPLAY)
    reasons.push('there is no graphical session (DISPLAY and WAYLAND_DISPLAY are both unset)');

  return reasons;
}

// How long a remote-looking machine waits for the first request before giving
// up. Long enough to copy the printed ssh -L command into another terminal;
// short enough that an unattended run does not hang. --force waits forever.
const NO_VISITOR_WAIT_SECS = 120;
const remoteSignals = FORCE ? [] : detectRemoteSignals();
let noVisitorTimer = null;

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
function gitOrDie(gitArgs, opts) {
  const r = git(gitArgs, opts);
  if (r.status !== 0) {
    process.stderr.write(`diff-review: git ${gitArgs.join(' ')} failed:\n${r.stderr}`);
    process.exit(1);
  }
  return r.stdout;
}

let ROOT = null;
ROOT = gitOrDie(['rev-parse', '--show-toplevel']).toString('utf8').trim();

// Comments live in the git directory by default: durable and one per repository,
// like the review they belong to, but never in `git status` and so never a file
// under review in the next round. --absolute-git-dir, not <root>/.git, because a
// linked worktree keeps its own git dir elsewhere.
const OUT =
  OUT_ARG != null
    ? resolve(OUT_ARG)
    : join(
        gitOrDie(['rev-parse', '--absolute-git-dir']).toString('utf8').trim(),
        'diff-review-comments.json'
      );

// ── Collect changed files ────────────────────────────────────────────────────
// Everything the UI needs to draw the file tree, and nothing else: no patches,
// because the viewer builds its diff from the two full sides it fetches later.

/// `git diff` from BASE to whichever end the review has. Only the working-tree
/// review sees uncommitted edits; the others read the index or a commit, so
/// local dirt never leaks into a review of work that is already recorded.
function diffArgs(...what) {
  if (TARGET.kind === 'ref') return ['diff', ...what, '-z', '-M', BASE, TARGET.ref];
  if (TARGET.kind === 'index') return ['diff', '--cached', ...what, '-z', '-M', BASE];
  return ['diff', ...what, '-z', '-M', BASE];
}

function collectEntries() {
  const entries = []; // { path, oldPath, status }
  const tokens = gitOrDie(diffArgs('--name-status'))
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
  // A file git has never been told about is part of uncommitted work and of
  // nothing else — not of the index, and not of a commit.
  if (TARGET.kind === 'worktree') {
    const untracked = gitOrDie(['ls-files', '--others', '--exclude-standard', '-z'])
      .toString('utf8')
      .split('\0')
      .filter((t) => t.length > 0);
    for (const p of untracked) entries.push({ path: p, oldPath: null, status: '?' });
  }
  return entries;
}

// Line counts per path, so the UI can show `+a −d`, and the binary flag: git
// reports `-` for both counts of a binary file. In `-z` output a rename emits
// the counts and a trailing tab, then the old and new paths as two tokens.
function collectNumstat() {
  const counts = new Map();
  const tokens = gitOrDie(diffArgs('--numstat')).toString('utf8').split('\0');
  for (let i = 0; i < tokens.length; ) {
    const token = tokens[i++];
    if (token === '') continue;
    const fields = token.split('\t');
    if (fields.length < 3) continue;
    let path = fields.slice(2).join('\t');
    if (path === '') {
      i++; // old path of a rename
      path = tokens[i++];
    }
    const num = (v) => (v === '-' ? null : Number(v));
    counts.set(path, { added: num(fields[0]), removed: num(fields[1]) });
  }
  return counts;
}

/// Blob sizes for `<rev>:<path>` specs in one git invocation, so a file too big
/// to serve is left out of the tree instead of failing when it is clicked.
function blobSizes(specs) {
  const sizes = new Map();
  if (specs.length === 0) return sizes;
  const out = gitOrDie(['cat-file', '--batch-check'], { input: specs.join('\n') + '\n' })
    .toString('utf8')
    .split('\n');
  specs.forEach((spec, i) => {
    const fields = (out[i] ?? '').split(' ');
    if (fields[1] === 'blob') sizes.set(spec, Number(fields[2]));
  });
  return sizes;
}

/// An untracked file has no numstat entry, so its counts and its binary-ness are
/// read off the working tree directly.
function untrackedInfo(path) {
  const abs = join(ROOT, path);
  const stat = lstatSync(abs, { throwIfNoEntry: false });
  if (stat == null) return null; // vanished between `ls-files` and here
  if (stat.isSymbolicLink()) return { size: readlinkSync(abs).length, binary: false, added: 1 };
  if (!stat.isFile()) return null;
  if (stat.size > MAX_FILE_BYTES) return { size: stat.size, binary: false, added: 0 };
  const buf = readFileSync(abs);
  let added = 0;
  for (let at = buf.indexOf(10); at !== -1; at = buf.indexOf(10, at + 1)) added++;
  if (buf.length > 0 && buf[buf.length - 1] !== 10) added++;
  return { size: stat.size, binary: buf.subarray(0, NUL_SCAN_BYTES).includes(0), added };
}

const entryByPath = new Map();

/// Where each side of a file lives, in git's `<rev>:<path>` syntax — `:<path>`
/// being the index. The new side of a working-tree review is in no git object at
/// all: it is the file on disk, so it has no spec.
const oldSpec = (e) =>
  e.status === 'A' || e.status === '?' ? null : `${BASE}:${e.oldPath ?? e.path}`;
const newSpec = (e) => {
  if (e.status === 'D') return null;
  if (TARGET.kind === 'ref') return `${TARGET.ref}:${e.path}`;
  if (TARGET.kind === 'index') return `:${e.path}`;
  return null;
};

function collectFiles() {
  const entries = collectEntries();
  const stats = collectNumstat();

  const sizes = blobSizes([
    ...new Set(entries.flatMap((e) => [oldSpec(e), newSpec(e)].filter((s) => s != null))),
  ]);
  const worktreeSize = (e) => {
    if (e.status === 'D') return 0;
    const stat = lstatSync(join(ROOT, e.path), { throwIfNoEntry: false });
    if (stat == null) return 0;
    return stat.isSymbolicLink() ? readlinkSync(join(ROOT, e.path)).length : stat.size;
  };

  entryByPath.clear();
  const files = [];
  const skipped = [];
  for (const e of entries) {
    entryByPath.set(e.path, e);
    const untracked = e.status === '?' ? untrackedInfo(e.path) : null;
    if (e.status === '?' && untracked == null) continue;
    const stat = untracked != null
      ? { added: untracked.added, removed: 0 }
      : stats.get(e.path) ?? { added: null, removed: null };

    if (untracked?.binary === true || stat.added == null) {
      skipped.push({ path: e.path, reason: 'binary' });
      continue;
    }
    // Size before line counts: a file too big to serve has no counts to report.
    const bytes = Math.max(
      sizes.get(oldSpec(e)) ?? 0,
      newSpec(e) != null ? sizes.get(newSpec(e)) ?? 0 : worktreeSize(e)
    );
    if (bytes > MAX_FILE_BYTES) {
      skipped.push({ path: e.path, reason: `larger than ${MAX_FILE_BYTES} bytes` });
      continue;
    }
    if (stat.added === 0 && stat.removed === 0) {
      // A pure rename has nothing to diff, but saying so plainly is confusing.
      const reason = e.oldPath == null ? 'no textual change' : `renamed from ${e.oldPath}`;
      skipped.push({ path: e.path, reason });
      continue;
    }
    files.push({
      path: e.path,
      oldPath: e.oldPath,
      status: e.status === '?' ? 'A' : e.status,
      added: stat.added,
      removed: stat.removed,
    });
  }
  files.sort((a, b) => (a.path < b.path ? -1 : 1));
  return { files, skipped };
}

/// Both sides of one file, as the UI needs them to build a diff it can expand.
function sidesFor(path) {
  const e = entryByPath.get(path);
  if (e == null) return null;
  const blob = (spec) => {
    const r = git(['show', spec]);
    return r.status === 0 ? r.stdout : Buffer.alloc(0);
  };
  const oldBuf = oldSpec(e) == null ? Buffer.alloc(0) : blob(oldSpec(e));
  let newBuf = Buffer.alloc(0);
  if (newSpec(e) != null) {
    newBuf = blob(newSpec(e));
  } else if (e.status !== 'D') {
    try {
      // Git stores a symlink blob as the link target path; following the link
      // would show the target file's bytes instead — the wrong diff, and a way to
      // expose unrelated local files in the review.
      const abs = join(ROOT, e.path);
      newBuf = lstatSync(abs).isSymbolicLink()
        ? Buffer.from(readlinkSync(abs))
        : readFileSync(abs);
    } catch {
      // vanished since the diff was collected; treat as deleted
    }
  }
  if (oldBuf.length > MAX_FILE_BYTES || newBuf.length > MAX_FILE_BYTES) return { tooBig: true };
  return { old: oldBuf.toString('utf8'), new: newBuf.toString('utf8') };
}

const RANGE = `${BASE} → ${TARGET.label}`;

// ── The pull request this branch is already open as, if any ──────────────────
// Looked up off the critical path: `gh` talks to the network, and the review must
// open at once whether or not it answers. Until it does, and for a branch with no
// pull request or a machine with no `gh`, the header simply has no link — the
// state this asks about is "not every branch has one", not an error.
//
// One call answers the checks too, since they are a property of the pull request
// rather than of the diff. It is repeated while the review is open, because checks
// finish on their own schedule, but never faster than CI_TTL_MS.
const CI_TTL_MS = 60_000;

let pr = null; // { number, url, title, draft } once known
let ci = null; // { state, failed, pending, passed, sameCommit } once known
let ciAskedAt = 0;
let ciAsking = false;

/// GitHub reports two kinds of check on a commit — Actions runs and commit
/// statuses — and the badge is the worst of them, one failure outweighing any
/// number of passes. Skipped, neutral and cancelled ones count as neither: a pull
/// request carries a dozen of those on its best day, and a badge that is red on
/// every branch says nothing. An unrecognised conclusion counts as failed, so a
/// state this does not know about shows up rather than passing for green.
const UNCOUNTED_CONCLUSIONS = new Set(['NEUTRAL', 'SKIPPED', 'CANCELLED', 'STALE']);

function rollUpChecks(checks) {
  let failed = 0;
  let pending = 0;
  let passed = 0;
  for (const c of checks) {
    if (c.__typename === 'CheckRun') {
      if (c.status !== 'COMPLETED') pending++;
      else if (c.conclusion === 'SUCCESS') passed++;
      else if (!UNCOUNTED_CONCLUSIONS.has(c.conclusion)) failed++;
    } else {
      if (c.state === 'SUCCESS') passed++;
      else if (c.state === 'PENDING' || c.state === 'EXPECTED') pending++;
      else if (c.state === 'FAILURE' || c.state === 'ERROR') failed++;
    }
  }
  const state = failed > 0 ? 'failure' : pending > 0 ? 'pending' : passed > 0 ? 'success' : null;
  return state == null ? null : { state, failed, pending, passed };
}

function lookUpPullRequest() {
  if (ciAsking) return;
  ciAsking = true;
  ciAskedAt = Date.now();
  const fields = 'number,url,title,isDraft,headRefOid,statusCheckRollup';
  const proc = spawn('gh', ['pr', 'view', '--json', fields], {
    cwd: ROOT,
    stdio: ['ignore', 'pipe', 'ignore'],
    timeout: 10_000, // a hung network call must not outlive its usefulness
  });
  proc.unref(); // …nor hold the process open when the review closes
  let out = '';
  proc.stdout.on('data', (d) => (out += d));
  proc.on('error', () => (ciAsking = false)); // no `gh` on this machine
  proc.on('close', (code) => {
    ciAsking = false;
    if (code !== 0) return; // most often: this branch has no pull request
    try {
      const v = JSON.parse(out);
      // Only a github.com URL is ever put in the page, and only as a whole href.
      if (!/^https:\/\/github\.com\/[\w.-]+\/[\w.-]+\/pull\/\d+$/.test(v.url ?? '')) return;
      pr = { number: v.number, url: v.url, title: v.title ?? '', draft: v.isDraft === true };
      const rolled = rollUpChecks(v.statusCheckRollup ?? []);
      // Whether those checks are even about the code on screen: a branch whose
      // local commits GitHub has not seen carries the checks of an older head.
      // Read now rather than at startup, because committing during a review moves
      // it. A HEAD this cannot read is not claimed to match.
      const head = git(['rev-parse', 'HEAD']);
      const sha = head.status === 0 ? head.stdout.toString('utf8').trim() : null;
      ci = rolled == null ? null : { ...rolled, sameCommit: sha != null && v.headRefOid === sha };
    } catch {
      // not the JSON this asked for; there is nothing to link to
    }
  });
}
lookUpPullRequest();

let { files, skipped } = collectFiles();
if (files.length === 0 && skipped.length === 0) {
  process.stderr.write(`diff-review: nothing to review (${RANGE}) in ${ROOT}\n`);
  process.exit(3);
}

let servedPaths = new Set(files.map((f) => f.path));

// ── The review file, which is the review's state ─────────────────────────────
// A review file that already exists is read back, not overwritten: comments it
// still holds open are handed to the UI, and comments it records as resolved are
// kept verbatim so the file stays the whole history of the review.
//
// It is read on every request that needs it rather than once at startup,
// because three writers share it while the review is open: the page as the
// reviewer types, this server as it saves, and the Claude session as it marks
// what it has addressed resolved.

/// Throws rather than clobbers: a file that cannot be understood may hold
/// comments nobody has acted on yet, and overwriting it would lose them.
function readReviewFile() {
  if (!existsSync(OUT)) return { comments: [] };
  let parsed;
  try {
    parsed = JSON.parse(readFileSync(OUT, 'utf8'));
  } catch (err) {
    throw new Error(`is not valid JSON (${err.message})`);
  }
  if (parsed == null || typeof parsed !== 'object' || !Array.isArray(parsed.comments)) {
    throw new Error('is not a review file (no "comments" array)');
  }
  if (parsed.repo != null && resolve(parsed.repo) !== ROOT) {
    throw new Error(`holds a review of ${parsed.repo}, not of ${ROOT}`);
  }
  return parsed;
}

let priorFile;
try {
  priorFile = readReviewFile();
} catch (err) {
  process.stderr.write(
    `diff-review: ${OUT} ${err.message}; refusing to overwrite it. ` +
      `Move it aside, or pass --out <other file>.\n`
  );
  process.exit(1);
}

/// The round now open for comments. Submitting hands a round to the session but
/// leaves the review up, so whatever is written next belongs to the round after
/// the one being worked on — that step happens on submit, and only there.
/// Reopening a review resumes the round it was writing: restarting the server is
/// not a round, and counting it as one would inflate a number the reviewer reads
/// as "how many times have I sent this".
let round = Math.max(
  Number(priorFile.round) || 1,
  (Number(priorFile.submitted?.round) || 0) + 1,
  ...priorFile.comments.map((c) => Number(c.round) || 0)
);

/// What the last submit handed over, recorded in every write so the session can
/// tell which round the file it is reading was handed to it as.
let lastSubmit = priorFile.submitted ?? null;

// Ids have to stay unique for the life of the file — a resolved comment keeps
// its own — so the counter starts past the highest one ever handed out.
const idNum = (id) => Number(/^c(\d+)$/.exec(id ?? '')?.[1]) || 0;
let seq = Math.max(0, ...priorFile.comments.map((c) => idNum(c.id)));
const mintId = () => `c${++seq}`;

const sideOf = (c) => (c.side === 'old' ? 'old' : 'new');

/// One side of a file, split into lines the way the UI numbers them. Kept for
/// the life of the process: the review is a snapshot, and the same lines are
/// read again every time a comment is anchored.
const linesCache = new Map();

// ── Files that have moved on since they were read ────────────────────────────
// Only a working-tree review can go stale: the other two ends are git objects.
// What is watched is what has been looked at — a file's `stat` is recorded as its
// sides are served, so the set is bounded by what the reviewer has opened and a
// poll costs one `stat` each. The page is told, and reloads on the word of the
// reviewer rather than under their hands: a diff that moved while a comment was
// being written on it is the one thing this review must not do.
const WATCHED = TARGET.kind === 'worktree';
const servedStat = new Map(); // path -> `${mtimeMs}:${size}`, as last handed out

function statOf(path) {
  try {
    const st = lstatSync(join(ROOT, path));
    return `${st.mtimeMs}:${st.size}`;
  } catch {
    return 'gone'; // deleted since it was served, which is a change like any other
  }
}

function recordServed(path) {
  if (WATCHED) servedStat.set(path, statOf(path));
}

/// The watched files whose bytes are not the ones the page holds. Reading them
/// again is the reviewer's call, so the only thing done here is to drop what was
/// derived from the old bytes — the lines a carried comment is anchored against,
/// which a reload then recomputes.
function changedOnDisk() {
  if (!WATCHED) return [];
  const changed = [];
  for (const [path, seen] of servedStat) {
    if (statOf(path) === seen) continue;
    changed.push(path);
    linesCache.delete(`old ${path}`);
    linesCache.delete(`new ${path}`);
  }
  return changed;
}

/// Read the diff again, as a freshly started server would: the file set, each
/// file's status and counts, and the bytes of both sides. This is what the page
/// asks for when it loads, so a reload is a restart in everything but the round
/// and the process. Nothing is refused when the diff has emptied - unlike at
/// startup there is a review open, and its comments are still in the file, so it
/// draws as a review of no files rather than taking the server down.
function recollect() {
  ({ files, skipped } = collectFiles());
  servedPaths = new Set(files.map((f) => f.path));
  linesCache.clear();
  servedStat.clear();
}

function linesOf(path, side) {
  const key = `${side} ${path}`;
  if (linesCache.has(key)) return linesCache.get(key);
  const sides = servedPaths.has(path) ? sidesFor(path) : null;
  const text = sides == null || sides.tooBig === true ? null : side === 'old' ? sides.old : sides.new;
  const lines = text == null ? null : splitLines(text);
  linesCache.set(key, lines);
  return lines;
}

/// Everything the review file holds at this moment, in the groups the rest of
/// the server needs it in: the comments this review can put on screen (each
/// relocated to wherever the code has moved it since it was filed), the ones on
/// files it cannot show, and the ones something has already resolved.
function reviewState() {
  const parsed = readReviewFile();
  const roundOf = new Map(); // a comment belongs to the round it was first written in
  const resolved = [];
  // Read and done with: off the page for good, kept in the file for the record.
  const dismissed = [];
  // A comment on a file this review does not show cannot be displayed, but it is
  // still open: it is kept verbatim and written back untouched.
  const elsewhere = [];
  const carried = [];
  for (const c of parsed.comments) {
    if (typeof c.id !== 'string' || c.id === '') c.id = mintId();
    seq = Math.max(seq, idNum(c.id));
    if (Number(c.round)) roundOf.set(c.id, Number(c.round));
    if (c.dismissed === true) {
      dismissed.push(c);
      continue;
    }
    if (c.resolved === true) {
      resolved.push(c);
      continue;
    }
    const lines = linesOf(c.file, sideOf(c));
    if (lines == null) elsewhere.push(c);
    else carried.push(reanchor(c, lines));
  }
  return { resolved, dismissed, elsewhere, carried, roundOf };
}

/// What the page needs to draw the review. Built per request, not once: a reload
/// has to come back to every comment made so far, including the ones made since
/// the server started and the ones the session has resolved in the meantime.
function dataPayload() {
  const { carried, elsewhere, resolved } = reviewState();
  const viewerComment = (c) => ({
    id: c.id,
    file: c.file,
    side: sideOf(c) === 'old' ? 'deletions' : 'additions',
    start: c.startLine,
    end: c.endLine,
    text: c.comment,
    draft: c.draft === true,
    round: c.round ?? null,
    // Only a comment an earlier round already handed over reads as carried;
    // one made since the last submit is still being written.
    carried: (Number(c.round) || round) < round,
    moved: c.moved === true,
    movedFrom: c.movedFrom ?? null,
    stale: c.stale === true,
  });
  return JSON.stringify({
    repo: ROOT,
    base: BASE,
    range: RANGE,
    // The far end, as something a tool can act on: 'worktree', 'index', or a ref.
    target: TARGET.kind === 'ref' ? TARGET.ref : TARGET.kind,
    pr,
    ci,
    files,
    skipped,
    out: OUT,
    round,
    submitted: lastSubmit?.round ?? 0,
    nextSeq: seq + 1,
    // In the viewer's own terms, so the UI can seed them without translating.
    carried: carried.map(viewerComment),
    // A reload has to find the addressed ones too, or a review whose every
    // comment is resolved comes back empty. Relocated like the open ones, since
    // the fixes that resolved them are exactly what moved the code.
    resolved: resolved.map((c) => {
      const lines = linesOf(c.file, sideOf(c));
      return {
        ...viewerComment(lines == null ? c : reanchor(c, lines)),
        resolved: true,
        resolution: c.resolution ?? null,
      };
    }),
    elsewhere: elsewhere.map((c) => ({ file: c.file, line: c.endLine })),
  });
}

/// The review file, rewritten whole from what the UI holds plus what the file
/// itself is keeping. Comments already resolved, and comments on files outside
/// this review, are preserved exactly as they were read: the UI never saw them
/// and cannot be asked to send them back.
function saveReview({ status, verdict, overall, comments }) {
  const { resolved, dismissed, elsewhere, roundOf } = reviewState();
  const done = new Set([...resolved, ...dismissed].map((c) => c.id));
  const live = comments
    // The session can resolve a comment while the page still has it on screen;
    // what the file says about it then outranks what the page sends back.
    .filter((c) => !done.has(c.id))
    .map((c) => {
      const side = sideOf(c);
      const anchor = linesOf(c.file, side)?.[c.endLine - 1];
      const id = typeof c.id === 'string' && c.id !== '' ? c.id : mintId();
      return {
        id,
        file: c.file,
        side,
        startLine: c.startLine,
        endLine: c.endLine,
        comment: c.comment,
        ...(c.draft === true ? { draft: true } : {}),
        resolved: false,
        round: roundOf.get(id) ?? (Number(c.round) || round),
        // What an addressed comment this one follows up on, so the session can
        // read the question, its answer and the follow-up as one thread.
        ...(typeof c.replyTo === 'string' ? { replyTo: c.replyTo } : {}),
        ...(anchor != null ? { anchor } : {}),
      };
    });
  const byPlace = (a, b) => (a.file === b.file ? a.endLine - b.endLine : a.file < b.file ? -1 : 1);
  const open = [...live, ...elsewhere.map((c) => ({ ...c, resolved: false }))].sort(byPlace);
  const review = {
    status,
    updatedAt: new Date().toISOString(),
    repo: ROOT,
    base: BASE,
    range: RANGE,
    round,
    submitted: lastSubmit,
    verdict,
    overall,
    // Open first: that is the work list. Addressed ones follow for the record,
    // and the ones the reviewer has read and dismissed come last.
    comments: [...open, ...resolved.sort(byPlace), ...dismissed.sort(byPlace)],
  };
  writeFileSync(OUT, JSON.stringify(review, null, 2) + '\n');
  return open.length;
}

// The UI's pinned dependencies, resolved and hash-checked before the review
// opens: a missing bundle is a blank page, and finding that out here — with
// nothing at stake yet — beats finding it out in the browser. The map is also
// the routing table, so only the manifest's paths are ever served. (Its
// /node/*.mjs entries are there because the esm.sh-built @pierre/diffs bundle
// imports its polyfills by absolute path.)
const ASSETS = await assetsOrDie();

// The page and its modules are read per request and never cached, so editing the
// UI needs nothing but a reload of the tab.
const UI_MODULE = /^[a-z][a-z0-9_]*\.mjs$/;
const NO_STORE = { 'cache-control': 'no-store' };

// The loopback bind keeps the network out but not a browser: a page served from
// an attacker's domain whose DNS flips to 127.0.0.1 reaches this server
// same-origin, so no CORS preflight and no Origin check sees anything wrong.
// Host still names the domain the browser thought it had, and only these can
// name this server. A browser always sends it; a request without one is refused.
const ALLOWED_HOSTS = new Set([`localhost:${PORT}`, `127.0.0.1:${PORT}`, `[::1]:${PORT}`]);

// ── HTTP server ──────────────────────────────────────────────────────────────
/// A request that has to read the review file. An unreadable file fails the
/// request instead of the review: the page reports it and keeps what it holds,
/// and the file — which may be the only copy of somebody's comments — is left
/// exactly as it is.
function withReviewFile(res, fn) {
  try {
    fn();
  } catch (err) {
    process.stderr.write(`diff-review: cannot read ${OUT}: ${err.message}\n`);
    res.writeHead(500, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ error: `${OUT} ${err.message}` }));
  }
}

const server = createServer((req, res) => {
  // Before anything else, including counting the request as a visitor: a
  // rebinding probe is not the browser this server is waiting for.
  if (!ALLOWED_HOSTS.has(req.headers.host ?? '')) {
    res.writeHead(403, { 'content-type': 'application/json' });
    res.end(`{"ok":false,"error":"bad host; this server answers only to localhost:${PORT}"}`);
    return;
  }
  // A request is the proof the environment signals could not give: some browser
  // does reach this server. From here on, wait for the review indefinitely.
  if (noVisitorTimer != null) {
    clearTimeout(noVisitorTimer);
    noVisitorTimer = null;
    process.stderr.write('diff-review: a browser reached the server; waiting for the review\n');
  }
  const url = new URL(req.url, `http://localhost:${PORT}`);
  if (req.method === 'GET' && (url.pathname === '/' || url.pathname === '/index.html')) {
    res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', ...NO_STORE });
    res.end(
      readFileSync(join(__dirname, 'ui.html'), 'utf8').replace('__DIFF_REVIEW_NONCE__', NONCE)
    );
  } else if (req.method === 'GET' && url.pathname.startsWith('/ui/')) {
    const name = url.pathname.slice('/ui/'.length);
    let source = null;
    if (UI_MODULE.test(name)) {
      try {
        source = readFileSync(join(__dirname, 'ui', name));
      } catch {
        source = null;
      }
    }
    if (source == null) {
      res.writeHead(404, { 'content-type': 'text/plain' });
      res.end('no such module');
    } else {
      res.writeHead(200, { 'content-type': 'text/javascript; charset=utf-8', ...NO_STORE });
      res.end(source);
    }
  } else if (req.method === 'GET' && url.pathname === '/slow') {
    // Test helper: responds after ?ms= milliseconds (used by ?waitload=).
    const ms = Math.min(Number(url.searchParams.get('ms')) || 0, 60_000);
    setTimeout(() => {
      res.writeHead(200, { 'content-type': 'text/plain' });
      res.end('ok');
    }, ms);
  } else if (req.method === 'GET' && ASSETS.has(url.pathname)) {
    // Held in memory, already verified, so a request cannot fail here.
    const asset = ASSETS.get(url.pathname);
    if (!asset.gzip) {
      res.writeHead(200, { 'content-type': asset.type });
      res.end(asset.body);
    } else if ((req.headers['accept-encoding'] ?? '').includes('gzip')) {
      res.writeHead(200, { 'content-type': asset.type, 'content-encoding': 'gzip' });
      res.end(asset.body);
    } else {
      asset.inflated ??= gunzipSync(asset.body);
      res.writeHead(200, { 'content-type': asset.type });
      res.end(asset.inflated);
    }
  } else if (req.method === 'GET' && url.pathname === '/data') {
    withReviewFile(res, () => {
      // The page is (re)loading, which is the reviewer's own act - the one moment
      // the diff may move. Between loads it is held still, so a comment is never
      // written against lines that shift out from under it.
      recollect();
      res.writeHead(200, { 'content-type': 'application/json; charset=utf-8' });
      res.end(dataPayload());
    });
  } else if (req.method === 'GET' && url.pathname === '/status') {
    // Polled by the page while the session works: which round is open now, and
    // what the session has marked resolved since the page last asked. It reads
    // the file and nothing else — relocating comments on every poll would be
    // work for a question that does not ask where anything is.
    withReviewFile(res, () => {
      const resolved = readReviewFile().comments.filter(
        (c) => c.resolved === true && c.dismissed !== true);
      // The page's own poll is what paces the check lookup: it runs while someone
      // is reading and stops when they close the tab.
      if (Date.now() - ciAskedAt > CI_TTL_MS) lookUpPullRequest();
      res.writeHead(200, { 'content-type': 'application/json; charset=utf-8' });
      const changed = changedOnDisk();
      res.end(
        JSON.stringify({
          round,
          submitted: lastSubmit?.round ?? 0,
          resolved: resolved.map((c) => ({ id: c.id, resolution: c.resolution ?? null })),
          ...(changed.length > 0 ? { changed } : {}),
          ...(pr != null ? { pr } : {}),
          ...(ci != null ? { ci } : {}),
        })
      );
    });
  } else if (req.method === 'GET' && url.pathname === '/file') {
    // Both sides of one file. Only paths that are part of this review are served
    // — never an arbitrary path from the query string.
    const path = url.searchParams.get('path') ?? '';
    const sides = servedPaths.has(path) ? sidesFor(path) : null;
    if (sides == null) {
      res.writeHead(404, { 'content-type': 'application/json' });
      res.end('{"error":"not part of this review"}');
    } else if (sides.tooBig === true) {
      res.writeHead(413, { 'content-type': 'application/json' });
      res.end(`{"error":"file larger than ${MAX_FILE_BYTES} bytes"}`);
    } else {
      // Before the write, so a file that changes between reading and answering is
      // seen as changed rather than missed.
      recordServed(path);
      res.writeHead(200, { 'content-type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify(sides));
    }
  } else if (
    req.method === 'POST' &&
    (url.pathname === '/save' || url.pathname === '/submit' || url.pathname === '/close'
      || url.pathname === '/dismiss')
  ) {
    // Only the page this server served knows the nonce; a cross-origin tab or a
    // blind local POST must not be able to write the review file on the user's
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
    if (url.pathname === '/close') {
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end('{"ok":true}');
      process.stdout.write(
        `diff-review: review closed after ${lastSubmit?.round ?? 0} round(s); ${OUT} is final\n`
      );
      setTimeout(() => {
        server.close();
        process.exit(0);
      }, 300);
      return;
    }
    const submitting = url.pathname === '/submit';
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
      // An addressed comment the reviewer is done with. The page has already
      // dropped it, so this only records the fact — in the file, not by deleting
      // it: what was asked and what was answered are worth keeping.
      if (url.pathname === '/dismiss') {
        try {
          const file = readReviewFile();
          const target = file.comments.find((c) => c.id === payload.id);
          if (target == null) {
            res.writeHead(404, { 'content-type': 'application/json' });
            res.end('{"ok":false,"error":"no such comment"}');
            return;
          }
          target.dismissed = true;
          writeFileSync(OUT, JSON.stringify(file, null, 2) + '\n');
        } catch (err) {
          process.stderr.write(`diff-review: cannot write ${OUT}: ${err.message}\n`);
          res.writeHead(500, { 'content-type': 'application/json' });
          res.end(JSON.stringify({ ok: false, error: err.message }));
          return;
        }
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end('{"ok":true}');
        return;
      }
      const handedOver = round;
      if (submitting) {
        lastSubmit = { round: handedOver, verdict: payload.verdict, at: new Date().toISOString() };
      }
      let open;
      try {
        open = saveReview({
          status: submitting ? 'submitted' : 'in_progress',
          verdict: lastSubmit?.verdict ?? null,
          overall: payload.overall ?? '',
          comments: payload.comments ?? [],
        });
      } catch (err) {
        // Never a silent loss: the page keeps what it holds and says it is unsaved.
        process.stderr.write(`diff-review: cannot write ${OUT}: ${err.message}\n`);
        res.writeHead(500, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ ok: false, error: err.message }));
        return;
      }
      if (!submitting) {
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ ok: true, round }));
        process.stderr.write(`diff-review: ${open} open comment(s) saved to ${OUT}\n`);
        return;
      }
      // The review stays up: the session goes to work on the round just handed
      // over while the reviewer keeps reading, and what they write next is the
      // round after it.
      round = handedOver + 1;
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ ok: true, submitted: handedOver, round }));
      process.stdout.write(
        `diff-review: round ${handedOver} submitted (verdict: ${payload.verdict}, ` +
          `${open} open comment(s)) — ${OUT}\n`
      );
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
        `Kill the stale server on this port (pkill -f "[s]erver.mjs.*--port ${PORT}") or pass ` +
        `--port <other>. The pattern matches the port rather than this skill's path, because ` +
        `another copy of the skill may be holding a review of its own; the [s] keeps it from ` +
        `matching the shell that runs it.\n`
    );
  } else {
    process.stderr.write(`diff-review: server error: ${err.message}\n`);
  }
  process.exit(1);
});

server.listen(PORT, '127.0.0.1', () => {
  const url = `http://localhost:${PORT}`;
  const { carried, elsewhere, resolved } = reviewState();
  const open = carried.length + elsewhere.length;
  // stdout is the event stream a watching session reacts to — one line here, one
  // per submitted round, one when the review closes. Everything else is stderr.
  process.stdout.write(
    `diff-review: round ${round} open at ${url} — ${files.length} file(s), ${RANGE}\n`
  );
  process.stderr.write(
    (open > 0
      ? `diff-review: ${open} unresolved comment(s) carried over` +
        (elsewhere.length > 0 ? ` (${elsewhere.length} on files outside this review)` : '') +
        `, ${resolved.length} already resolved\n`
      : '') +
      `diff-review: comments are saved to ${OUT} as they are made; each submit hands a ` +
      `round over and leaves the review open\n`
  );
  // Auto-open skips only where it literally cannot work: Linux without a
  // display. Under `ssh -X` or on a cloud desktop, DISPLAY is set and the
  // browser opens on the user's screen despite the remote-looking signals.
  const canOpenBrowser =
    process.platform !== 'linux' || process.env.DISPLAY || process.env.WAYLAND_DISPLAY;
  if (!NO_OPEN && canOpenBrowser) {
    const opener =
      process.platform === 'darwin'
        ? ['open', [url]]
        : process.platform === 'win32'
          ? ['cmd', ['/c', 'start', '', url]]
          : ['xdg-open', [url]];
    try {
      spawn(opener[0], opener[1], { detached: true, stdio: 'ignore' }).on('error', () => {
        process.stderr.write(`diff-review: could not open a browser; open ${url} manually\n`);
      });
    } catch {
      process.stderr.write(`diff-review: could not open a browser; open ${url} manually\n`);
    }
  }
  if (remoteSignals.length > 0) {
    // SSH_CONNECTION is "<client_ip> <client_port> <server_ip> <server_port>":
    // the address and port the user's own ssh connected to, which a forwarding
    // command must reuse. hostname() would often name a private interface
    // (ip-172-31-…) that the user's machine cannot resolve, so without
    // SSH_CONNECTION the hint shows placeholders instead of posing as
    // copy-pasteable.
    const ssh = (process.env.SSH_CONNECTION ?? '').trim().split(/\s+/);
    let target = '<user>@<this-machine>';
    if (ssh.length === 4) {
      let user;
      try {
        user = userInfo().username;
      } catch {
        user = '<user>';
      }
      target = (ssh[3] === '22' ? '' : `-p ${ssh[3]} `) + `${user}@${ssh[2]}`;
    }
    // The forwarding command is the one thing here a watching session has to
    // pass on to the user, so it joins the events on stdout; the reasons behind
    // it are diagnostics and stay on stderr.
    process.stdout.write(
      `diff-review: forward the port to review from your own machine: ` +
        `ssh -L ${PORT}:localhost:${PORT} ${target}\n`
    );
    process.stderr.write(
      'diff-review: this machine looks remote:\n' +
        remoteSignals.map((r) => `  - ${r}\n`).join('') +
        `diff-review: the server listens on loopback only; once the port is forwarded, ${url} ` +
        `opens the review from anywhere.\n` +
        `diff-review: exiting in ${NO_VISITOR_WAIT_SECS} s unless the review page is opened ` +
        `(--force waits indefinitely).\n`
    );
    noVisitorTimer = setTimeout(() => {
      process.stderr.write(
        `diff-review: no browser reached the server within ${NO_VISITOR_WAIT_SECS} s, exiting without a review.\n` +
          'diff-review: show the diff in the terminal instead, with git diff or git show.\n' +
          'diff-review: or forward the port and re-run with --force to wait indefinitely.\n'
      );
      process.exit(4);
    }, NO_VISITOR_WAIT_SECS * 1000);
  }
});

for (const sig of ['SIGINT', 'SIGTERM']) {
  process.on(sig, () => {
    const rounds = lastSubmit?.round ?? 0;
    process.stderr.write(
      `diff-review: ${sig} received after ${rounds} round(s); ${OUT} holds everything said so far\n`
    );
    // Stopping a review that has already handed something over is how it ends;
    // stopping one that has not is giving up on it.
    process.exit(rounds > 0 ? 0 : 130);
  });
}
