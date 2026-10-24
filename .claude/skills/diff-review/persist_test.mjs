// Tests for the half of the review that outlives the browser: comments are
// written as they are made, survive a killed server, come back in the next round
// on whatever line the code has moved to, and disappear only once something
// marks them resolved.
//
//   node <skill-dir>/persist_test.mjs
//
// Self-contained: it builds a throwaway git repository under this skill's own
// tmp/ — gitignored here, and never in the repository being worked on, wherever
// the test is run from — and runs real servers against it, so it needs no review
// to be open and touches nothing else.
import { spawn, execFileSync } from 'node:child_process';
import { mkdirSync, mkdtempSync, writeFileSync, readFileSync, rmSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const SKILL_DIR = dirname(fileURLToPath(import.meta.url));
const SERVER = join(SKILL_DIR, 'server.mjs');
const PORT = Number(process.env.DIFF_REVIEW_TEST_PORT ?? 3199);

let failures = 0;
const check = (name, cond, extra = '') => {
  if (!cond) failures++;
  console.log(`${cond ? 'ok  ' : 'FAIL'} ${name}${extra ? ' — ' + extra : ''}`);
};

mkdirSync(join(SKILL_DIR, 'tmp'), { recursive: true });
const repo = mkdtempSync(join(SKILL_DIR, 'tmp', 'diff-review-test-'));
const OUT = join(repo, 'review.json');
const git = (...a) => execFileSync('git', ['-C', repo, ...a], { encoding: 'utf8' });
git('init', '-q');
git('config', 'user.email', 'test@example.com');
git('config', 'user.name', 'test');
git('config', 'commit.gpgsign', 'false'); // a throwaway repo must not need the user's key
writeFileSync(join(repo, 'f.txt'), 'base\n');
git('add', '-A');
git('commit', '-qm', 'base');

/// The reviewed file, in the two shapes the test needs: as it is when the
/// comments are made, and as a round of fixes leaves it.
const body = ({ prelude = [], keepDoomed = true, rewrite = false } = {}) =>
  [
    ...prelude,
    'void first();',
    'void second();',
    ...(keepDoomed ? ['void doomed();'] : []),
    rewrite ? 'void rewritten_entirely();' : 'void third();',
    'void fourth();',
  ].join('\n') + '\n';

function start() {
  const proc = spawn(
    'node',
    [SERVER, '--repo', repo, '--base', 'HEAD', '--port', String(PORT), '--out', OUT, '--no-open'],
    { stdio: ['ignore', 'pipe', 'pipe'] }
  );
  let log = '';
  proc.stdout.on('data', (d) => (log += d));
  proc.stderr.on('data', (d) => (log += d));
  return new Promise((ok, no) => {
    const poll = setInterval(async () => {
      if (proc.exitCode != null) {
        clearInterval(poll);
        no(new Error(`server exited ${proc.exitCode}:\n${log}`));
        return;
      }
      try {
        const res = await fetch(`http://localhost:${PORT}/data`);
        if (!res.ok) return;
        clearInterval(poll);
        const html = await (await fetch(`http://localhost:${PORT}/`)).text();
        const nonce = /name="diff-review-nonce" content="([^"]+)"/.exec(html)[1];
        ok({ proc, data: await res.json(), nonce });
      } catch {
        // not listening yet
      }
    }, 50);
  });
}

const stop = (s) => new Promise((ok) => { s.proc.on('exit', ok); s.proc.kill('SIGKILL'); });
const post = (s, where, body) =>
  fetch(`http://localhost:${PORT}${where}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', 'x-diff-review-nonce': s.nonce },
    body: JSON.stringify(body),
  });
const read = () => JSON.parse(readFileSync(OUT, 'utf8'));
/// What a reload does: ask the running server for the review again.
const reload = async (s) => {
  s.data = await (await fetch(`http://localhost:${PORT}/data`)).json();
  return s.data;
};
const wire = (c) => ({
  id: c.id, file: c.file, side: c.side === 'deletions' ? 'old' : 'new',
  startLine: c.start, endLine: c.end, comment: c.text, round: c.round,
});

// ── Round 1: three comments, then the server is killed ───────────────────────
writeFileSync(join(repo, 'f.txt'), body()); // 1 first  2 second  3 doomed  4 third  5 fourth
let s = await start();
check('a fresh review carries nothing', s.data.carried.length === 0 && s.data.round === 1);

const res = await post(s, '/save', { overall: '', comments: [
  { file: 'f.txt', side: 'new', startLine: 2, endLine: 2, comment: 'on second()' },
  { file: 'f.txt', side: 'new', startLine: 3, endLine: 3, comment: 'on the doomed line' },
  { file: 'f.txt', side: 'new', startLine: 4, endLine: 5, comment: 'a range', draft: true },
]});
check('the UI can write to the review file', res.status === 200);
let saved = read();
check('comments are on disk before any submit',
  saved.status === 'in_progress' && saved.comments.length === 3);
check('every comment gets an id', saved.comments.every((c) => /^c\d+$/.test(c.id)),
  saved.comments.map((c) => c.id).join(','));
// The anchor is the end of the range: that is the line the box hangs off.
check('each one records the text of the line it was filed on',
  saved.comments.map((c) => c.anchor).join('|') === 'void second();|void doomed();|void fourth();',
  saved.comments.map((c) => c.anchor).join('|'));
check('a half-typed comment is kept, and marked as one',
  saved.comments.filter((c) => c.draft === true).length === 1);

// The page is served from the file, not from a snapshot taken at startup: a
// reload has to come back to everything said so far, this round's included.
const reloaded = await (await fetch(`http://localhost:${PORT}/data`)).json();
check('reloading comes back to the comments made since the server started',
  reloaded.carried.length === 3 && reloaded.carried.every((c) => c.carried === false),
  `${reloaded.carried.length} carried, ${reloaded.carried.filter((c) => c.carried).length} as earlier rounds`);
await stop(s);

// ── Round 2: the fix moves the code under the comments ───────────────────────
writeFileSync(join(repo, 'f.txt'),
  body({ prelude: ['#include <a>', '#include <b>', ''], keepDoomed: false, rewrite: true }));
// 1 #include <a>  2 #include <b>  3 (blank)  4 first  5 second  6 rewritten  7 fourth

s = await start();
const at = (text) => s.data.carried.find((c) => c.text === text);
check('a killed server loses nothing', s.data.carried.length === 3, `${s.data.carried.length} carried`);
// Restarting the server is not a round. Nothing was handed over, so what was
// written before the kill still belongs to the round being written — counting it
// as a new one inflates "how many times have I sent this", and makes comments
// nobody has seen read as if they were already with the session.
check('a restart does not advance the round',
  s.data.round === 1 && s.data.carried.every((c) => c.round === 1), `round ${s.data.round}`);
check('…so comments that were never sent do not read as handed over',
  s.data.carried.every((c) => c.carried === false));
check('a draft comes back a draft', s.data.carried.filter((c) => c.draft === true).length === 1);
check('a comment follows its line down the file',
  at('on second()').end === 5 && at('on second()').moved === true && at('on second()').movedFrom === 2,
  JSON.stringify(at('on second()')));
check('a range keeps its width when it moves',
  at('a range').end - at('a range').start === 1, JSON.stringify(at('a range')));
check('a comment whose line was deleted says so, and stays inside the file',
  at('on the doomed line').stale === true && at('on the doomed line').end <= 7,
  JSON.stringify(at('on the doomed line')));

// Resolving is what ends a comment's life — nothing else does.
const resolvedId = at('on second()').id;
const now = read();
for (const c of now.comments) if (c.id === resolvedId) { c.resolved = true; c.resolution = 'renamed'; }
writeFileSync(OUT, JSON.stringify(now, null, 2) + '\n');
await stop(s);

// ── Round 3: what was resolved is gone, what was not is back ─────────────────
s = await start();
check('a resolved comment does not come back',
  s.data.carried.length === 2 && !s.data.carried.some((c) => c.id === resolvedId),
  s.data.carried.map((c) => c.id).join(','));
check('…but stays in the file for the record',
  read().comments.some((c) => c.id === resolvedId && c.resolved === true && c.resolution === 'renamed'));
// It is off the work list, not off the page: a reload has to be able to show
// what was addressed, or a review whose every comment is resolved comes back
// looking empty.
check('…and a reload is still told about it',
  s.data.resolved?.length === 1 && s.data.resolved[0].id === resolvedId
    && s.data.resolved[0].resolved === true && s.data.resolved[0].resolution === 'renamed',
  JSON.stringify(s.data.resolved));
check('ids are never reused', s.data.nextSeq > 3, `nextSeq ${s.data.nextSeq}`);

// ── Dismissing what has been read ────────────────────────────────────────────
// The reviewer read the answer and is done with it. That is recorded, not acted
// on by deletion: the question and the answer are worth keeping.
const dismissRes = await post(s, '/dismiss', { id: resolvedId });
check('dismissing an addressed comment is accepted', dismissRes.ok === true, JSON.stringify(dismissRes));
check('…and is recorded in the file, comment and answer intact',
  read().comments.some((c) => c.id === resolvedId && c.dismissed === true
    && c.resolution === 'renamed' && c.comment !== ''));
check('dismissing a comment that is not there is refused',
  (await post(s, '/dismiss', { id: 'nope' })).ok === false);
// A save from the page must not undo it: the page no longer holds the comment,
// so a naive rewrite would drop the flag with it.
await post(s, '/save', { overall: '', comments: s.data.carried.map((c) => ({
  id: c.id, file: c.file, side: 'new', startLine: c.start, endLine: c.end, comment: c.text })) });
check('…and a later save from the page keeps it dismissed',
  read().comments.some((c) => c.id === resolvedId && c.dismissed === true));
await stop(s);

s = await start();
check('a dismissed comment comes back on no list at all',
  !s.data.carried.some((c) => c.id === resolvedId)
    && !(s.data.resolved ?? []).some((c) => c.id === resolvedId),
  `${s.data.carried.length} carried, ${(s.data.resolved ?? []).length} resolved`);
check('…and is still in the file', read().comments.some((c) => c.id === resolvedId));

// A follow-up on an addressed comment carries the link, so the session can read
// the question, its answer and what is being asked now as one thread.
await post(s, '/save', { overall: '', comments: [
  { id: 'c50', file: 'f.txt', side: 'new', startLine: 1, endLine: 1,
    comment: 'still not convinced', replyTo: 'c9' },
] });
check('a follow-up keeps what it replies to',
  read().comments.some((c) => c.id === 'c50' && c.replyTo === 'c9'));

// A comment on a file this review does not show: not displayable, still open.
const held = read();
held.comments.push({ id: 'c99', file: 'elsewhere.txt', side: 'new', startLine: 1, endLine: 1,
  comment: 'on a file outside this review', resolved: false, round: 1 });
writeFileSync(OUT, JSON.stringify(held, null, 2) + '\n');
await stop(s);

s = await start();
check('a comment on a file outside the review is not shown',
  !s.data.carried.some((c) => c.file === 'elsewhere.txt') &&
    s.data.elsewhere.some((c) => c.file === 'elsewhere.txt'), JSON.stringify(s.data.elsewhere));
await post(s, '/save', { overall: '', comments: [] });
check('…and survives a write it took no part in',
  read().comments.some((c) => c.id === 'c99'));

// ── Submitting hands a round over and leaves the review open ─────────────────
const submitting = s.data.carried.filter((c) => c.draft !== true).map(wire);
const sentRound = s.data.round;
await post(s, '/submit', { verdict: 'request_changes', overall: 'one more round', comments: submitting });
const final = read();
check('submitting marks the file submitted',
  final.status === 'submitted' && final.verdict === 'request_changes' &&
    final.overall === 'one more round' && final.submitted?.round === sentRound,
  JSON.stringify({ status: final.status, submitted: final.submitted }));
check('a draft that was never saved is not part of the review',
  !final.comments.some((c) => c.draft === true));
check('open comments come before resolved ones',
  final.comments.findIndex((c) => c.resolved === true) === final.comments.length - 1);
check('the review is still up afterwards', s.proc.exitCode === null);

const status = async () => (await fetch(`http://localhost:${PORT}/status`)).json();
check('the round advances, so what is said next is the next round',
  (await status()).round === sentRound + 1);

// What was handed over keeps the round it was first said in; what is written
// after it belongs to the round now open.
const handed = submitting[0];
await post(s, '/save', { overall: '', comments: [
  ...submitting,
  { file: 'f.txt', side: 'new', startLine: 1, endLine: 1, comment: 'said after the hand-over' },
]});
const during = read();
const said = (text) => during.comments.find((c) => c.comment === text);
check('a comment written after a submit belongs to the next round',
  said('said after the hand-over').round === sentRound + 1,
  `round ${said('said after the hand-over').round}`);
check('…and one already handed over keeps the round it was made in',
  during.comments.find((c) => c.id === handed.id).round === handed.round,
  `round ${during.comments.find((c) => c.id === handed.id).round}`);

// ── The session resolves a comment while the review is still open ────────────
const marked = read();
for (const c of marked.comments) {
  if (c.id === handed.id) { c.resolved = true; c.resolution = 'fixed while you read on'; }
}
writeFileSync(OUT, JSON.stringify(marked, null, 2) + '\n');
const live = await status();
check('the page is told what the session has addressed',
  live.resolved.some((r) => r.id === handed.id && r.resolution === 'fixed while you read on'),
  JSON.stringify(live.resolved));
// The page has not noticed yet and sends the comment back as open anyway.
await post(s, '/save', { overall: '', comments: [
  ...submitting,
  { file: 'f.txt', side: 'new', startLine: 1, endLine: 1, comment: 'said after the hand-over' },
]});
const merged = read();
const stillThere = merged.comments.filter((c) => c.id === handed.id);
check('a comment resolved while the page still holds it is not reopened',
  stillThere.length === 1 && stillThere[0].resolved === true &&
    stillThere[0].resolution === 'fixed while you read on',
  JSON.stringify(stillThere));

// ── A reload re-reads the diff, as a restart would ───────────────────────────
// The file set is not a startup constant: a fix that adds or removes a file has to
// show up without the server being restarted, and a comment on a file that leaves
// the diff has to survive in the file even though it can no longer be drawn.
const before = s.data.files.length;
writeFileSync(join(repo, 'g.txt'), 'brand new\n');
await reload(s);
check('a file that joins the diff appears on a reload',
  s.data.files.length === before + 1 && s.data.files.some((f) => f.path === 'g.txt'),
  s.data.files.map((f) => f.path).join(','));

const gRes = await post(s, '/save', { overall: '', comments: [
  ...s.data.carried.map(wire),
  { file: 'g.txt', side: 'new', startLine: 1, endLine: 1, comment: 'on the new file' },
] });
check('a comment can be made on the file that joined', gRes.status === 200);

rmSync(join(repo, 'g.txt'));
await reload(s);
check('…and it is gone again once the file leaves the diff',
  !s.data.files.some((f) => f.path === 'g.txt') && s.data.files.length === before,
  s.data.files.map((f) => f.path).join(','));
check('…while the comment on it is kept, off the page but still open',
  s.data.elsewhere.some((c) => c.file === 'g.txt') &&
    read().comments.some((c) => c.file === 'g.txt' && c.resolved === false),
  JSON.stringify(s.data.elsewhere));

// Content, not just the file list: the bytes of a file already on screen are read
// again too, which is what makes a comment follow its line after a round of fixes.
writeFileSync(join(repo, 'f.txt'), 'inserted at the top\n' + readFileSync(join(repo, 'f.txt'), 'utf8'));
const movedBy1 = read().comments.find((c) => c.file === 'f.txt' && c.resolved === false);
await reload(s);
const after = s.data.carried.find((c) => c.id === movedBy1.id);
check('a reload re-reads the bytes and moves comments with them',
  after != null && after.start === movedBy1.startLine + 1,
  `${movedBy1.startLine} -> ${after?.start}`);

// ── Ending the review ────────────────────────────────────────────────────────
await post(s, '/close', {});
const closed = await new Promise((ok) => s.proc.on('exit', ok));
check('closing the review stops the server', closed === 0, `exit ${closed}`);

// ── A file that cannot be read must never be clobbered ───────────────────────
writeFileSync(OUT, 'not json at all');
const bad = spawn('node', [SERVER, '--repo', repo, '--base', 'HEAD', '--port', String(PORT),
  '--out', OUT, '--no-open'], { stdio: ['ignore', 'pipe', 'pipe'] });
let badLog = '';
bad.stderr.on('data', (d) => (badLog += d));
const code = await new Promise((ok) => bad.on('exit', ok));
check('a corrupt review file stops the server instead of being overwritten',
  code === 1 && readFileSync(OUT, 'utf8') === 'not json at all', `exit ${code}: ${badLog.trim()}`);

rmSync(repo, { recursive: true, force: true });
console.log(failures === 0 ? '\nall checks passed' : `\n${failures} check(s) failed`);
process.exit(failures === 0 ? 0 : 1);
