// The shell: header, keys, layout, and the wiring between the pieces.
//
//   session.mjs   what changed, and the two sides of each file
//   diffs.mjs     parsed diffs, cached
//   comments.mjs  the reviewer's comments, keyed by path
//   viewer.mjs    everything that knows about CodeView
//   sidebar.mjs   the file tree and its filter        (tree.mjs   — its model)
//   commentpane.mjs  every comment of the review, in one list

import { $, el, painted, splitLeftPx, whenIdle } from './dom.mjs';
import { Session } from './session.mjs';
import { Diffs } from './diffs.mjs';
import { Comments } from './comments.mjs';
import { CommentPane } from './commentpane.mjs';
import { ReviewViewer, ALL_FILES } from './viewer.mjs';
import { Sidebar } from './sidebar.mjs';
import { commentBox, headerCopyButton, headerFileSuffix } from './annotations.mjs';
import { findMatches, stepMatch } from './search.mjs';
import { clearMatches, paintMatches } from './highlight.mjs';

const params = new URLSearchParams(location.search);
const NONCE = document.querySelector('meta[name="diff-review-nonce"]').content;

/// An added line over a removed one, on a tile tinted from the checkout under
/// review, so two reviews open at once are told apart in the tab strip. Six
/// colours named one by one rather than a hue swept around the wheel: at a tile
/// dark enough to carry the marks, half the wheel comes out brown, and
/// neighbouring hues from the half that does not are two blues nobody tells
/// apart at 16 px. Each of these is its own step at that size.
const TINTS = [
  'oklch(0.36 0.10 195)', // teal
  'oklch(0.55 0.11 230)', // sky
  'oklch(0.34 0.13 255)', // blue
  'oklch(0.36 0.13 350)', // wine
  'oklch(0.30 0.02 255)', // graphite
  'oklch(0.36 0.15 300)', // violet
];
function favicon(repo) {
  let hash = 0;
  for (const ch of repo) hash = (hash * 31 + ch.charCodeAt(0)) >>> 0;
  const svg =
    `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 16 16">` +
    `<rect width="16" height="16" rx="3.5" fill="${TINTS[hash % TINTS.length]}"/>` +
    `<g stroke-width="2" stroke-linecap="round">` +
    `<path d="M4 5.4h8" stroke="#56d364"/><path d="M4 10.6h5.4" stroke="#ff7b72"/></g></svg>`;
  return `data:image/svg+xml,${encodeURIComponent(svg)}`;
}

const session = await Session.load();
$('repo').textContent = session.name;

/// The pull request this branch is already open as. Looked up on the server and
/// off its critical path, so it can arrive with the review or a poll later.
function showPr(pr) {
  if (pr == null || $('pr').href === pr.url) return;
  $('pr').href = pr.url;
  $('pr').textContent = `#${pr.number}${pr.draft ? ' (draft)' : ''}`;
  $('pr').title = pr.title ? `${pr.title} — opens on GitHub` : 'Opens on GitHub';
  $('pr').hidden = false;
}
showPr(session.pr);

/// The state of that pull request's checks, as one glyph: the worst of them, since
/// one failure outweighs any number of passes. Redrawn only when it actually
/// changes — the poll that carries it runs every couple of seconds.
const CI_GLYPHS = { success: ['\u2713', 'pass'], failure: ['\u2717', 'fail'], pending: ['\u25cb', 'run'] };
let ciShown = null;
function showCi(ci) {
  const glyph = ci == null ? null : CI_GLYPHS[ci.state];
  if (glyph == null) return;
  const signature = `${ci.state} ${ci.failed} ${ci.pending} ${ci.passed} ${ci.sameCommit}`;
  if (signature === ciShown) return;
  ciShown = signature;
  const counts = [
    ci.failed > 0 ? `${ci.failed} failing` : null,
    ci.pending > 0 ? `${ci.pending} running` : null,
    ci.passed > 0 ? `${ci.passed} passed` : null,
  ].filter((c) => c != null).join(', ');
  $('ci').textContent = glyph[0];
  $('ci').className = `ci ${glyph[1]}${ci.sameCommit ? '' : ' elsewhere'}`;
  $('ci').title = ci.sameCommit
    ? `CI: ${counts}`
    : `CI: ${counts} \u2014 on the pull request's head commit, not the one under review`;
  $('ci').hidden = false;
}
showCi(session.ci);
document.title = `review: ${session.name}`;
document.head.append(el('link', { rel: 'icon', href: favicon(session.repo) }));

/// The round being written now, and the last one handed to the session. Both
/// move while the page is open — a submit advances one and sets the other — so
/// the header is drawn from them rather than written once.
let round = session.round;
let handedOver = session.submitted;

/// What the review comes to in all, which does not change while the page is open.
const added = session.files.reduce((n, f) => n + f.added, 0);
const removed = session.files.reduce((n, f) => n + f.removed, 0);

/// The round is on the button that sends it and nowhere else: a second copy in
/// the bar, and the number of the round before it, only ever restated what that
/// button already says.
function updateHeader() {
  $('base').replaceChildren(
    `${session.range} · ${session.files.length} file(s) · `,
    el('span', { class: 'num' }, [
      el('span', { class: 'a', text: `+${added}` }),
      ' ',
      el('span', { class: 'd', text: `−${removed}` }),
    ]),
  );
  // The button says what it does; which round that is belongs in the tooltip and
  // on the dialog it opens, where the number is about to matter.
  $('finish').title = handedOver > 0
    ? `Hand these comments over as round ${round}; ${handedOver} sent so far. The review stays open.`
    : 'Hand these comments over. The review stays open, so you can keep reading and commenting.';
}
updateHeader();
$('help-out').textContent = session.out;
const notShown = [
  ...session.skipped.map((s) => `${s.path} (${s.reason})`),
  ...session.elsewhere.map((c) => `a comment on ${c.file}:${c.line} (not part of this review)`),
];
if (notShown.length > 0) {
  $('skipped').hidden = false;
  $('skipped').textContent = 'Not shown: ' + notShown.join(', ');
}

const comments = new Comments({ nextSeq: session.nextSeq });
const diffs = new Diffs(session);

// ── How the diff is laid out ─────────────────────────────────────────────────
// These four are the reviewer's preference, not a property of the review, so
// they outlive the page: a reload — including the one that comes from restarting
// the server to show a round's fixes — must not put the view back to the
// defaults. Each stored value is parsed strictly; anything else is the default.
const STORE = {
  theme: 'diff-review:theme',
  diffStyle: 'diff-review:diff-style',
  wholeFile: 'diff-review:whole-file',
  plainBackground: 'diff-review:plain-background',
};

function read(key) {
  try {
    return localStorage.getItem(key);
  } catch {
    return null; // a browser that refuses storage still gets to pick, for this page
  }
}

function write(key, value) {
  try {
    localStorage.setItem(key, String(value));
  } catch {
    // Not worth a word on screen: the choice holds for this page either way.
  }
}

/// `null` is the all-files view; every other view is one file.
let current;
let diffStyle = read(STORE.diffStyle) === 'unified' ? 'unified' : 'split';
/// Whole file on screen rather than hunks and their context. A mode, not a
/// per-file state: it holds as the reviewer moves through the review.
let wholeFile = read(STORE.wholeFile) === 'true';
/// No tint behind changed lines; the gutter markers still say what changed.
let plainBackground = read(STORE.plainBackground) === 'true';
const keyOf = (path) => (path == null ? ALL_FILES : path);

const viewer = new ReviewViewer({
  root: $('review-root'),
  session,
  diffs,
  annotationsFor: (path) =>
    comments.forFile(path).map((c) => ({
      side: c.side,
      lineNumber: c.end,
      metadata: { commentId: c.id },
    })),
  renderAnnotation: (annotation) => {
    const comment = comments.get(annotation?.metadata?.commentId);
    if (comment == null) return document.createElement('div');
    return commentBox(comment, {
      folded: folded.has(comment.extId),
      onToggleFold: () => toggleFold(comment),
      onInput: (text) => {
        comments.setText(comment.id, text);
        scheduleSave(); // a draft is only lost once
      },
      onSave: () => {
        comments.save(comment.id);
        // A saved follow-up supersedes what it answers.
        if (comment.replyTo != null) dismissByExtId(comment.replyTo);
      },
      onDiscard: () => comments.remove(comment.id),
      onEdit: () => comments.reopen(comment.id),
      onDelete: () => comments.remove(comment.id),
      onReply: () => replyTo(comment),
      onDismiss: () => dismiss(comment),
    });
  },
  onDraft: (path, range) => {
    const start = Math.min(range.start, range.end ?? range.start);
    const end = Math.max(range.start, range.end ?? range.start);
    comments.draft({ file: path, side: range.endSide ?? range.side ?? 'additions', start, end });
  },
  onSelectLine: (at) => setLineRef(at),
  renderHeaderControl: (path) => {
    const button = headerCopyButton(() => copyRef(path));
    copiers.set(path, () => button.click());
    return headerFileSuffix(diffs.fileLines(path), button);
  },
});

// ── The name and range last selected, as something to paste elsewhere ────────
// `path:line`, or `path:start-end` over a range — repo-relative, which is what an
// editor, a shell or a comment somewhere else expects. The range is the one last
// clicked, dragged over or jumped to; the control that copies it sits next to the
// file's name in its own header.
let lineRef = null;

function setLineRef(at) {
  lineRef = at;
}

/// The reference for a file: with the selected lines if the selection is in that
/// file, and the path alone if it is elsewhere, was cleared, or was never made.
function refFor(path) {
  if (lineRef?.path !== path) return path;
  const { start, end } = lineRef;
  return start === end ? `${path}:${start}` : `${path}:${start}-${end}`;
}

async function copyRef(path) {
  try {
    await navigator.clipboard.writeText(refFor(path));
    return true;
  } catch (err) {
    // Refusing quietly would look exactly like copying, so the button says so
    // and the reason goes where a reason can be read.
    paneInfo(`Cannot copy: ${err.message}`);
    return false;
  }
}

/// Every file header's copy control, so the key can reach the one on screen.
const copiers = new Map();

/// One count, in the header: how much is still open, and only when there is any.
/// The rest — addressed, and what is already with the session — is on hover,
/// where it costs nothing to carry.
function updateCommentUi() {
  const open = comments.open();
  const carried = open.filter((c) => c.carried).length;
  const done = comments.resolved().length;
  const total = open.length + done;

  $('ccountLabel').textContent = open.length > 0 ? String(open.length) : '';
  $('ccount').disabled = total === 0;
  $('ccount').title =
    (total === 0
      ? 'No comments yet'
      : `${open.length} open, ${done} addressed` + (carried > 0 ? `, ${carried} with Claude` : '')) +
    '  c';

  sidebar.setBadges(comments.countsByFile());
  if (!$('side-comments').hidden) commentPane.render(comments);
}

comments.onChange((path) => {
  viewer.refresh(path);
  updateCommentUi();
  scheduleSave();
});

// ── Comments folded away ─────────────────────────────────────────────────────
// A comment box sits between two lines of the diff, which is where it belongs
// while it is being dealt with and in the way once it has been. Folding leaves
// the header — who said it, where, and the first line of it — and takes the rest
// out of the flow. It is how the page looks, not what the review says, so it is
// held by `extId` (stable for the life of a comment), never written anywhere,
// and refreshes the file's annotations without touching the review file.
const folded = new Set();

function toggleFold(comment) {
  if (!folded.delete(comment.extId)) folded.add(comment.extId);
  viewer.refresh(comment.file);
}

// ── Keeping the review file up to date ───────────────────────────────────────
// Every comment is written to the review file as it is made, not only on submit:
// a closed tab, a killed server or a second round must not lose what the
// reviewer has already said. Writes are coalesced, and never overlap — a save
// that arrives while one is in flight runs after it, with the state by then.
let saveTimer = null;
let saving = false;
let saveAgain = false;

function scheduleSave() {
  clearTimeout(saveTimer);
  saveTimer = setTimeout(() => saveNow(), 400);
}

async function saveNow() {
  if (saving) {
    saveAgain = true;
    return;
  }
  saving = true;
  saveAgain = false;
  try {
    const res = await fetch('/save', {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'x-diff-review-nonce': NONCE },
      body: JSON.stringify({ overall: $('overall').value.trim(), comments: comments.state() }),
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    // Saving is the normal case and says nothing worth a word in the bar; the
    // indicator exists for the one state the reviewer has to know about.
    $('autosave').textContent = '';
    $('autosave').className = '';
  } catch (err) {
    // Silence here would be the one failure that costs the reviewer their work.
    $('autosave').textContent = `comments not saved (${err.message})`;
    $('autosave').className = 'bad';
  } finally {
    saving = false;
    if (saveAgain) scheduleSave();
  }
}

const sidebar = new Sidebar({
  treeEl: $('tree'),
  filterEl: $('filter'),
  foldAllEl: $('foldAll'),
  files: session.files,
  onSelect: (path) => showFile(path),
});

const commentPane = new CommentPane({
  el: $('side-comments'),
  paths: new Set(session.files.map((f) => f.path)),
  onGoto: (c) => goToComment(c),
  onReply: (c) => replyTo(c),
  onDismiss: (c) => dismiss(c),
});

/// The sidebar shows the files or the comments, never both: the tree is how a
/// review is read, the list is how it is answered.
function showSidePane(which) {
  const onComments = which === 'comments';
  // Asking for a pane is asking to see it, even with the sidebar folded away.
  if (onComments) {
    $('sidebar').hidden = false;
    $('resizer').hidden = false;
  }
  $('side-files').hidden = onComments;
  $('side-comments').hidden = !onComments;
  $('tabFiles').classList.toggle('active', !onComments);
  $('tabComments').classList.toggle('active', onComments);
  if (onComments) commentPane.render(comments);
}

$('tabFiles').addEventListener('click', () => showSidePane('files'));
$('tabComments').addEventListener('click', () => showSidePane('comments'));

// ── Showing a file ───────────────────────────────────────────────────────────
let switchToken = 0;
let hunkAt = -1;

async function showFile(path) {
  if (path === current) return;
  const key = keyOf(path);
  const token = ++switchToken;
  // A view that is already rendered comes back in a frame; a new one has to
  // tokenize the file first, and that blocks — so the spinner goes up first.
  const slow = !viewer.isRendered(key);
  if (slow) {
    $('busy').textContent =
      path == null ? `Rendering ${session.files.length} files…` : `Rendering ${path.split('/').pop()}…`;
    $('busy').hidden = false;
    await painted();
    if (token !== switchToken) return;
  }
  try {
    await viewer.prepare(key);
  } catch (err) {
    if (token === switchToken) {
      paneMessage(`Cannot show ${path ?? 'the review'}: ${err.message}`);
      $('busy').hidden = true;
    }
    return;
  }
  // Only the switch the reviewer is still waiting for is allowed to render:
  // two files can be loading at once, and the slower one must not win.
  if (token !== switchToken) return;
  viewer.display(key);
  current = path;
  hunkAt = -1;
  if ($('findInput').value.trim() !== '') runFind({ jump: false });
  paneMessage(null);
  paneInfo(path == null ? 'Every file on one page, without syntax colours: highlighting them all would take minutes.' : null);
  sidebar.setActive(path);
  viewer.scrollToTop(key);
  // The overlay can only come down a frame after the blocking render, and it is
  // taken down by whichever switch finishes last — including a fast one that
  // overtook a slow one.
  if (slow) await painted();
  if (token !== switchToken) return;
  $('busy').hidden = true;
  applySplit();
  updateNav();
  prefetchNeighbours(path);
}

/// Each pair of stepping buttons goes dead when there is nowhere to step: the
/// all-files page has no hunks of its own, and neither does a file whose whole
/// content is one change.
function updateNav() {
  const paths = sidebar.paths();
  const at = paths.indexOf(current);
  $('prevFile').disabled = paths.length === 0 || at === 0;
  $('nextFile').disabled = paths.length === 0 || at === paths.length - 1;
  const hunks = current == null ? 0 : viewer.hunkStarts(current).length;
  $('prevHunk').disabled = hunks === 0;
  $('nextHunk').disabled = hunks === 0;
}

/// Both neighbours are fetched right away — transfer is nothing next to
/// rendering — but only the file the reviewer is heading towards is rendered
/// ahead of time, and only while the browser is idle: a pre-render blocks like
/// any other, so one is a head start and two are a stall.
function prefetchNeighbours(path) {
  if (path == null) return;
  const [next] = session.neighbours(path);
  for (const neighbour of session.neighbours(path)) session.prefetch(neighbour);
  if (next == null) return;
  diffs
    .diff(next)
    .then(() => whenIdle(() => current === path && viewer.prerender(next)))
    .catch(() => {});
}

function paneMessage(text) {
  $('panemsg').textContent = text ?? '';
  $('panemsg').hidden = text == null;
}

/// A line under the file filter for whatever the pane is doing: which hunk is
/// in view, or why the all-files page looks the way it does.
function paneInfo(text) {
  $('paneinfo').textContent = text ?? '';
  $('paneinfo').hidden = text == null;
}

function stepFile(delta) {
  const paths = sidebar.paths();
  if (paths.length === 0) return;
  const at = paths.indexOf(current);
  showFile(paths[Math.min(paths.length - 1, Math.max(0, (at < 0 ? 0 : at) + delta))]);
}

function stepHunk(delta) {
  if (current == null) return;
  const starts = viewer.hunkStarts(current);
  if (starts.length === 0) return;
  const from = hunkAt < 0 ? (delta > 0 ? -1 : 0) : hunkAt;
  hunkAt = Math.min(starts.length - 1, Math.max(0, from + delta));
  const target = starts[hunkAt];
  // Selecting the line it lands on: scrolling alone leaves the reviewer looking
  // for what moved, and the viewport centre is not where the hunk starts.
  viewer.goToLine(keyOf(current), current, target.lineNumber, target.side, { select: true });
  paneInfo(`Hunk ${hunkAt + 1} of ${starts.length} · line ${target.lineNumber}`);
}

/// Goes to where a comment sits. Worth having because a comment carried over
/// from an earlier round can sit in a region the diff folds away — `goToLine`
/// unfolds it, so it can always be reached.
async function goToComment(comment) {
  if (comment.file !== current) await showFile(comment.file);
  if (comment.file !== current) return; // the switch failed; its message is on screen
  viewer.goToLine(keyOf(current), comment.file, comment.end, comment.side, { select: true });
  commentPane.setActive(comment.id);
  commentPane.render(comments);
}

/// Taking an addressed comment off the page, once the reviewer has read the
/// answer and is done with it. The review file keeps it, marked dismissed, so
/// what was asked and what was answered stay on the record.
async function dismiss(comment) {
  const extId = comments.dismiss(comment.id);
  if (extId == null) return;
  commentPane.render(comments);
  try {
    const res = await fetch('/dismiss', {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'x-diff-review-nonce': NONCE },
      body: JSON.stringify({ id: extId }),
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
  } catch (err) {
    // It is off the page either way; saying so is what keeps that honest.
    $('autosave').textContent = `not dismissed (${err.message})`;
    $('autosave').className = 'bad';
  }
}

function dismissByExtId(extId) {
  const target = comments.saved().find((c) => c.extId === extId);
  if (target != null) dismiss(target);
}

/// A follow-up on an addressed comment: a new comment on the same lines, linked
/// to the one it answers so the session can read the two together. The answered
/// one leaves the page when the follow-up is saved, not before — otherwise
/// abandoning the draft would lose both.
async function replyTo(comment) {
  await goToComment(comment);
  comments.draft({
    file: comment.file,
    side: comment.side,
    start: comment.start,
    end: comment.end,
    replyTo: comment.extId,
  });
}

/// Walks the open comments in file order, then the addressed ones: the work list
/// first, since that is what a round is made of.
let commentAt = -1;

async function stepComment(delta) {
  const list = [...comments.open(), ...comments.resolved()];
  if (list.length === 0) return;
  const from = commentAt < 0 ? (delta > 0 ? -1 : 0) : commentAt;
  commentAt = (from + delta + list.length) % list.length;
  const comment = list[commentAt];
  await goToComment(comment);
  paneInfo(`Comment ${commentAt + 1} of ${list.length}`);
}

// ── Finding text on this page ────────────────────────────────────────────────
// The browser's own find cannot read the diff: the viewer renders into a shadow
// root and only lays out what is on screen, so `cmd+F` would walk past most of
// the file. This searches what the page is made of instead — the file's own
// text, as fetched — and points the viewer at each line it lands on.
//
// It is the page, not the review: one file, or every file of the all-files view.
// A file is searched on the side it has, which for everything but a deletion is
// the new one; that is the file as the change leaves it, which is what a line
// number in the diff means.
let findHits = [];
let findAt = -1;

function findSources() {
  const paths = current == null ? session.files.map((f) => f.path) : [current];
  return paths.flatMap((path) => {
    const sides = session.sidesByPath.get(path);
    if (sides == null) return []; // not fetched: it is not on screen either
    const deleted = session.byPath.get(path)?.status === 'D';
    return [{ path, side: deleted ? 'deletions' : 'additions', text: deleted ? sides.old : sides.new }];
  });
}

/// The term is painted where it is rendered, so anything that changes what is on
/// screen asks for it again: a jump, a scroll, a file switch. Coalesced to a
/// frame, because a scroll asks on every one of them.
let repaintQueued = false;

function repaintFind() {
  if (repaintQueued) return;
  repaintQueued = true;
  requestAnimationFrame(() => {
    repaintQueued = false;
    if ($('findbar').hidden) clearMatches();
    else paintMatches(viewer.activePane(), $('findInput').value);
  });
}

/// Scrolling renders lines that were not there to paint: `scroll` does not
/// bubble, so this listens for it on the way down instead.
document.addEventListener('scroll', () => {
  if (!$('findbar').hidden) repaintFind();
}, true);

function showFind(on) {
  $('findbar').hidden = !on;
  if (on) {
    $('findInput').focus();
    $('findInput').select();
    runFind({ jump: false });
  } else {
    // The query and where the walk had got to are kept: `cmd+G` goes on stepping
    // it with the bar out of the way, which is what find-again means everywhere.
    // What is painted does not outlive the bar, though — the page is for reading.
    clearMatches();
    $('findInput').blur(); // back to the page, so the review's own keys answer again
  }
}

/// Re-runs the search over whatever the page now holds. `jump` goes to the first
/// hit, which is what typing does; re-running after a file switch only recounts.
function runFind({ jump = true } = {}) {
  const query = $('findInput').value;
  findHits = findMatches(findSources(), query);
  findAt = -1;
  updateFindCount();
  repaintFind();
  if (jump && findHits.length > 0) stepFind(1);
}

function updateFindCount() {
  const query = $('findInput').value.trim();
  const count = $('findCount');
  count.textContent =
    query === '' ? '' : findHits.length === 0 ? 'no match' : `${findAt < 0 ? '–' : findAt + 1}/${findHits.length} lines`;
  count.className = query !== '' && findHits.length === 0 ? 'none' : '';
}

/// `cmd+G`: the next match of the search already made. With nothing searched for
/// yet there is nothing to repeat, so it opens the bar to be typed into instead;
/// with a query whose matches are stale — the page changed while the bar was
/// closed — it counts them again before stepping.
function findAgain(delta) {
  if ($('findInput').value.trim() === '') {
    showFind(true);
    return;
  }
  $('findbar').hidden = false;
  if (findHits.length === 0) runFind({ jump: false });
  stepFind(delta);
}

function stepFind(delta) {
  findAt = stepMatch(findAt, delta, findHits.length);
  if (findAt < 0) return;
  const hit = findHits[findAt];
  viewer.goToLine(keyOf(current), hit.path, hit.line, hit.side, { select: true });
  updateFindCount();
  repaintFind(); // the lines it scrolled to may not have been rendered before
}

$('findInput').addEventListener('input', () => runFind());
$('findInput').addEventListener('keydown', (e) => {
  if (e.key === 'Enter') stepFind(e.shiftKey ? -1 : 1);
  else if (e.key === 'Escape') showFind(false);
  else if (e.key === 'f' && (e.metaKey || e.ctrlKey)) {
    $('findInput').select();
    e.preventDefault();
  } else if (e.key === 'g' && (e.metaKey || e.ctrlKey)) {
    findAgain(e.shiftKey ? -1 : 1);
    e.preventDefault();
  }
  e.stopPropagation();
});
$('findNext').addEventListener('click', () => stepFind(1));
$('findPrev').addEventListener('click', () => stepFind(-1));
$('findClose').addEventListener('click', () => showFind(false));

// ── Header, keys ─────────────────────────────────────────────────────────────
$('prevFile').addEventListener('click', () => stepFile(-1));
$('nextFile').addEventListener('click', () => stepFile(1));
$('prevHunk').addEventListener('click', () => stepHunk(-1));
$('nextHunk').addEventListener('click', () => stepHunk(1));
$('ccount').addEventListener('click', () => showSidePane('comments'));

/// The keys and the shape of a round, out of the way until asked for: every
/// control says its own key on hover, so the bar does not have to.
function showHelp(on) {
  $('help-overlay').hidden = !on;
}
$('help').addEventListener('click', () => showHelp(true));
$('help-close').addEventListener('click', () => showHelp(false));
$('help-overlay').addEventListener('click', (e) => {
  if (e.target === $('help-overlay')) showHelp(false);
});
// Each of the three view toggles has its state applied in one place, called both
// on a click and once at startup from what was stored — so a restored choice and
// a clicked one cannot drift apart.
function applyDiffStyle() {
  const unified = diffStyle === 'unified';
  $('styleToggle').classList.toggle('unified', unified);
  $('styleToggle').title = unified ? 'Unified view — click for split' : 'Split view — click for unified';
  viewer.setDiffStyle(diffStyle);
  applySplit();
}
$('styleToggle').addEventListener('click', () => {
  diffStyle = diffStyle === 'split' ? 'unified' : 'split';
  write(STORE.diffStyle, diffStyle);
  applyDiffStyle();
});
// ── Light, dark, or whatever the system says ─────────────────────────────────
// Two things to tell, because they decide independently: this page is written
// with `light-dark()`, which follows the `color-scheme` property, while the
// viewer picks between its own two themes from `themeType` — left at `system`
// it reads the OS preference and ignores anything the page says.
const THEMES = ['system', 'light', 'dark'];

const storedTheme = read(STORE.theme);
let theme = THEMES.includes(storedTheme) ? storedTheme : 'system';

function applyTheme() {
  document.documentElement.style.colorScheme = theme === 'system' ? 'light dark' : theme;
  viewer.setThemeType(theme);
  $('themeToggle').className = `icon theme-${theme}`;
  const next = THEMES[(THEMES.indexOf(theme) + 1) % THEMES.length];
  $('themeToggle').title =
    `Theme: ${theme === 'system' ? 'follows the system' : theme} — click for ${next}  t`;
}
applyTheme();

$('themeToggle').addEventListener('click', () => {
  theme = THEMES[(THEMES.indexOf(theme) + 1) % THEMES.length];
  write(STORE.theme, theme);
  applyTheme();
});

function applyPlainBackground() {
  $('bgToggle').classList.toggle('plain', plainBackground);
  $('bgToggle').title = plainBackground
    ? 'Changed lines are plain — click to tint them  b'
    : 'Changed lines are tinted — click for plain  b';
  viewer.setPlainBackground(plainBackground);
}
$('bgToggle').addEventListener('click', () => {
  plainBackground = !plainBackground;
  write(STORE.plainBackground, plainBackground);
  applyPlainBackground();
});
function applyWholeFile() {
  $('foldToggle').title = wholeFile
    ? 'Whole files — click to fold the unchanged parts  e'
    : 'Whole file, not just the hunks  e';
  $('foldToggle').classList.toggle('on', wholeFile);
  viewer.setExpandUnchanged(wholeFile);
  applySplit();
}
$('foldToggle').addEventListener('click', () => {
  wholeFile = !wholeFile;
  write(STORE.wholeFile, wholeFile);
  applyWholeFile();
});

$('sideToggle').addEventListener('click', () => {
  const hide = !$('sidebar').hidden;
  $('sidebar').hidden = hide;
  $('resizer').hidden = hide;
});
$('filter').addEventListener('input', () => updateNav()); // the filter is what "next file" walks
$('filter').addEventListener('keydown', (e) => {
  if (e.key === 'Enter') stepFile(0);
  if (e.key === 'Escape') {
    $('filter').value = '';
    sidebar.refresh();
    updateNav();
    $('filter').blur();
  }
  e.stopPropagation();
});

function focusSidebar(input) {
  $('sidebar').hidden = false;
  $('resizer').hidden = false;
  input.focus();
  input.select();
}

document.addEventListener('keydown', (e) => {
  // Before the guard below: this is the one combination the page takes over, and
  // it has to work from anywhere, including out of a box being typed in.
  if (e.key === 'f' && (e.metaKey || e.ctrlKey) && !e.altKey) {
    showFind(true);
    e.preventDefault();
    return;
  }
  if (e.key === 'g' && (e.metaKey || e.ctrlKey) && !e.altKey) {
    findAgain(e.shiftKey ? -1 : 1);
    e.preventDefault();
    return;
  }
  if (e.target instanceof HTMLTextAreaElement || e.target instanceof HTMLInputElement) return;
  if (e.metaKey || e.ctrlKey || e.altKey) return;
  if (e.key === 'Escape' && !$('help-overlay').hidden) showHelp(false);
  else if (e.key === '?') showHelp($('help-overlay').hidden);
  else if (e.key === ',' || e.key === 'k') stepFile(-1);
  else if (e.key === '.' || e.key === 'j') stepFile(1);
  else if (e.key === ']') stepHunk(1);
  else if (e.key === '[') stepHunk(-1);
  else if (e.key === 'c') showSidePane($('side-comments').hidden ? 'comments' : 'files');
  else if (e.key === 'C') stepComment(1);
  else if (e.key === 'y') copiers.get(lineRef?.path ?? current)?.();
  else if (e.key === 'r') $('reload').click();
  else if (e.key === 'e') $('foldToggle').click();
  else if (e.key === 'b') $('bgToggle').click();
  else if (e.key === 't') $('themeToggle').click();
  else if (e.key === '/') {
    focusSidebar($('filter'));
    e.preventDefault();
  } else if (e.key === '\\') $('sideToggle').click();
});

// ── Layout: sidebar width and the split boundary ─────────────────────────────
/// Pointer capture keeps a drag alive while the pointer travels over the diff's
/// shadow DOM, which otherwise swallows `pointermove`.
function draggable(bar, onMove) {
  bar.addEventListener('pointerdown', (e) => {
    bar.setPointerCapture(e.pointerId);
    bar.classList.add('dragging');
    const move = (ev) => onMove(ev);
    const up = () => {
      bar.classList.remove('dragging');
      bar.removeEventListener('pointermove', move);
      bar.removeEventListener('pointerup', up);
    };
    bar.addEventListener('pointermove', move);
    bar.addEventListener('pointerup', up);
    e.preventDefault();
  });
}

draggable($('resizer'), (ev) => {
  const width = Math.min(Math.max(ev.clientX, 160), Math.round(window.innerWidth * 0.6));
  document.documentElement.style.setProperty('--sidebar-w', `${width}px`);
});

/// The handle is a host-page overlay placed on the column boundary inside the
/// viewer's shadow DOM. It is measured from the visible pane's client width, not
/// the pane box, or it sits off by the scrollbar. `--split-left` inherits through
/// the shadow boundary and drives the columns with no re-render.
let splitFraction = 0.5;

function paneWidth() {
  return viewer.activePane()?.clientWidth || $('review-root').clientWidth;
}

function applySplit() {
  document.documentElement.style.setProperty('--split-left', `${(splitFraction * 100).toFixed(2)}%`);
  $('splitter').hidden = !viewer.isSplitView(keyOf(current));
  $('splitter').style.left = `${splitLeftPx(paneWidth(), splitFraction)}px`;
}

draggable($('splitter'), (ev) => {
  const rect = $('review-root').getBoundingClientRect();
  splitFraction = Math.min(0.85, Math.max(0.15, (ev.clientX - rect.left) / paneWidth()));
  applySplit();
});
$('splitter').addEventListener('dblclick', () => {
  splitFraction = 0.5;
  applySplit();
});
new ResizeObserver(() => applySplit()).observe($('review-root'));

// ── Sending a round, and ending the review ───────────────────────────────────
// Sending is not the end of the review: the session starts on what it was just
// handed while the page stays up, so everything written from here on is the next
// round. The review ends when the reviewer says so, which is what stops the
// server.
$('finish').addEventListener('click', () => {
  const drafts = comments.drafts();
  if (drafts.length > 0 && !confirm(`${drafts.length} unsaved draft comment(s) will be discarded. Continue?`)) {
    return;
  }
  const open = comments.open();
  $('finish-title').textContent = `Send round ${round} to Claude`;
  $('finish-summary').textContent =
    (open.length === 0
      ? 'No open line comments. You can still leave an overall comment below.'
      : `${open.length} line comment(s) go to the session.`) +
    ' The review stays open — keep commenting while it works.';
  $('finish-overlay').hidden = false;
  $('overall').focus();
});
$('finish-cancel').addEventListener('click', () => ($('finish-overlay').hidden = true));

async function submit(verdict) {
  const res = await fetch('/submit', {
    method: 'POST',
    headers: { 'content-type': 'application/json', 'x-diff-review-nonce': NONCE },
    body: JSON.stringify({
      verdict,
      overall: $('overall').value.trim(),
      comments: comments.payload(),
    }),
  });
  if (!res.ok) {
    alert('Sending the round failed — is the server still running?');
    return;
  }
  const sent = await res.json();
  handedOver = sent.submitted;
  round = sent.round;
  // What was open is now with the session: it reads as an earlier round from
  // here on, which is what keeps it apart from whatever is written next.
  comments.markSubmitted(handedOver);
  updateHeader();
  updateCommentUi();
  $('finish-overlay').hidden = true;
  $('sent-title').textContent = `Round ${handedOver} sent ✓`;
  $('sent-sub').textContent =
    verdict === 'approve'
      ? 'Approved — the session can go ahead. Anything you add now becomes round ' +
        `${round}, so end the review if you are done.`
      : 'Claude is working on it, and each comment turns green here as it is ' +
        `addressed. Anything you add now becomes round ${round}.`;
  $('sent-keep').className = verdict === 'approve' ? '' : 'primary';
  $('sent-close').className = verdict === 'approve' ? 'primary' : '';
  $('sent-overlay').hidden = false;
}
$('finish-approve').addEventListener('click', () => submit('approve'));
$('finish-request').addEventListener('click', () => submit('request_changes'));
$('sent-keep').addEventListener('click', () => ($('sent-overlay').hidden = true));
$('sent-close').addEventListener('click', () => endReview());

$('end').addEventListener('click', () => {
  const unsent = comments.open().filter((c) => !c.carried).length;
  if (unsent > 0 && !confirm(`${unsent} comment(s) were never sent to Claude. End the review anyway?`)) {
    return;
  }
  endReview();
});

/// Closing is the reviewer's to do: it stops the server, which is what tells the
/// session no more rounds are coming.
async function endReview() {
  await saveNow(); // the last keystroke belongs in the file too
  try {
    await fetch('/close', {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'x-diff-review-nonce': NONCE },
      body: '{}',
    });
  } catch {
    // The server closes the connection as it exits; that is the point.
  }
  $('sent-overlay').hidden = true;
  $('done-overlay').hidden = false;
}

// ── What the session is doing with the comments ──────────────────────────────
// It marks each one resolved in the review file as it deals with it, so polling
// is how they turn green here — without the reviewer reloading and losing their
// place, and without the diff on screen moving under them.
setInterval(async () => {
  let status;
  try {
    const res = await fetch('/status');
    if (!res.ok) return;
    status = await res.json();
  } catch {
    return; // the server is busy or gone; the next tick asks again
  }
  round = status.round;
  handedOver = status.submitted;
  updateHeader();
  if (comments.applyResolved(status.resolved)) updateCommentUi();
  showReload(status.changed ?? []);
  showPr(status.pr ?? null);
  showCi(status.ci ?? null);
}, 2000);

// ── Reading the diff again ───────────────────────────────────────────────────
// Always available, and always the reviewer's own act: the server re-reads the
// whole diff — the file set, the statuses, both sides of every file — when this
// page loads, and holds it still in between, because a diff that reflowed while a
// comment was being written on it would take the lines that comment names with
// it. A reload comes back to the same review, every comment re-anchored to where
// its line has moved to.
//
// The server also watches what it has served, and says which of those files are
// no longer the bytes on screen. That does not re-read anything; it colours the
// control, so there is a reason to press it rather than a diff moving unasked.
const changedOnDisk = new Set();

function showReload(changed) {
  for (const path of changed) changedOnDisk.add(path);
  if (changedOnDisk.size === 0) return;
  const names = [...changedOnDisk].map((p) => p.split('/').pop());
  $('reload').classList.add('stale');
  $('reloadLabel').textContent = 'Reload';
  $('reload').title =
    `${changedOnDisk.size} file${changedOnDisk.size === 1 ? '' : 's'} changed on disk since this page read ` +
    `${changedOnDisk.size === 1 ? 'it' : 'them'} — ${names.slice(0, 6).join(', ')}` +
    `${names.length > 6 ? ', …' : ''}. Read the diff again (r); comments follow their lines.`;
}

$('reload').addEventListener('click', () => location.reload());

// ── Boot ─────────────────────────────────────────────────────────────────────
// The stored view choices go on before the first file is rendered: the viewer
// reads them as it builds each view, so seeding them here is what keeps a
// restored choice from reaching only the second file onwards. They belong to the
// boot rather than to their buttons, because `applySplit` reads layout state
// declared further down the file.
applyDiffStyle();
applyPlainBackground();
applyWholeFile();

// Whatever the last round left open is on screen from the first frame, on the
// file it belongs to rather than the first one, so it cannot be reviewed past.
// The addressed ones come back too, or a review whose every comment has been
// resolved reopens as an empty one.
comments.adopt(session.carried);
comments.adopt(session.resolved);
await showFile(session.carried[0]?.file ?? session.resolved?.[0]?.file ?? session.files[0]?.path ?? null);
$('loading').remove();
updateCommentUi();

// Test hook: ?whole opens in whole-file mode, so a headless run can check that
// nothing is left folded. It asks for the mode rather than toggling it — a stored
// preference from an earlier run would otherwise turn the mode off.
if (params.has('whole') && !wholeFile) $('foldToggle').click();

// Test hook: ?testhunk=<n> steps that many hunks forward, so a headless run can
// check that arriving at one marks where it landed.
if (params.has('testhunk')) for (let i = 0; i < Number(params.get('testhunk')); i++) stepHunk(1);

// Test hook: ?testannotation seeds a saved, a draft and a resolved comment on
// lines the rendered diff actually contains, so a headless run can check how the
// three of them render.
if (params.has('testannotation') && current != null) {
  const line = viewer.hunkStarts(current)[0]?.lineNumber ?? 1;
  comments.draft({
    file: current,
    side: 'additions',
    start: line,
    end: line + 1,
    text: 'Saved test comment: please rename this.',
    saved: true,
  });
  comments.draft({ file: current, side: 'additions', start: line + 4, end: line + 4 });
  comments.draft({
    file: current,
    side: 'additions',
    start: line + 6,
    end: line + 6,
    text: 'Resolved test comment: this was said last round.',
    saved: true,
    carried: true,
    round: 1,
    resolved: true,
    resolution: 'renamed it, and the caller with it',
  });
}

// Test hook: ?testsend seeds a comment and sends the round, so a headless run
// can check that the review carries on afterwards rather than closing.
if (params.has('testsend') && current != null) {
  const line = viewer.hunkStarts(current)[0]?.lineNumber ?? 1;
  comments.draft({ file: current, side: 'additions', start: line, end: line,
    text: 'Sent by the test hook.', saved: true });
  await submit('request_changes');
}
