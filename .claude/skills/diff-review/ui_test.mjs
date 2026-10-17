// Unit tests for the parts of the review UI that are pure logic: the file tree
// model, comment state and the split geometry. They import the shipped modules,
// so the tested code is the code the browser gets.
//
//   node <skill-dir>/ui_test.mjs                       # against a running server
//   DIFF_REVIEW_URL=http://localhost:3001 node …       # …on another port
//
// Run it from the repository root of the review being served; it reads the
// review from the server.
import { readFileSync } from 'node:fs';
import { execFileSync } from 'node:child_process';

import { buildRows, visibility, visiblePaths } from './ui/tree.mjs';
import { Comments } from './ui/comments.mjs';
import { Session } from './ui/session.mjs';
import { findMatches, stepMatch } from './ui/search.mjs';
import { splitLeftPx } from './ui/dom.mjs';

const BASE_URL = process.env.DIFF_REVIEW_URL ?? 'http://localhost:3000';
const data = await (await fetch(`${BASE_URL}/data`)).json();

let failures = 0;
const check = (name, cond, extra = '') => {
  if (!cond) failures++;
  console.log(`${cond ? 'ok  ' : 'FAIL'} ${name}${extra ? ' — ' + extra : ''}`);
};

// ── The file tree ────────────────────────────────────────────────────────────
const rows = buildRows(data.files);
const fileRows = rows.filter((r) => r.kind === 'file');
const dirRows = rows.filter((r) => r.kind === 'dir');

check('every file gets a row', fileRows.length === data.files.length,
  `${fileRows.length}/${data.files.length}`);
check('the tree starts with an "All files" row', rows[0].kind === 'all' && rows[0].count === data.files.length);
check('directory rows exist', dirRows.length > 0, `${dirRows.length} dirs`);

const folded = dirRows.filter((r) => r.label.includes('/'));
check('single-child dir chains are folded', folded.length > 0,
  folded.slice(0, 3).map((r) => r.label).join(', '));

const all = visibility(rows, {});
check('everything is visible without a filter', visiblePaths(rows, all).length === data.files.length);

const needlePath = data.files[0].path.split('/').pop();
const filtered = visibility(rows, { query: needlePath });
const shownFiles = visiblePaths(rows, filtered);
check('the filter narrows to matching paths',
  shownFiles.length > 0 && shownFiles.every((p) => p.includes(needlePath)), shownFiles.join(', '));
const shownDirs = rows.filter((r, i) => r.kind === 'dir' && filtered[i]).map((r) => r.dirKey);
check('ancestors of a match stay visible',
  shownFiles.every((p) => fileRows.find((r) => r.path === p).ancestors.every((a) => shownDirs.includes(a))));

check('no match hides every file',
  visiblePaths(rows, visibility(rows, { query: 'zzz-no-such-file' })).length === 0);

const dir = dirRows.find((d) => fileRows.some((f) => f.ancestors.includes(d.dirKey)));
const under = fileRows.filter((f) => f.ancestors.includes(dir.dirKey)).map((f) => f.path);
const collapsed = visibility(rows, { collapsed: new Set([dir.dirKey]) });
const shownWhenCollapsed = visiblePaths(rows, collapsed);
check('collapsing a directory hides its subtree',
  under.every((p) => !shownWhenCollapsed.includes(p)), `${dir.dirKey} (${under.length} files)`);
check('collapsing keeps the directory row itself visible', collapsed[rows.indexOf(dir)]);
check('collapsing leaves other files alone',
  shownWhenCollapsed.length === data.files.length - under.length);

// ── Comment state ────────────────────────────────────────────────────────────
{
  const comments = new Comments();
  const target = data.files[0].path;
  const other = data.files[1]?.path ?? target;
  const saved = comments.draft({ file: target, side: 'additions', start: 3, end: 4 });
  comments.setText(saved, '  needs a name  ');
  comments.save(saved);
  const second = comments.draft({ file: target, side: 'deletions', start: 9, end: 9 });
  comments.setText(second, 'why?');
  comments.save(second);
  const draft = comments.draft({ file: other, side: 'additions', start: 1, end: 1 });

  check('only saved comments are counted', comments.saved().length === 2);
  check('a draft stays a draft', comments.drafts().length === 1 && comments.drafts()[0].id === draft);
  check('an empty comment cannot be saved', comments.save(draft) === false);
  check('badge counts are per file', comments.countsByFile().get(target) === 2);
  check('saving trims the text', comments.get(saved).text === 'needs a name');

  const payload = comments.payload();
  check('the payload names the side the way server.mjs writes it',
    payload[0].side === 'new' && payload[1].side === 'old', JSON.stringify(payload.map((p) => p.side)));
  check('the payload carries the line range',
    payload[0].startLine === 3 && payload[0].endLine === 4);
  check('every comment has an id the review file can name it by',
    payload.every((p) => /^c\d+$/.test(p.id)), payload.map((p) => p.id).join(','));

  // What is written as the review is made: drafts included, empty boxes not.
  comments.setText(draft, 'still typing');
  const state = comments.state();
  check('the saved state keeps drafts, marked as drafts',
    state.length === 3 && state.filter((c) => c.draft === true).length === 1,
    JSON.stringify(state.map((c) => [c.id, c.draft ?? false])));
  check('an empty box is not worth saving',
    comments.draft({ file: target, side: 'additions', start: 40, end: 40 }) &&
      comments.state().length === 3);

  let notified = null;
  comments.onChange((path) => (notified = path));
  comments.remove(second);
  check('removing a comment notifies its file', notified === target && comments.saved().length === 1);
}

// ── Comments an earlier round left open ──────────────────────────────────────
{
  const comments = new Comments({ nextSeq: data.nextSeq ?? 7 });
  const target = data.files[0].path;
  comments.adopt([
    { id: 'c1', file: target, side: 'additions', start: 5, end: 5, text: 'still open',
      round: 1, moved: true, movedFrom: 2 },
    { id: 'c2', file: target, side: 'deletions', start: 9, end: 9, text: 'half typed',
      draft: true, round: 1 },
  ]);
  check('a carried comment comes back saved, a carried draft comes back a draft',
    comments.saved().length === 1 && comments.drafts().length === 1);
  check('a carried comment keeps the id the review file knows it by',
    comments.payload()[0].id === 'c1');
  check('a carried comment keeps the round it was made in',
    comments.payload()[0].round === 1);
  check('a carried comment knows it was relocated',
    comments.saved()[0].carried === true && comments.saved()[0].movedFrom === 2);
  const fresh = comments.draft({ file: target, side: 'additions', start: 1, end: 1 });
  comments.setText(fresh, 'new this round');
  check('a new comment does not reuse an id from an earlier round',
    !['c1', 'c2'].includes(comments.get(fresh).extId), comments.get(fresh).extId);

  // A resolved comment has to survive the whole way in — the payload field, the
  // Session that holds it and the store that adopts it. Miss any one of them and
  // a review whose every comment is addressed reopens reading "0 comments".
  const seen = new Session({ ...data, resolved: [
    { id: 'c3', file: target, side: 'additions', start: 7, end: 7, text: 'was addressed',
      round: 1, carried: true, resolved: true, resolution: 'fixed' },
  ] });
  check('Session carries the addressed comments', seen.resolved.length === 1);
  comments.adopt(seen.resolved);
  check('an adopted comment stays addressed, and off the open list',
    comments.resolved().length === 1 && comments.resolved()[0].resolution === 'fixed'
      && !comments.open().some((c) => c.extId === 'c3'),
    `${comments.resolved().length} resolved, ${comments.open().length} open`);
}

// ── A round handed over, and what the session does with it ───────────────────
// The review outlives a submit: what was sent reads as an earlier round from
// then on, and turns up resolved once the session has dealt with it.
{
  const comments = new Comments({ nextSeq: 1 });
  const target = data.files[0].path;
  const sent = comments.draft({ file: target, side: 'additions', start: 1, end: 1,
    text: 'rename this', saved: true });
  comments.draft({ file: target, side: 'additions', start: 2, end: 2, text: 'and this', saved: true });
  const extId = comments.get(sent).extId;

  comments.markSubmitted(3);
  check('sending a round puts everything that was open behind it',
    comments.open().length === 2 && comments.open().every((c) => c.carried && c.round === 3));
  const later = comments.draft({ file: target, side: 'additions', start: 4, end: 4,
    text: 'thought of afterwards', saved: true });
  check('a comment made after the hand-over stands apart from it',
    !comments.get(later).carried && comments.get(later).round == null);

  const changed = comments.applyResolved([{ id: extId, resolution: 'renamed' }]);
  check('a comment the session resolved leaves the review but stays on screen',
    changed && comments.open().length === 2 && comments.resolved().length === 1 &&
      comments.saved().length === 3);
  check('…and is not sent back or saved as open again',
    !comments.payload().some((c) => c.id === extId) && !comments.state().some((c) => c.id === extId));
  check('…and does not count towards its file',
    comments.countsByFile().get(target) === 2, String(comments.countsByFile().get(target)));
  check('hearing the same resolution twice redraws nothing',
    comments.applyResolved([{ id: extId, resolution: 'renamed' }]) === false);
}

// ── Finding text on the page ─────────────────────────────────────────────────
{
  const sources = [
    { path: 'a.cpp', side: 'additions', text: 'int Foo = 1;\nreturn foo(foo);\nnothing here\n' },
    { path: 'b.cpp', side: 'deletions', text: 'gone: foo\n' },
  ];

  const hits = findMatches(sources, 'foo');
  check('a match names the file, the side and the line',
    hits.length === 3 && hits[0].path === 'a.cpp' && hits[0].side === 'additions' && hits[0].line === 1,
    JSON.stringify(hits.map((h) => `${h.path}:${h.line}`)));
  check('matching ignores case', hits[0].text === 'int Foo = 1;');
  check('a line holding the query twice is still one match',
    hits.filter((h) => h.path === 'a.cpp' && h.line === 2).length === 1);
  check('every source is searched, on the side it was given',
    hits[2].path === 'b.cpp' && hits[2].side === 'deletions');
  check('an empty query matches nothing', findMatches(sources, '   ').length === 0);
  check('a source with no text is skipped',
    findMatches([{ path: 'c', side: 'additions', text: null }], 'foo').length === 0);

  check('the walk enters at the first match going forwards', stepMatch(-1, 1, 3) === 0);
  check('…and at the last one going backwards', stepMatch(-1, -1, 3) === 2);
  check('the walk wraps at both ends',
    stepMatch(2, 1, 3) === 0 && stepMatch(0, -1, 3) === 2);
  check('nothing to walk stays nowhere', stepMatch(-1, 1, 0) === -1);
}

// ── Split geometry ───────────────────────────────────────────────────────────
check('the handle sits at the middle of the pane', splitLeftPx(1200, 0.5) === 600);
check('the handle follows the ratio', splitLeftPx(1200, 0.3) === 360);

console.log(failures === 0 ? '\nall checks passed' : `\n${failures} check(s) failed`);
process.exit(failures === 0 ? 0 : 1);
