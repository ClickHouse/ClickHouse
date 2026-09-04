// The sidebar's second pane: every comment of the review in one list, the open
// ones first and the addressed ones under them. It is the way to find a comment
// once the file-tree badges no longer point at it — badges count open comments,
// so a review the session has answered in full leaves nothing to follow.
//
// State belongs to comments.mjs; this draws it and reports clicks.

import { el } from './dom.mjs';

/// The last line of a file's path, which is what tells two rows apart; the
/// directories above it are the same for most of a review.
function shortPath(path) {
  const cut = path.lastIndexOf('/');
  return cut === -1 ? path : path.slice(cut + 1);
}

function place(c) {
  const lines = c.start === c.end ? `${c.end}` : `${c.start}–${c.end}`;
  return `${shortPath(c.file)}:${lines}`;
}

export class CommentPane
{
  /// `paths` is every file the review can show. A comment can outlive its file —
  /// a file added on the branch and deleted since is no change at all against
  /// the base, so it drops out of the review while what was said about it stays.
  /// There is nowhere to jump to for those, so the pane is where they are read.
  constructor({ el: root, paths, onGoto, onReply, onDismiss })
  {
    this.root = root;
    this.paths = paths;
    this.onGoto = onGoto;
    this.onReply = onReply;
    this.onDismiss = onDismiss;
    this.activeId = null;
    this.expanded = new Set();
  }

  /// A row per comment, the open ones first. There are no headings between the
  /// two groups: the tab already counts them, and each row says which it is by
  /// its colour and by whether it carries an answer. Clicking anywhere but the
  /// buttons goes to the comment; the buttons act on it in place.
  render(comments)
  {
    this.comments = comments;
    const open = comments.open();
    const done = comments.resolved();
    this.root.replaceChildren();

    if (open.length === 0 && done.length === 0)
    {
      this.root.append(el('div', { class: 'empty', text: 'No comments yet. Click the + over a line number to write one.' }));
      return;
    }

    for (const c of open) this.root.append(this.row(c, false));
    for (const c of done) this.root.append(this.row(c, true));
  }

  row(c, addressed)
  {
    const gone = !this.paths.has(c.file);
    const open = gone && this.expanded.has(c.id);
    const tag = c.round == null ? '' : `r${c.round}`;
    const note = gone ? 'file not in this review' : c.stale ? 'line gone' : '';
    const meta = el('div', { class: 'where' }, [
      el('span', { class: 'at', text: place(c) }),
      el('span', { class: 'round', text: [tag, note].filter(Boolean).join(' · ') }),
    ]);
    const row = el('div', {
      class: `crow ${addressed ? 'done' : 'open'}${gone ? ' gone' : ''}` +
        `${open ? ' shown' : ''}${c.id === this.activeId ? ' active' : ''}`,
      title: gone
        ? 'The file this was written on is no longer part of the review — click to read it here'
        : '',
      // With no file to go to, the row is the only place it can be read, so it
      // opens in place instead.
      onClick: gone
        ? () => { this.toggle(c.id); }
        : () => this.onGoto(c),
    }, [meta, el('div', { class: 'txt', text: c.text })]);

    if (addressed)
    {
      row.append(el('div', { class: 'res', text: c.resolution ?? 'addressed by the session' }));
      row.append(el('div', { class: 'acts' }, [
        // A follow-up is a comment of its own on the same lines, so the answer
        // stays on the record and the new question goes to the next round. With
        // the file gone there are no lines left to put one on.
        gone ? null : el('button', {
          title: 'Ask a follow-up on these lines, and dismiss this one',
          text: '↩ reply',
          onClick: (e) => { e.stopPropagation(); this.onReply(c); },
        }),
        el('button', {
          title: 'Take this off the page; the review file keeps it',
          text: '× hide',
          onClick: (e) => { e.stopPropagation(); this.onDismiss(c); },
        }),
      ]));
    }
    return row;
  }

  toggle(id)
  {
    if (this.expanded.has(id)) this.expanded.delete(id);
    else this.expanded.add(id);
    if (this.comments != null) this.render(this.comments);
  }

  setActive(id)
  {
    this.activeId = id;
  }
}
