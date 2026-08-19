// The comment box the viewer renders under a line. It lives inside the viewer's
// shadow root, where the page's stylesheet does not reach, so every rule here is
// inline. State belongs to comments.mjs; this only draws it and reports clicks.

import { el } from './dom.mjs';

const BOX =
  'margin:6px 12px;padding:10px 12px;border-radius:8px;' +
  'font-family:system-ui,-apple-system,"Segoe UI",Roboto,sans-serif;font-size:13px;line-height:1.45;' +
  'background:light-dark(#f3f7ff,#10161f);border:1px solid light-dark(#c9dcf5,#23334a);' +
  'border-left:3px solid light-dark(#0969da,#2f81f7);';
/// A comment from an earlier round that is still open reads differently: it is
/// something already said and not yet dealt with, not something being written.
const CARRIED =
  'background:light-dark(#fff8e1,#1c1806);border-color:light-dark(#eee2b0,#3a3218);' +
  'border-left-color:light-dark(#bf8700,#d29922);';
/// Dealt with by the session, and shown as such: still readable, out of the way.
const RESOLVED =
  'background:light-dark(#f2f8f4,#0d150f);border-color:light-dark(#cde8d6,#1e3a29);' +
  'border-left-color:light-dark(#1a7f37,#3fb950);opacity:.85;';
const RESOLUTION =
  'margin-top:6px;padding-top:6px;border-top:1px dashed light-dark(#cde8d6,#1e3a29);' +
  'color:light-dark(#1a7f37,#3fb950);white-space:pre-wrap;overflow-wrap:anywhere;';
const HEADER = 'font-weight:600;font-size:12px;color:light-dark(#555,#aaa);';
/// The header is a row so that the fold control can sit at its end, and the rest
/// of the box hangs off it: folded, the header is the whole box.
const HEADER_ROW = 'display:flex;align-items:baseline;gap:8px;';
const HEADER_GAP = 'margin-bottom:6px;';
/// What a folded comment shows of itself, after the header: enough to recognise
/// which one it is without reading it again.
const PREVIEW =
  'flex:1 1 auto;min-width:0;font-weight:400;overflow:hidden;text-overflow:ellipsis;' +
  'white-space:nowrap;opacity:.8;';
const FOLD_BUTTON =
  'flex:0 0 auto;margin-left:auto;font:inherit;font-size:11px;line-height:1;' +
  'padding:2px 6px;border-radius:5px;cursor:pointer;' +
  'background:transparent;color:inherit;border:1px solid transparent;opacity:.6;';
const EDITOR =
  'width:100%;min-height:64px;resize:vertical;font:inherit;padding:6px 8px;' +
  'border-radius:6px;border:1px solid light-dark(#ccc,#444);' +
  'background:light-dark(#fff,#0a0a0a);color:inherit;';
const ROW = 'display:flex;gap:6px;justify-content:flex-end;margin-top:6px;';
const BUTTON =
  'font:inherit;font-size:12px;padding:4px 10px;border-radius:6px;cursor:pointer;' +
  'background:transparent;color:inherit;border:1px solid light-dark(#ccc,#444);';
const PRIMARY =
  'font:inherit;font-size:12px;padding:4px 10px;border-radius:6px;cursor:pointer;' +
  'background:light-dark(#0969da,#2f81f7);color:#fff;border:none;font-weight:600;';

/// Where the comment came from and whether it is still where it was filed —
/// both only apply to a comment an earlier round left open.
function provenance(comment) {
  const tags = [];
  if (comment.carried) tags.push(comment.round == null ? 'earlier round' : `round ${comment.round}`);
  if (comment.stale) tags.push(`line ${comment.movedFrom ?? comment.end} is gone`);
  else if (comment.moved) tags.push(`moved from line ${comment.movedFrom}`);
  return tags;
}

/// One line of the comment, for the header of a folded one.
const oneLine = (text) => text.replace(/\s+/g, ' ').trim();

/// `actions` is { onInput, onSave, onDiscard, onEdit, onDelete, onReply, onDismiss,
/// onToggleFold }, plus `folded`. A box being written is never folded: there is
/// nothing to fold away yet, and hiding a half-typed comment behind a chevron is
/// how it gets lost.
export function commentBox(comment, actions) {
  const lines =
    comment.start === comment.end ? `line ${comment.end}` : `lines ${comment.start}–${comment.end}`;
  const side = comment.side === 'deletions' ? 'old' : 'new';
  const tags = provenance(comment);
  const skin = comment.resolved ? RESOLVED : comment.carried ? CARRIED : '';
  const foldable = comment.saved && actions.onToggleFold != null;
  const folded = foldable && actions.folded === true;

  const header = el('div', { style: HEADER_ROW + HEADER + (folded ? '' : HEADER_GAP) }, [
    el('span', {
      style: 'flex:0 0 auto;',
      text:
        `${comment.resolved ? 'Addressed ✓' : 'Your comment'} · ${lines} (${side})` +
        `${tags.length > 0 ? ' · ' + tags.join(' · ') : ''}`,
    }),
    folded ? el('span', { style: PREVIEW, text: oneLine(comment.text) }) : null,
    foldable
      ? el('button', {
          type: 'button',
          style: FOLD_BUTTON,
          title: folded ? 'Show this comment' : 'Fold this comment away',
          text: folded ? '▸ show' : '▾ fold',
          onClick: (e) => {
            e.stopPropagation();
            actions.onToggleFold();
          },
        })
      : null,
  ]);
  if (foldable) {
    header.style.cursor = 'pointer';
    header.title = folded ? 'Show this comment' : 'Fold this comment away';
    header.addEventListener('click', () => actions.onToggleFold());
  }

  const box = el('div', { style: BOX + skin }, [header]);
  if (folded) return box;

  if (comment.resolved) {
    box.append(
      el('div', { style: 'white-space:pre-wrap;overflow-wrap:anywhere;', text: comment.text }),
      el('div', {
        style: RESOLUTION,
        text: comment.resolution ?? 'addressed by the session',
      }),
      // The same two moves the Comments pane offers, where the answer is being
      // read: a follow-up on these lines, or off the page for good.
      el('div', { style: ROW }, [
        actions.onDismiss == null
          ? null
          : el('button', {
              type: 'button',
              style: BUTTON,
              title: 'Take this off the page; the review file keeps it',
              text: '× hide',
              onClick: actions.onDismiss,
            }),
        actions.onReply == null
          ? null
          : el('button', {
              type: 'button',
              style: BUTTON,
              title: 'Ask a follow-up on these lines, and dismiss this one',
              text: '↩ reply',
              onClick: actions.onReply,
            }),
      ])
    );
  } else if (!comment.saved) {
    const editor = el('textarea', {
      style: EDITOR,
      value: comment.text,
      placeholder: 'Leave a comment… (Ctrl+Enter to save, Esc to discard)',
      onInput: () => actions.onInput(editor.value),
      onKeydown: (e) => {
        if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) actions.onSave();
        if (e.key === 'Escape') actions.onDiscard();
        e.stopPropagation();
      },
    });
    box.append(
      editor,
      el('div', { style: ROW }, [
        el('button', { type: 'button', style: BUTTON, text: 'Cancel', onClick: actions.onDiscard }),
        el('button', {
          type: 'button',
          style: PRIMARY,
          text: 'Add comment',
          onClick: actions.onSave,
        }),
      ])
    );
    setTimeout(() => editor.focus(), 50);
  } else {
    box.append(
      el('div', { style: 'white-space:pre-wrap;overflow-wrap:anywhere;', text: comment.text }),
      el('div', { style: ROW }, [
        el('button', { type: 'button', style: BUTTON, text: 'Delete', onClick: actions.onDelete }),
        el('button', { type: 'button', style: BUTTON, text: 'Edit', onClick: actions.onEdit }),
      ])
    );
  }
  return box;
}

/// Centred against the filename whichever way the slot lays its children out:
/// `align-self` covers a flex row (whose default `stretch`, or a `baseline`,
/// would otherwise hang it off the text), `vertical-align` covers an inline one,
/// and a zero line-height keeps the button's own line box from adding leading.
const HEADER_BUTTON =
  'display:inline-flex;align-items:center;justify-content:center;' +
  'align-self:center;vertical-align:middle;line-height:0;' +
  'width:20px;height:20px;padding:0;margin-left:4px;flex:0 0 auto;' +
  'border:none;border-radius:5px;cursor:pointer;' +
  'background:transparent;color:inherit;opacity:.55;';
/// `display:block` too: an inline SVG sits on the text baseline, which would
/// leave a descender's worth of space under it inside the button.
const HEADER_ICON =
  'display:block;width:13px;height:13px;fill:none;stroke:currentColor;stroke-width:1.5;' +
  'stroke-linecap:round;stroke-linejoin:round;';
const COPY_ICON =
  '<rect x="5.5" y="5.5" width="9" height="9" rx="1.5"/>' +
  '<path d="M10.5 5.5v-3a1 1 0 0 0-1-1h-7a1 1 0 0 0-1 1v7a1 1 0 0 0 1 1h3"/>';
const DONE_ICON = '<path d="m3 8.5 3.5 3.5L13 5"/>';
const FAIL_ICON = '<path d="M4.5 4.5l7 7M11.5 4.5l-7 7"/>';

const icon = (paths, colour) =>
  `<svg viewBox="0 0 16 16" style="${HEADER_ICON}${colour ? `stroke:${colour};` : ''}">${paths}</svg>`;

/// Aligned like the button beside it, for the same reasons.
const HEADER_SUFFIX =
  'display:inline-flex;align-items:center;' +
  'align-self:center;vertical-align:middle;flex:0 0 auto;';
const HEADER_LINES =
  'margin-left:8px;font-size:11px;font-variant-numeric:tabular-nums;' +
  'opacity:.5;white-space:nowrap;';

/// What follows a file's name in its own header: the control that copies a
/// reference into it, right against the name, and then how long the file is.
export function headerFileSuffix(lines, button) {
  return el('span', { style: HEADER_SUFFIX }, [
    button,
    lines == null ? null : el('span', { style: HEADER_LINES, text: `${lines} lines` }),
  ]);
}

/// The control beside a file's name in its own header: the shortest way from
/// reading a line to naming it somewhere else. One fixed square, answering on
/// the icon, so nothing in the header moves when it is used.
export function headerCopyButton(copy) {
  const button = el('button', {
    type: 'button',
    style: HEADER_BUTTON,
    title: 'Copy path:line (y)',
  });
  button.innerHTML = icon(COPY_ICON);
  button.addEventListener('pointerenter', () => { button.style.opacity = '1'; });
  button.addEventListener('pointerleave', () => { button.style.opacity = '.55'; });
  let flash = null;
  button.addEventListener('click', async (e) => {
    e.stopPropagation();
    const ok = await copy();
    button.innerHTML = ok
      ? icon(DONE_ICON, 'light-dark(#1a7f37,#3fb950)')
      : icon(FAIL_ICON, 'light-dark(#a40e26,#f85149)');
    clearTimeout(flash);
    flash = setTimeout(() => { button.innerHTML = icon(COPY_ICON); }, 1200);
  });
  return button;
}
