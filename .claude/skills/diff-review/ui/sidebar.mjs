// The file tree and its path filter. Rows are built once from tree.mjs's model;
// filtering, collapsing and comment badges only touch what already exists.

import { el } from './dom.mjs';
import { buildRows, visibility, visiblePaths } from './tree.mjs';

/// One chevron, pointing right; CSS turns it down when the directory is open.
/// A drawn glyph rather than a text one: `▸` renders at the font's own weight
/// and reads as a speck next to 13px labels.
const CHEVRON = '<svg viewBox="0 0 16 16" aria-hidden="true"><path d="m6 3.5 5 4.5-5 4.5"/></svg>';

export class Sidebar {
  constructor({ treeEl, filterEl, foldAllEl, files, onSelect }) {
    this.treeEl = treeEl;
    this.filterEl = filterEl;
    this.foldAllEl = foldAllEl;
    this.onSelect = onSelect;
    this.rows = buildRows(files);
    this.dirKeys = this.rows.filter((row) => row.kind === 'dir').map((row) => row.dirKey);
    this.collapsed = new Set();
    this.badges = new Map();
    this.chevrons = new Map();
    this.elements = this.rows.map((row) => this.rowElement(row));
    this.treeEl.append(...this.elements);
    this.empty = el('div', { class: 'empty', text: 'No file matches the filter.', hidden: true });
    this.treeEl.append(this.empty);
    this.filterEl.addEventListener('input', () => this.refresh());
    this.foldAllEl.addEventListener('click', () => this.toggleFoldAll());
    this.refresh();
    this.updateFoldAll();
  }

  rowElement(row) {
    const indent = `padding-left:${8 + row.depth * 12}px`;
    if (row.kind === 'all') {
      return el('button', { type: 'button', class: 'trow all', onClick: () => this.onSelect(null) }, [
        el('span', { class: 'name', text: row.label }),
        el('span', { class: 'num', text: String(row.count) }),
      ]);
    }
    if (row.kind === 'dir') {
      const chevron = el('span', { class: 'chev' });
      chevron.innerHTML = CHEVRON;
      this.chevrons.set(row.dirKey, chevron);
      return el(
        'button',
        {
          type: 'button',
          class: 'trow dir',
          style: indent,
          onClick: () => this.toggleDir(row.dirKey),
        },
        [chevron, el('span', { class: 'name', text: row.label }), el('span', { class: 'num', text: String(row.count) })]
      );
    }
    const badge = el('span', { class: 'badge' });
    this.badges.set(row.path, badge);
    return el(
      'button',
      {
        type: 'button',
        class: 'trow file',
        style: indent,
        title: row.path,
        onClick: () => this.onSelect(row.path),
      },
      [
        el('span', { class: `st ${row.file.status}`, text: row.file.status }),
        el('span', { class: 'name', text: row.label }),
        counts(row.file),
        badge,
      ]
    );
  }

  refresh() {
    const visible = visibility(this.rows, {
      query: this.filterEl.value,
      collapsed: this.collapsed,
    });
    visible.forEach((shown, i) => (this.elements[i].hidden = !shown));
    this.visible = visible;
    this.empty.hidden = visiblePaths(this.rows, visible).length > 0;
  }

  toggleDir(dirKey) {
    if (this.collapsed.has(dirKey)) this.collapsed.delete(dirKey);
    else this.collapsed.add(dirKey);
    const chevron = this.chevrons.get(dirKey);
    if (chevron != null) chevron.classList.toggle('closed', this.collapsed.has(dirKey));
    this.refresh();
    this.updateFoldAll();
  }

  /// Every folder folded, or every one open. Which way a click goes is read off
  /// the tree rather than remembered: while any folder is open there is something
  /// to fold, and only once they are all folded does the control open them again.
  toggleFoldAll() {
    const fold = !this.allFolded();
    this.collapsed = fold ? new Set(this.dirKeys) : new Set();
    for (const chevron of this.chevrons.values()) chevron.classList.toggle('closed', fold);
    this.refresh();
    this.updateFoldAll();
  }

  allFolded() {
    return this.dirKeys.every((dirKey) => this.collapsed.has(dirKey));
  }

  updateFoldAll() {
    // A flat review has nothing to fold, and a control that cannot act says so.
    const folded = this.allFolded();
    this.foldAllEl.disabled = this.dirKeys.length === 0;
    this.foldAllEl.classList.toggle('folded', folded);
    this.foldAllEl.title = folded ? 'Unfold every folder' : 'Fold every folder';
  }

  paths() {
    return visiblePaths(this.rows, this.visible);
  }

  setActive(path) {
    this.rows.forEach((row, i) => {
      const active = row.kind === 'file' ? row.path === path : row.kind === 'all' && path == null;
      this.elements[i].classList.toggle('active', active);
      if (active && row.kind === 'file') this.elements[i].scrollIntoView({ block: 'nearest' });
    });
  }

  setBadges(counts) {
    for (const [path, badge] of this.badges) {
      const n = counts.get(path) ?? 0;
      badge.textContent = n === 0 ? '' : String(n);
    }
  }
}

function counts(file) {
  const node = el('span', { class: 'num' });
  if (file.added > 0) node.append(el('span', { class: 'a', text: `+${file.added}` }));
  if (file.removed > 0) {
    if (node.childNodes.length > 0) node.append(' ');
    node.append(el('span', { class: 'd', text: `−${file.removed}` }));
  }
  return node;
}
