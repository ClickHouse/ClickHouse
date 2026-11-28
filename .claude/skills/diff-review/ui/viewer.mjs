// Everything that knows about @pierre/diffs `CodeView`.
//
// One viewer per file, each in its own scroll pane, shown and hidden instead of
// being torn down. That is the whole performance design: handing a file to a
// viewer tokenizes it at roughly 1.2 ms a line — 3.2 s for a 2 237-line C++ file
// — and the highlight cache does not survive an item being replaced, so a design
// that swaps items on one viewer pays that on every visit. Hidden panes keep it:
// coming back to a file costs a frame.
//
// The cost that remains is the first visit, and it is bounded from three sides:
// `TOKENIZE_MAX_LINES` (past it a file renders without syntax colours rather
// than blocking for seconds), a busy overlay the shell raises before the
// blocking call, and rendering neighbours ahead of time while the browser idles.

import { CodeView } from '/vendor/pierre-diffs.mjs';
import { el } from './dom.mjs';

const THEME = { dark: 'pierre-dark', light: 'pierre-light' };

/// The viewer's core CSS pins the split to `1fr 1fr` inside its shadow root. This
/// goes in after it (`unsafeCSS` appends a style node), and reads a custom property:
/// those inherit through the shadow boundary, so a drag only has to set the property
/// on the host and no re-render is involved.
const SPLIT_CSS =
  '[data-diff-type="split"][data-overflow="scroll"] {' +
  ' grid-template-columns: var(--split-left, 50%) 1fr !important; }';

/// A cap on colours, not on content: above it the file still renders whole, just
/// without syntax highlighting. 3 000 lines is ~3.5 s of tokenizing, which is as
/// long as a first visit should ever block.
const TOKENIZE_MAX_LINES = 3000;
/// The all-files view puts every file on one page; tokenizing them all would be
/// minutes, so it renders plain (0 = no file is small enough for colours).
const ALL_FILES_TOKENIZE = 0;
/// How much rendered output to keep alive, counted in lines rather than files:
/// a review of forty small files should stay entirely cached, while a handful of
/// huge ones should not pin their highlighted output for the whole session.
const MAX_RETAINED_LINES = 20_000;
/// …but always keep the last few views whatever their size, so stepping back and
/// forth between two big files does not re-render both every time.
const MIN_VIEWS = 3;
/// A neighbour is only rendered ahead of time if it is this cheap — about half a
/// second of tokenizing. Anything longer would be a visible stall in the middle
/// of reading, for a file that may never be opened.
const PRERENDER_MAX_LINES = 500;

export const ALL_FILES = Symbol('all files');

export class ReviewViewer {
  constructor({ root, session, diffs, annotationsFor, renderAnnotation, renderHeaderControl,
                onDraft, onSelectLine }) {
    this.root = root;
    this.session = session;
    this.diffs = diffs;
    this.annotationsFor = annotationsFor;
    this.renderAnnotation = renderAnnotation;
    this.renderHeaderControl = renderHeaderControl;
    this.onDraft = onDraft;
    this.onSelectLine = onSelectLine;
    this.diffStyle = 'split';
    this.expandUnchanged = false;
    this.plainBackground = false;
    /// Which of the two themes the viewer paints with: `system` resolves it from
    /// the OS preference itself, which is why forcing one has to be told to it
    /// rather than left to the page's `color-scheme`.
    this.themeType = 'system';
    this.views = new Map(); // key (path | ALL_FILES) -> { pane, viewer, paths }
    this.order = []; // most recently shown first
    this.versions = new Map();
    this.active = null;
  }

  /// True when `display` would not have to tokenize anything, so the shell knows
  /// whether the switch needs a spinner.
  isRendered(key) {
    return this.views.has(key);
  }

  /// Whether there are two columns to drag the boundary between. A file with
  /// only one side is laid out in a single column whatever the diff style —
  /// mirrors what CodeView itself does — and a handle over it resizes nothing.
  isSplitView(key) {
    if (this.diffStyle !== 'split') return false;
    return this.pathsIn(key).some((path) => {
      const diff = this.diffs.parsed(path);
      return diff != null && diff.type !== 'new' && diff.type !== 'deleted';
    });
  }

  activePane() {
    return this.views.get(this.active)?.pane ?? null;
  }

  pathsIn(key) {
    return key === ALL_FILES ? this.session.files.map((f) => f.path) : [key];
  }

  /// Fetches and parses what the view will need. Split from `display` so that a
  /// caller can drop a switch the reviewer has already moved on from: two loads
  /// can be in flight at once, but only one of them may reach the screen.
  async prepare(key) {
    await Promise.all(this.pathsIn(key).map((path) => this.diffs.diff(path)));
  }

  /// Synchronous, and the expensive half: a view that does not exist yet is
  /// rendered here.
  display(key) {
    const view = this.views.get(key) ?? this.create(key);
    if (this.active != null && this.active !== key) {
      const previous = this.views.get(this.active);
      if (previous != null) previous.pane.hidden = true;
    }
    view.pane.hidden = false;
    this.active = key;
    this.touch(key);
  }

  /// Renders a file into a hidden pane so that showing it later is instant.
  /// Skipped unless the file is already fetched and small enough to be quick.
  prerender(path) {
    if (path == null || this.views.has(path)) return;
    const lines = this.diffs.lineCount(path);
    if (lines == null || lines > PRERENDER_MAX_LINES) return;
    this.create(path);
    this.touch(path);
  }

  create(key) {
    const pane = el('div', { class: 'filepane', hidden: true });
    this.root.append(pane);
    const paths = this.pathsIn(key);
    const viewer = new CodeView({
      theme: THEME,
      themeType: this.themeType,
      unsafeCSS: SPLIT_CSS,
      tokenizeMaxLength: key === ALL_FILES ? ALL_FILES_TOKENIZE : TOKENIZE_MAX_LINES,
      stickyHeaders: true,
      diffStyle: this.diffStyle,
      expandUnchanged: this.expandUnchanged,
      disableBackground: this.plainBackground,
      layout: { paddingTop: 12, paddingBottom: 160, gap: 10 },
      lineHoverHighlight: 'both',
      enableLineSelection: true,
      enableGutterUtility: true,
      onGutterUtilityClick: (range, ctx) => {
        const path = typeof ctx === 'string' ? ctx : ctx?.item?.id;
        if (typeof path === 'string' && range != null) this.onDraft(path, range);
      },
      // Which lines are selected, so they can be named outside the viewer. On the
      // all-files page the selection carries the file it is in, which is the
      // only place that is not already known. A selection dragged upwards
      // arrives with its ends the other way round, so they are ordered here.
      onSelectedLinesChange: (selection) => {
        const path = selection?.id;
        const { start, end, side } = selection?.range ?? {};
        if (typeof path !== 'string' || !Number.isInteger(start)) {
          this.onSelectLine?.(null); // nothing is selected any more
          return;
        }
        const last = Number.isInteger(end) ? end : start;
        this.onSelectLine?.({
          path,
          side,
          start: Math.min(start, last),
          end: Math.max(start, last),
        });
      },
      renderAnnotation: (annotation) => this.renderAnnotation(annotation),
      // Immediately after the displayed filename, which is what this slot is
      // for. The slot is handed the parsed diff rather than the path, so the
      // file is identified by matching that object against the cache it came
      // from — no guessing at its field names.
      renderHeaderFilenameSuffix: (fileDiff) => {
        const path = paths.find((p) => this.diffs.parsed(p) === fileDiff);
        return path == null ? undefined : this.renderHeaderControl?.(path);
      },
    });
    viewer.setup(pane);
    const view = { pane, viewer, paths };
    this.views.set(key, view);
    viewer.setItems(view.paths.map((path) => this.item(path)));
    return view;
  }

  item(path) {
    return {
      id: path,
      type: 'diff',
      fileDiff: this.diffs.parsed(path),
      annotations: this.annotationsFor(path),
      version: this.versions.get(path) ?? 0,
    };
  }

  linesIn(key) {
    return this.pathsIn(key).reduce((n, path) => n + (this.diffs.lineCount(path) ?? 0), 0);
  }

  /// Most recently shown first; whatever falls past the budget is torn down.
  touch(key) {
    this.order = [key, ...this.order.filter((k) => k !== key)];
    const kept = [];
    let lines = 0;
    for (const candidate of this.order) {
      lines += this.linesIn(candidate);
      if (kept.length < MIN_VIEWS || lines <= MAX_RETAINED_LINES || candidate === this.active) {
        kept.push(candidate);
        continue;
      }
      const view = this.views.get(candidate);
      view?.viewer.cleanUp();
      view?.pane.remove();
      this.views.delete(candidate);
    }
    this.order = kept;
  }

  /// Re-renders one file's comment boxes wherever it is mounted.
  refresh(path) {
    const version = (this.versions.get(path) ?? 0) + 1;
    this.versions.set(path, version);
    for (const view of this.views.values()) {
      if (!view.paths.includes(path)) continue;
      const item = view.viewer.getItem(path);
      if (item == null) continue;
      view.viewer.updateItem({ ...item, annotations: this.annotationsFor(path), version });
    }
  }

  /// Split and unified are a viewer option, and setting options re-uses the
  /// highlighted output — no file is tokenized again.
  setDiffStyle(style) {
    this.diffStyle = style;
    this.setOption({ diffStyle: style });
  }

  /// Whole file rather than hunks and their context: the unchanged regions are
  /// laid out instead of folded, and their expand controls go away with them.
  /// Costs nothing to tokenize — the file was already handed over whole — so
  /// this is only about how much of it is put on screen.
  setExpandUnchanged(on) {
    this.expandUnchanged = on;
    this.setOption({ expandUnchanged: on });
  }

  /// Drops the tint behind added and deleted lines, leaving the markers in the
  /// gutter and the syntax colours to say what changed. `CodeView` calls it
  /// `disableBackground`, and it is a per-view option like the two above.
  setPlainBackground(on) {
    this.plainBackground = on;
    this.setOption({ disableBackground: on });
  }

  /// `'system' | 'light' | 'dark'`. The syntax colours are the viewer's own, and
  /// it picks between its two themes from the OS preference unless told which —
  /// so the page's `color-scheme` alone would leave the diff behind.
  setThemeType(type) {
    this.themeType = type;
    this.setOption({ themeType: type });
  }

  setOption(option) {
    for (const view of this.views.values()) {
      view.viewer.setOptions({ ...view.viewer.options, ...option });
    }
  }

  instanceFor(key, path) {
    return this.views.get(key)?.viewer.getRenderedItems().find((r) => r.id === path)?.instance;
  }

  /// Makes a line reachable before jumping to it: unchanged regions are folded,
  /// and a comment can sit inside one. Deleted lines are always part of a hunk,
  /// so only the new side can be folded away.
  reveal(key, path, lineNumber, side) {
    if (side === 'deletions') return;
    const diff = this.diffs.parsed(path);
    const instance = this.instanceFor(key, path);
    if (diff == null || instance?.expandHunk == null) return;
    const hunks = diff.hunks ?? [];
    const at = lineNumber - 1;
    for (let i = 0; i < hunks.length; i++) {
      const hunk = hunks[i];
      if (at < hunk.additionLineIndex) {
        // The region before hunk `i` is keyed by `i`; expanding both ends by its
        // size renders all of it.
        instance.expandHunk(i, 'both', hunk.collapsedBefore);
        return;
      }
      if (at < hunk.additionLineIndex + hunk.additionCount) return; // inside the hunk
    }
    // What follows the last hunk is keyed one past it, and only expands downwards.
    instance.expandHunk(hunks.length, 'up', diff.additionLines.length);
  }

  /// Unfolds the line if it is hidden, then scrolls to it. Going to a comment
  /// also selects the line, so the one the reviewer was sent to is marked when
  /// they arrive; stepping through hunks only moves the viewport.
  goToLine(key, path, lineNumber, side, { select = false } = {}) {
    const viewer = this.views.get(key)?.viewer;
    if (viewer == null) return;
    this.reveal(key, path, lineNumber, side);
    if (select) {
      viewer.setSelectedLines({ id: path, range: { start: lineNumber, end: lineNumber, side } });
      // A programmatic selection does not always come back through the viewer's
      // own callback, and being sent to a line is as good a way of picking it as
      // clicking it.
      this.onSelectLine?.({ path, side, start: lineNumber, end: lineNumber });
    }
    viewer.scrollTo({ type: 'line', id: path, lineNumber, side, align: 'center' });
  }

  scrollToTop(key) {
    const pane = this.views.get(key)?.pane;
    if (pane != null) pane.scrollTop = 0;
  }

  /// Where each hunk starts, read off the diff being displayed and on the side
  /// that has lines there — a hunk that only deletes has nothing on the new side.
  hunkStarts(path) {
    const diff = this.diffs.parsed(path);
    return (diff?.hunks ?? []).map((hunk) =>
      hunk.additionCount > 0
        ? { lineNumber: hunk.additionStart, side: 'additions' }
        : { lineNumber: hunk.deletionStart, side: 'deletions' }
    );
  }
}
