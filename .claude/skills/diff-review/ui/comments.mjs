// The reviewer's comments. State only — the boxes are drawn in annotations.mjs.
//
// Comments live here rather than inside the viewer, keyed by path, so they
// survive switching files, toggling split/unified and rebuilding a file's view.
// A comment is { id, extId, file, side, start, end, text, saved }; `side` is the
// viewer's ('additions' / 'deletions') and is translated on the wire. `id` is
// this page's; `extId` is the one the review file knows the comment by, and is
// what makes a comment the same comment across rounds.
//
// A comment can also be resolved — the session has addressed it and said so in
// the review file. That is the one state the page does not own: it arrives from
// the server while the review is open, and only ever reads back.

export class Comments {
  constructor({ nextSeq = 1 } = {}) {
    this.byId = new Map();
    this.nextId = 1;
    this.seq = nextSeq;
    this.listeners = new Set();
  }

  onChange(fn) {
    this.listeners.add(fn);
  }

  changed(path) {
    for (const fn of this.listeners) fn(path);
  }

  get(id) {
    return this.byId.get(id);
  }

  /// A new comment starts as a draft: it shows an editor and does not count
  /// towards the review until it is saved.
  draft({ file, side, start, end, text = '', saved = false, extId = null, carried = false,
          round = null, moved = false, movedFrom = null, stale = false,
          resolved = false, resolution = null, replyTo = null }) {
    const id = this.nextId++;
    this.byId.set(id, {
      id, extId: extId ?? `c${this.seq++}`, file, side, start, end, text, saved,
      carried, round, moved, movedFrom, stale, resolved, resolution, replyTo,
    });
    this.changed(file);
    return id;
  }

  /// An addressed comment the reviewer has read and is done with. It leaves the
  /// page for good; the review file keeps it, marked dismissed, so the question
  /// and its answer stay on the record and nothing resurfaces them.
  dismiss(id) {
    const c = this.byId.get(id);
    if (c == null) return null;
    this.byId.delete(id);
    this.changed(c.file);
    return c.extId;
  }

  /// Comments the review file holds, seeded as it left them — saved ones saved,
  /// drafts still in their editor, resolved ones resolved, and each marked as
  /// carried only if an earlier round already handed it over. Minting continues
  /// past every id adopted here, so this round cannot hand out one of theirs.
  adopt(carried) {
    for (const c of carried ?? []) {
      this.seq = Math.max(this.seq, Number(/^c(\d+)$/.exec(c.id ?? '')?.[1] ?? 0) + 1);
      this.draft({
        file: c.file, side: c.side, start: c.start, end: c.end, text: c.text,
        saved: c.draft !== true, extId: c.id, carried: c.carried !== false,
        round: c.round, moved: c.moved, movedFrom: c.movedFrom, stale: c.stale,
        resolved: c.resolved === true, resolution: c.resolution ?? null,
        replyTo: c.replyTo ?? null,
      });
    }
  }

  /// What the session has addressed, as the review file records it. Resolved
  /// comments stay on screen — seeing what was done with them is the point —
  /// but they are out of the review: they are neither sent again nor counted.
  applyResolved(resolved) {
    const byExtId = new Map([...this.byId.values()].map((c) => [c.extId, c]));
    const touched = new Set();
    for (const { id, resolution } of resolved ?? []) {
      const c = byExtId.get(id);
      if (c == null || (c.resolved && c.resolution === resolution)) continue;
      c.resolved = true;
      c.resolution = resolution;
      touched.add(c.file);
    }
    for (const file of touched) this.changed(file);
    return touched.size > 0;
  }

  /// A submitted round is a line under everything that was open at the time: it
  /// has been handed over, so it reads from here on as a comment from an earlier
  /// round, and what the reviewer writes next stands apart from it.
  markSubmitted(round) {
    const touched = new Set();
    for (const c of this.byId.values()) {
      if (!c.saved || c.resolved || c.carried) continue;
      c.carried = true;
      c.round ??= round;
      touched.add(c.file);
    }
    for (const file of touched) this.changed(file);
  }

  /// Typing does not notify: the box holding the text is the one being edited,
  /// and re-rendering it would take the cursor with it.
  setText(id, text) {
    const c = this.byId.get(id);
    if (c != null) c.text = text;
  }

  save(id) {
    const c = this.byId.get(id);
    if (c == null || c.text.trim() === '') return false;
    c.text = c.text.trim();
    c.saved = true;
    this.changed(c.file);
    return true;
  }

  reopen(id) {
    const c = this.byId.get(id);
    if (c == null) return;
    c.saved = false;
    this.changed(c.file);
  }

  remove(id) {
    const c = this.byId.get(id);
    if (c == null) return;
    this.byId.delete(id);
    this.changed(c.file);
  }

  forFile(path) {
    return [...this.byId.values()].filter((c) => c.file === path).sort((a, b) => a.end - b.end);
  }

  drafts() {
    return [...this.byId.values()].filter((c) => !c.saved);
  }

  saved() {
    return [...this.byId.values()].filter((c) => c.saved).sort(byPlace);
  }

  /// The review as it stands: everything said and not yet addressed.
  open() {
    return this.saved().filter((c) => !c.resolved);
  }

  resolved() {
    return this.saved().filter((c) => c.resolved);
  }

  countsByFile() {
    const counts = new Map();
    for (const c of this.byId.values()) {
      if (c.saved && !c.resolved) counts.set(c.file, (counts.get(c.file) ?? 0) + 1);
    }
    return counts;
  }

  /// The shape server.mjs writes out: line numbers on the side they belong to.
  wire(c, extra) {
    return {
      id: c.extId,
      file: c.file,
      side: c.side === 'deletions' ? 'old' : 'new',
      startLine: c.start,
      endLine: c.end,
      comment: c.text,
      ...(c.round != null ? { round: c.round } : {}),
      ...(c.replyTo != null ? { replyTo: c.replyTo } : {}),
      ...extra,
    };
  }

  /// What the review is: comments the reviewer saved and nothing has addressed.
  payload() {
    return this.open().map((c) => this.wire(c));
  }

  /// What is worth keeping if the browser goes away, half-typed drafts included.
  /// Resolved comments are left out: the file already holds them, resolution and
  /// all, and sending them back would only ask the server to ignore them again.
  state() {
    return [...this.byId.values()]
      .filter((c) => !c.resolved && c.text.trim() !== '')
      .sort(byPlace)
      .map((c) => this.wire(c, c.saved ? undefined : { draft: true }));
  }
}

const byPlace = (a, b) => (a.file === b.file ? a.end - b.end : a.file < b.file ? -1 : 1);
