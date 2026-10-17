// The review's data: what changed, and the two sides of each file. No DOM here.
//
// Both sides are fetched per file, on demand, because that is what lets the
// viewer build a diff the reviewer can expand out to the whole file. Nothing is
// fetched up front: a 70-file review would move megabytes before showing a line.

export class Session {
  constructor(payload) {
    this.repo = payload.repo;
    this.base = payload.base;
    this.range = payload.range; // what is being reviewed, for the header
    this.files = payload.files;
    this.skipped = payload.skipped;
    this.out = payload.out; // the review file, which is where comments live
    this.round = payload.round ?? 1; // the round now open for comments
    this.submitted = payload.submitted ?? 0; // the last round handed to the session
    this.nextSeq = payload.nextSeq ?? 1;
    this.carried = payload.carried ?? []; // still open from an earlier round
    this.resolved = payload.resolved ?? []; // …and the ones the session addressed
    this.elsewhere = payload.elsewhere ?? []; // …on files this review does not show
    this.pr = payload.pr ?? null; // the pull request this branch is open as, if any
    this.ci = payload.ci ?? null; // …and the state of its checks, if they are known
    this.byPath = new Map(payload.files.map((f) => [f.path, f]));
    this.sidesByPath = new Map();
    this.inFlight = new Map();
  }

  static async load() {
    const res = await fetch('/data');
    if (!res.ok) throw new Error(`/data failed: ${res.status}`);
    return new Session(await res.json());
  }

  get name() {
    return this.repo.split('/').pop();
  }

  has(path) {
    return this.byPath.has(path);
  }

  loaded(path) {
    return this.sidesByPath.has(path);
  }

  /// Concurrent callers for the same path share one request; the result is kept
  /// for the rest of the session, so revisiting a file never hits the network.
  sides(path) {
    const done = this.sidesByPath.get(path);
    if (done != null) return Promise.resolve(done);
    const running = this.inFlight.get(path);
    if (running != null) return running;
    const request = (async () => {
      const res = await fetch(`/file?path=${encodeURIComponent(path)}`);
      if (!res.ok) {
        const body = await res.json().catch(() => ({}));
        throw new Error(body.error ?? `HTTP ${res.status}`);
      }
      const sides = await res.json();
      this.sidesByPath.set(path, sides);
      this.inFlight.delete(path);
      return sides;
    })();
    this.inFlight.set(path, request);
    return request;
  }

  /// Best-effort warm-up of the cache; failures surface when the file is opened.
  prefetch(path) {
    if (path == null || this.loaded(path) || !this.has(path)) return;
    this.sides(path).catch(() => {});
  }

  neighbours(path) {
    const at = this.files.findIndex((f) => f.path === path);
    if (at < 0) return [];
    return [this.files[at + 1]?.path, this.files[at - 1]?.path].filter((p) => p != null);
  }
}
