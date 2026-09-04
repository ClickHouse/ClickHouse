// Parsed diffs, cached per path.
//
// Every diff is built from both full sides of the file (`parseDiffFromFile`), so
// it carries the whole file and the viewer can expand any folded region out to
// it. Parsing is cheap — 27 ms for a megabyte — and it is the rendering that
// costs; see viewer.mjs.

import { parseDiffFromFile } from '/vendor/pierre-diffs.mjs';

export class Diffs {
  constructor(session) {
    this.session = session;
    this.byPath = new Map();
  }

  parsed(path) {
    return this.byPath.get(path);
  }

  async diff(path) {
    const done = this.byPath.get(path);
    if (done != null) return done;
    const sides = await this.session.sides(path);
    const file = this.session.byPath.get(path);
    const diff = parseDiffFromFile(
      { name: file.oldPath ?? path, contents: sides.old },
      { name: path, contents: sides.new }
    );
    this.byPath.set(path, diff);
    return diff;
  }

  /// New-side length, once the file has been fetched — what decides whether a
  /// file is cheap enough to render ahead of being asked for.
  lineCount(path) {
    return this.byPath.get(path)?.additionLines?.length;
  }

  /// How long the file is, to say so in its header: the side it still has, which
  /// for a deleted file is the old one. `null` until it has been fetched.
  fileLines(path) {
    const diff = this.byPath.get(path);
    if (diff == null) return null;
    const deleted = this.session.byPath.get(path)?.status === 'D';
    return (deleted ? diff.deletionLines : diff.additionLines)?.length ?? null;
  }
}
