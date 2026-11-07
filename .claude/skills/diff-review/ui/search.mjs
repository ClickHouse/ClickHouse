// Finding text on the page being read. Pure: the caller says which sources the
// current page is made of, this says where the query occurs in them. Scrolling
// and selecting belong to main.mjs, which owns the viewer.
//
// A match is a line rather than an occurrence, because a line is the smallest
// thing the page can point at: the viewer selects and scrolls to one. A line
// holding the query twice is one match, and the count says "lines" for that
// reason.

/// `sources` is [{ path, side, text }] — the file's own side of the review, as
/// fetched. Matching is case-insensitive and literal: a review is read for names
/// and strings, and a stray `(` in a path should not have to be escaped.
export function findMatches(sources, query) {
  const needle = query.trim().toLowerCase();
  if (needle === '') return [];

  const matches = [];
  for (const { path, side, text } of sources) {
    if (typeof text !== 'string') continue;
    const lines = text.split('\n');
    for (let i = 0; i < lines.length; i++) {
      if (lines[i].toLowerCase().includes(needle))
        matches.push({ path, side, line: i + 1, text: lines[i].trim().slice(0, 200) });
    }
  }
  return matches;
}

/// Where the walk goes from where it is: `delta` steps and wraps around, and a
/// search that has not moved yet enters at the end it is walking towards.
export function stepMatch(at, delta, count) {
  if (count === 0) return -1;
  if (at < 0 || at >= count) return delta > 0 ? 0 : count - 1;
  return (at + delta + count) % count;
}
