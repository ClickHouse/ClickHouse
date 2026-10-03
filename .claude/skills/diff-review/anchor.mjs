// Finding a comment's line again in a later round.
//
// A comment is filed against a line number, but between rounds the code moves:
// fixing the first comment shifts every comment below it, and the line one was
// filed on may be gone entirely. Each comment therefore carries `anchor` — the
// text of the line it was filed on — and is relocated to the nearest line that
// still holds that text.

export function splitLines(text) {
  const lines = text.split('\n');
  if (lines.length > 0 && lines[lines.length - 1] === '') lines.pop();
  return lines;
}

/// The comment with its line range brought up to date against `lines` (the side
/// of the file it is filed on), plus a transient flag saying what happened:
/// `moved` when it was relocated, `stale` when its line is gone and the recorded
/// position is all that is left. Neither flag is stored: the next write records
/// the new position and a fresh anchor.
export function reanchor(comment, lines) {
  const at = comment.endLine - 1;
  const last = Math.max(lines.length - 1, 0);
  const clamped = Math.min(Math.max(at, 0), last);
  const fellOff = () => ({
    ...comment,
    startLine: Math.min(comment.startLine, clamped + 1),
    endLine: clamped + 1,
    stale: true,
    movedFrom: comment.endLine,
  });

  // Written before anchors existed, or filed on the very end of the file: the
  // line number is the only thing to go on.
  if (comment.anchor == null) return at === clamped ? { ...comment } : fellOff();
  if (lines[at] === comment.anchor) return { ...comment };

  let best = -1;
  for (let i = 0; i < lines.length; i++) {
    if (lines[i] === comment.anchor && (best < 0 || Math.abs(i - at) < Math.abs(best - at))) best = i;
  }
  if (best < 0) return fellOff();
  const delta = best - at;
  return {
    ...comment,
    startLine: Math.max(1, comment.startLine + delta),
    endLine: best + 1,
    moved: true,
    movedFrom: comment.endLine,
  };
}
