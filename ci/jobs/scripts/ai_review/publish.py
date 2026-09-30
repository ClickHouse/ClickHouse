"""
Validate and publish what the review agent wrote.

The agent does not post anything. It writes three files into the output directory:

  summary.md           the top-level review summary
  comments.json        new inline findings:
                       [{"path", "line", "side", "start_line"?, "start_side"?,
                         "severity": "blocker"|"major"|"nit", "body_file"}]
  thread_actions.json  actions on existing threads:
                       [{"action": "reply", "thread_id", "body_file"},
                        {"action": "resolve"|"unresolve", "thread_id"}]

The job posts them once, after the agent run succeeded, so a retried attempt
can never post a review twice. Before posting it enforces what used to be only
prompt rules:

  * An inline comment must target a line GitHub can attach it to (a line of the
    PR diff on the given side, and a multi-line range within one hunk).
    GitHub rejects the whole review when a single comment misses, so an
    unattachable finding is moved into the summary instead of losing them all.
  * Nits are never posted inline; they stay in the summary.
  * A new comment that repeats an open thread of ours (same line, or the same
    file and mostly the same words) is dropped as a duplicate.
  * At most MAX_INLINE_COMMENTS are posted per run, Blockers first; the rest
    stay in the summary. A review that posts a dozen comments at once is
    read as noise, and the summary already lists every finding.
  * Only threads the review created may be resolved; a thread is re-opened only
    when the review resolved it itself, or together with a reply in the same
    run. At most one reply per thread per run.
"""

import json
import os
import re

from ci.jobs.scripts.ai_review.context import thread_is_ours, is_bot

_HUNK_RE = re.compile(r"^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@")

MAX_INLINE_COMMENTS = 6

# Two comments on the same file whose word sets overlap this much (Jaccard)
# say the same thing.
_DUPLICATE_SIMILARITY = 0.5
_WORD_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_:]{2,}")


def _words(text):
    return {w.lower() for w in _WORD_RE.findall(text or "")}


def _similar(a, b):
    wa, wb = _words(a), _words(b)
    if not wa or not wb:
        return False
    return len(wa & wb) / len(wa | wb) >= _DUPLICATE_SIMILARITY


def commentable_lines(patch):
    """Lines of one file's patch that accept a review comment, per side, with
    the hunk each belongs to: {"RIGHT": {line: hunk}, "LEFT": {line: hunk}}."""
    result = {"RIGHT": {}, "LEFT": {}}
    old = new = 0
    hunk = -1
    for raw in (patch or "").split("\n"):
        m = _HUNK_RE.match(raw)
        if m:
            hunk += 1
            old, new = int(m.group(1)), int(m.group(3))
            continue
        if hunk < 0 or raw.startswith("\\"):
            continue
        if raw.startswith("+"):
            result["RIGHT"][new] = hunk
            new += 1
        elif raw.startswith("-"):
            result["LEFT"][old] = hunk
            old += 1
        else:
            result["RIGHT"][new] = hunk
            result["LEFT"][old] = hunk
            old += 1
            new += 1
    return result


def _load_json_list(path):
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError(f"{path} must contain a JSON array")
    return data


def _read_body(entry, base_dir):
    body_file = entry.get("body_file") or ""
    if body_file and not os.path.isabs(body_file) and not os.path.exists(body_file):
        body_file = os.path.join(base_dir, body_file)
    if not body_file or not os.path.exists(body_file):
        return "", body_file
    with open(body_file, "r", encoding="utf-8") as f:
        return f.read().strip(), body_file


def validate_comments(entries, files, threads, base_dir):
    """Split the agent's inline comments into (postable, moved) where `moved`
    are (entry, body, reason) to be listed in the summary instead."""
    lines_by_path = {f["filename"]: commentable_lines(f.get("patch")) for f in files}
    open_ours = set()
    open_texts = {}
    for t in threads or []:
        if thread_is_ours(t) and not t.get("isResolved"):
            if t.get("line"):
                open_ours.add((t.get("path"), int(t["line"])))
            first = ((t.get("comments") or {}).get("nodes") or [{}])[0]
            open_texts.setdefault(t.get("path"), []).append(first.get("body") or "")

    postable, moved = [], []
    seen = set()
    for e in entries:
        body, body_file = _read_body(e, base_dir)
        path = e.get("path") or ""
        side = (e.get("side") or "RIGHT").upper()
        try:
            line = int(e.get("line"))
            start = int(e["start_line"]) if e.get("start_line") is not None else None
        except (TypeError, ValueError):
            moved.append((e, body, "no valid line number"))
            continue
        if not body:
            print(f"WARNING: dropping inline comment on {path}:{line}: empty or missing body file [{body_file}]")
            continue
        if (e.get("severity") or "").lower() == "nit":
            moved.append((e, body, "nits are listed in the summary only"))
            continue
        lines = lines_by_path.get(path)
        if lines is None:
            moved.append((e, body, "file is not part of the PR diff"))
            continue
        side_lines = lines.get(side, {})
        if line not in side_lines:
            moved.append((e, body, f"line {line} ({side}) is not in the PR diff"))
            continue
        if start is not None:
            start_side = (e.get("start_side") or side).upper()
            if start >= line or start_side != side or side_lines.get(start) != side_lines[line]:
                start = None  # keep the comment, anchored on its last line only
        if (path, line) in open_ours or (path, line, side) in seen or any(
                _similar(body, other) for other in open_texts.get(path, [])):
            print(f"Skipping duplicate inline comment on {path}:{line}")
            continue
        seen.add((path, line, side))
        open_texts.setdefault(path, []).append(body)
        comment = {"path": path, "line": line, "side": side, "body_file": body_file,
                   "_blocker": (e.get("severity") or "").lower() == "blocker"}
        if start is not None:
            comment["start_line"] = start
            comment["start_side"] = side
        postable.append(comment)
    # Blockers first, then in the agent's order; the overflow stays in the summary.
    postable.sort(key=lambda c: not c["_blocker"])
    for c in postable[MAX_INLINE_COMMENTS:]:
        body, _ = _read_body(c, base_dir)
        moved.append((c, body, f"more than {MAX_INLINE_COMMENTS} inline comments in one review"))
    postable = postable[:MAX_INLINE_COMMENTS]
    for c in postable:
        del c["_blocker"]
    return postable, moved


def validate_thread_actions(entries, threads, base_dir):
    """Return the thread actions allowed by policy, as
    [(action, thread, body_file_or_None)], replies first."""
    by_id = {t.get("id"): t for t in threads or []}
    replies, state_changes = [], []
    replied = set()
    for e in entries:
        action = (e.get("action") or "").lower()
        thread = by_id.get(e.get("thread_id"))
        if thread is None:
            print(f"WARNING: thread action on unknown thread [{e.get('thread_id')}] ignored")
            continue
        tid = thread["id"]
        if action == "reply":
            body, body_file = _read_body(e, base_dir)
            if not body or tid in replied:
                continue
            replied.add(tid)
            replies.append(("reply", thread, body_file))
        elif action in ("resolve", "unresolve"):
            state_changes.append((action, thread, e))
        else:
            print(f"WARNING: unknown thread action [{action}] ignored")

    allowed = []
    done = set()
    for action, thread, _ in state_changes:
        tid = thread["id"]
        if tid in done:
            continue
        if not thread_is_ours(thread):
            print(f"Refusing to {action} thread {tid}: it was not created by the review")
            continue
        if action == "resolve" and thread.get("isResolved"):
            continue
        if action == "unresolve":
            if not thread.get("isResolved"):
                continue
            resolved_by = (thread.get("resolvedBy") or {}).get("login")
            if not (is_bot(resolved_by) or tid in replied):
                print(f"Refusing to re-open thread {tid}: resolved by {resolved_by} and no reply explains why")
                continue
        done.add(tid)
        allowed.append((action, thread, None))
    return replies + allowed


def moved_findings_markdown(moved):
    """The inline comments that could not be posted, in a collapsed block. The
    summary already lists every finding; this keeps their full text visible."""
    if not moved:
        return ""
    out = ["", "<details><summary>Inline comments that could not be attached to the diff "
           f"({len(moved)})</summary>", ""]
    for e, body, reason in moved:
        where = f"`{e.get('path')}:{e.get('line')}`" if e.get("path") else "(no location)"
        out.append(f"**{where}** ({reason})")
        out.append("")
        out.append(body)
        out.append("")
    out.append("</details>")
    return "\n".join(out) + "\n"


def failed_actions_markdown(failed, repo, pr_number, base_dir):
    """Thread actions GitHub did not accept, so the posted summary says what
    was meant to happen instead of silently dropping it."""
    if not failed:
        return ""
    out = ["", f"<details><summary>Thread actions that could not be applied ({len(failed)})</summary>", ""]
    for action, thread, body_file in failed:
        first = ((thread.get("comments") or {}).get("nodes") or [{}])[0]
        link = (f"https://github.com/{repo}/pull/{pr_number}#discussion_r{first['databaseId']}"
                if first.get("databaseId") else thread.get("id"))
        out.append(f"- {action} on {link}")
        if action == "reply" and body_file:
            body, _ = _read_body({"body_file": body_file}, base_dir)
            out += ["", "  " + body.replace("\n", "\n  "), ""]
    out.append("</details>")
    return "\n".join(out) + "\n"


# `[text](/abs/checkout/path/src/X.cpp:12)` or `(src/X.cpp:12)`-style links the
# agent writes to files in its checkout. Rewritten to GitHub at the reviewed commit.
_LOCAL_LINK_RE = re.compile(r"\]\((?:/[^)\s]*?/)?((?:src|base|programs|tests|ci|utils|docs|cmake|contrib|\.claude)/[^):\s#]+)(?::(\d+)(?:-\d+)?)?\)")


def local_links_to_github(text, repo, sha):
    def repl(m):
        anchor = f"#L{m.group(2)}" if m.group(2) else ""
        return f"](https://github.com/{repo}/blob/{sha}/{m.group(1)}{anchor})"
    return _LOCAL_LINK_RE.sub(repl, text or "")


def publish(gh, repo, pr_number, head_sha, files, threads, output_dir, summary_text):
    """Post the inline review and the thread actions. `gh` is the praktika GH
    class (injected for tests). Returns the summary text to post, with the
    comments that could not be attached inline appended."""
    comments, moved = validate_comments(
        _load_json_list(os.path.join(output_dir, "comments.json")), files, threads, output_dir)
    actions = validate_thread_actions(
        _load_json_list(os.path.join(output_dir, "thread_actions.json")), threads, output_dir)

    if comments:
        print(f"Posting a review with {len(comments)} inline comment(s)")
        if not gh.post_pr_review(commit_id=head_sha, comments=comments, pr=pr_number, repo=repo):
            print("WARNING: posting the batched review failed; listing its findings in the summary instead")
            for c in comments:
                body, _ = _read_body(c, output_dir)
                moved.append((c, body, "GitHub rejected the inline review"))

    failed = []
    replied = set()
    for action, thread, body_file in actions:
        tid = thread["id"]
        if action == "reply":
            first = ((thread.get("comments") or {}).get("nodes") or [{}])[0]
            parent = first.get("databaseId")
            ok = parent is not None and gh.post_pr_line_comment(
                body_file=body_file, in_reply_to=parent, pr=pr_number, repo=repo)
            if ok:
                replied.add(tid)
        elif action == "resolve":
            ok = gh.resolve_pr_review_thread(tid)
        else:
            resolved_by = (thread.get("resolvedBy") or {}).get("login")
            if not is_bot(resolved_by) and tid not in replied:
                # Allowed only together with a reply, which did not get posted.
                print(f"Not re-opening thread {tid}: its reply was not posted")
                continue
            ok = gh.unresolve_pr_review_thread(tid)
        if not ok:
            print(f"WARNING: thread action {action} on {tid} failed")
            failed.append((action, thread, body_file))

    summary = local_links_to_github(summary_text, repo, head_sha)
    summary = summary.rstrip() + "\n" + moved_findings_markdown(moved) + failed_actions_markdown(failed, repo, pr_number, output_dir)
    return summary
