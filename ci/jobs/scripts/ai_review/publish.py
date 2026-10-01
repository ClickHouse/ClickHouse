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
  * Every Blocker and Major that passes these checks is posted inline,
    Blockers first; they have already passed the evidence bar.
  * Only threads the review created may be resolved; a thread is re-opened only
    when the review resolved it itself, or together with a reply in the same
    run. At most one reply per thread per run.
"""

import json
import os
import re

from ci.jobs.scripts.ai_review import units as review_units
from ci.jobs.scripts.ai_review.context import thread_is_ours, is_bot

_HUNK_RE = re.compile(r"^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@")

# How far from a changed line a comment about unchanged code in the same file
# may be anchored on it.
_REANCHOR_DISTANCE = 40

# The output directory relative to the tree, as the agent sees it.
_OUTPUT_SUFFIX = "/ci/tmp/ai_review/out/"

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
    """The body of a comment or reply, read only from a regular file inside
    `base_dir` (the output directory). The agent chooses `body_file` and the
    text ends up on GitHub, so a path outside the directory, a traversal or a
    symlink out of it would let it publish any file the job can read."""
    body_file = entry.get("body_file") or ""
    if not body_file:
        return "", body_file
    base = os.path.realpath(base_dir)
    if os.path.isabs(body_file) and _OUTPUT_SUFFIX in body_file:
        # An absolute path into the agent's copy of the tree: the same file
        # was copied into the output directory.
        body_file = os.path.join(base_dir, body_file.rsplit(_OUTPUT_SUFFIX, 1)[1])
    candidate = body_file if os.path.isabs(body_file) or os.path.exists(body_file) else os.path.join(base_dir, body_file)
    resolved = os.path.realpath(candidate)
    if os.path.commonpath([base, resolved]) != base or os.path.islink(candidate) or not os.path.isfile(resolved):
        print(f"WARNING: ignoring body file [{body_file}]: not a regular file inside {base_dir}")
        return "", body_file
    with open(resolved, "r", encoding="utf-8", errors="replace") as f:
        return f.read().strip(), resolved


def dismissed_findings(records):
    """From recalled review memory: the findings authors pushed back on (they
    replied, and the review did not resolve the thread as fixed), per path."""
    out = {}
    for r in records or []:
        if r.get("author_replied") and r.get("state") in ("resolved_by_author", "open"):
            finding = (r.get("finding") or "").split("\n\nReply by", 1)[0]
            out.setdefault(r.get("path"), []).append(finding)
    return out


def _write_reanchored(base_dir, path, line, body):
    target = os.path.join(base_dir, "comments", f"reanchored_{abs(hash((path, line)))}.md")
    os.makedirs(os.path.dirname(target), exist_ok=True)
    with open(target, "w", encoding="utf-8") as f:
        f.write(body + "\n")
    return target


def validate_comments(entries, files, threads, base_dir, dismissed=None, units=None, known_findings=None):
    """Split the agent's inline comments into (postable, moved) where `moved`
    are (entry, body, reason) to be listed in the summary instead. `dismissed`
    maps a path to findings authors pushed back on in earlier PRs; a new
    comment that repeats one goes to the summary instead of inline."""
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
            # A finding about related code next to the change (a caller in the
            # same file, the other half of a pair) is anchored on the nearest
            # changed line, naming the line it is about, instead of disappearing
            # into the summary.
            nearest = min(lines.get("RIGHT", {}), key=lambda n: abs(n - line), default=None)
            if nearest is None or abs(nearest - line) > _REANCHOR_DISTANCE:
                moved.append((e, body, f"line {line} ({side}) is not in the PR diff"))
                continue
            if f"{path}:{line}" not in body and f":{line}`" not in body:
                body = f"`{path}:{line}`: " + body
                body_file = _write_reanchored(base_dir, path, line, body)
            line, side, start = nearest, "RIGHT", None
            side_lines = lines["RIGHT"]
        if start is not None:
            start_side = (e.get("start_side") or side).upper()
            if start >= line or start_side != side or side_lines.get(start) != side_lines[line]:
                start = None  # keep the comment, anchored on its last line only
        if (path, line) in open_ours or (path, line, side) in seen or any(
                _similar(body, other) for other in open_texts.get(path, [])):
            print(f"Skipping duplicate inline comment on {path}:{line}")
            continue
        if any(_similar(body, other) for other in (dismissed or {}).get(path, [])):
            moved.append((e, body, "an author pushed back on the same finding in an earlier PR"))
            continue
        unit = review_units.unit_for_line(units or [], path, line, side)
        fingerprint = review_units.finding_fingerprint(path, unit["key"] if unit else "", body)
        e["_fingerprint"] = fingerprint
        if (unit and not review_units.in_scope(unit) and fingerprint not in (known_findings or set())
                and (e.get("severity") or "").lower() != "blocker"):
            # A late finding: code unchanged since the previous review, so not
            # caused by this push. Kept, but not as a new inline comment.
            moved.append((e, body, "code unchanged since the previous review"))
            continue
        seen.add((path, line, side))
        open_texts.setdefault(path, []).append(body)
        comment = {"path": path, "line": line, "side": side, "body_file": body_file,
                   "_blocker": (e.get("severity") or "").lower() == "blocker", "_fingerprint": fingerprint}
        if start is not None:
            comment["start_line"] = start
            comment["start_side"] = side
        postable.append(comment)
    # Blockers first, then in the agent's order.
    postable.sort(key=lambda c: not c["_blocker"])
    for c in postable:
        del c["_blocker"]
    return postable, moved


SIMPLICITY_RULES = frozenset({
    "reuse_existing", "unused_code", "single_use", "impossible_check", "unrecoverable_fallback",
    "duplicated_block", "commented_out", "comment_restates", "comment_narrates_change",
    "comment_oversized", "test_comment_internals", "scope_creep", "simpler_equivalent",
})
# Simplicity findings have their own, smaller inline budget, so they never
# displace a bug; only the ones whose fix is a ready `suggestion` go inline.
MAX_INLINE_SIMPLICITY = 3
MAX_LISTED_SIMPLICITY = 10
RULE_MARKER = "<!-- ai-review-rule: {rule} -->"
_RULE_RE = re.compile(r"<!-- ai-review-rule: ([a-z_]+) -->")


def rule_of(body):
    m = _RULE_RE.search(body or "")
    return m.group(1) if m else ""


def validate_simplicity(entries, files, threads, base_dir, units=None, known_findings=None):
    """Split simplicity findings into (inline, listed). A finding needs a
    known rule, evidence, and a line of the diff in code this push changed;
    inline additionally needs a `suggestion` block on a RIGHT line. Inline
    entries are written to new body files carrying the rule marker, so the
    review memory can later tell which rules authors act on."""
    lines_by_path = {f["filename"]: commentable_lines(f.get("patch")) for f in files}
    open_texts = {}
    for t in threads or []:
        if thread_is_ours(t) and not t.get("isResolved"):
            first = ((t.get("comments") or {}).get("nodes") or [{}])[0]
            open_texts.setdefault(t.get("path"), []).append(first.get("body") or "")
    inline, listed = [], []
    for e in entries:
        rule = (e.get("rule") or "").strip()
        body, _ = _read_body(e, base_dir)
        if rule not in SIMPLICITY_RULES or not (e.get("evidence") or "").strip() or not body:
            print(f"Dropping simplicity finding without a known rule, evidence or body: {e.get('path')}:{e.get('line')}")
            continue
        path, side = e.get("path") or "", (e.get("side") or "RIGHT").upper()
        try:
            line = int(e.get("line"))
        except (TypeError, ValueError):
            continue
        if line not in lines_by_path.get(path, {}).get(side, {}):
            listed.append((e, body, rule))
            continue
        unit = review_units.unit_for_line(units or [], path, line, side)
        if unit and not review_units.in_scope(unit):
            continue  # unchanged since the previous review: not this push's to raise
        if any(_similar(body, other) for other in open_texts.get(path, [])):
            continue
        open_texts.setdefault(path, []).append(body)
        if side == "RIGHT" and "```suggestion" in body and len(inline) < MAX_INLINE_SIMPLICITY:
            posted = os.path.join(base_dir, "simplicity", f"posted_{len(inline) + 1}.md")
            os.makedirs(os.path.dirname(posted), exist_ok=True)
            with open(posted, "w", encoding="utf-8") as f:
                f.write(body + "\n\n" + RULE_MARKER.format(rule=rule) + "\n")
            inline.append({"path": path, "line": line, "side": side, "body_file": posted,
                           "_fingerprint": review_units.finding_fingerprint(path, unit["key"] if unit else "", body)})
        else:
            listed.append((e, body, rule))
    return inline, listed


def simplicity_markdown(listed):
    if not listed:
        return ""
    out = ["", f"<details><summary>Simplification and comments ({len(listed)})</summary>", ""]
    for e, body, rule in listed[:MAX_LISTED_SIMPLICITY]:
        first = " ".join(body.split("```", 1)[0].split()).lstrip("💡 ").strip()
        if len(first) > 300:
            first = first[:300] + " ..."
        out.append(f"- `{e.get('path')}:{e.get('line')}` ({rule.replace('_', ' ')}): {first}")
    if len(listed) > MAX_LISTED_SIMPLICITY:
        out.append(f"- ... and {len(listed) - MAX_LISTED_SIMPLICITY} more of the same kinds")
    out.append("</details>")
    return "\n".join(out) + "\n"


def coverage_gaps(output_dir, units):
    """In-scope units the agent gave no verdict for in `coverage.json`."""
    try:
        entries = _load_json_list(os.path.join(output_dir, "coverage.json"))
    except (ValueError, OSError):
        entries = []
    covered = {str(e.get("unit")) for e in entries if isinstance(e, dict) and e.get("verdict")}
    return [u for u in units or [] if review_units.in_scope(u) and u["id"] not in covered]


def coverage_markdown(gaps):
    if not gaps:
        return ""
    names = ", ".join(f"{u['id']} `{u['path']}`" + (f" `{u['heading']}`" if u["heading"] else "") for u in gaps[:30])
    more = f" and {len(gaps) - 30} more" if len(gaps) > 30 else ""
    return (f"\n<details><summary>Review units without a verdict in this run ({len(gaps)})</summary>\n\n"
            f"{names}{more}\n\n</details>\n")


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


def publish(gh, repo, pr_number, head_sha, files, threads, output_dir, summary_text, memory=None,
            units=None, previous_state=None, simplicity=True):
    """Post the inline review and the thread actions. `gh` is the praktika GH
    class (injected for tests). Returns the summary text to post, with the
    comments that could not be attached inline appended."""
    known = {f.get("fp") for f in (previous_state or {}).get("findings") or []}
    comments, moved = validate_comments(
        _load_json_list(os.path.join(output_dir, "comments.json")), files, threads, output_dir,
        dismissed_findings(memory), units, known)
    simplicity_inline, simplicity_listed = ([], [])
    if simplicity:
        simplicity_inline, simplicity_listed = validate_simplicity(
            _load_json_list(os.path.join(output_dir, "simplicity.json")), files, threads, output_dir, units, known)
        print(f"Simplicity findings: {len(simplicity_inline)} inline, {len(simplicity_listed)} in the summary")
    comments = comments + simplicity_inline
    late = [m for m in moved if m[2] == "code unchanged since the previous review"]
    gaps = coverage_gaps(output_dir, units)
    print(f"Review units: {sum(1 for u in units or [] if review_units.in_scope(u))} in scope, "
          f"{len(gaps)} without a verdict; late findings kept out of inline comments: {len(late)}")
    actions = validate_thread_actions(
        _load_json_list(os.path.join(output_dir, "thread_actions.json")), threads, output_dir)

    if comments:
        print(f"Posting a review with {len(comments)} inline comment(s)")
        payload = [{k: v for k, v in c.items() if not k.startswith("_")} for c in comments]
        if not gh.post_pr_review(commit_id=head_sha, comments=payload, pr=pr_number, repo=repo):
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
    summary = (summary.rstrip() + "\n" + simplicity_markdown(simplicity_listed) + coverage_markdown(gaps)
               + moved_findings_markdown(moved) + failed_actions_markdown(failed, repo, pr_number, output_dir))
    if units is not None:
        findings = list((previous_state or {}).get("findings") or [])
        posted = {c["_fingerprint"] for c in comments if c.get("_fingerprint")}
        findings += [{"fp": fp} for fp in sorted(posted - known)]
        contract, _ = _read_body({"body_file": os.path.join(output_dir, "contract.md")}, output_dir)
        summary += "\n" + review_units.encode_state(
            units, findings[-200:], contract or (previous_state or {}).get("contract", "")) + "\n"
    return summary
