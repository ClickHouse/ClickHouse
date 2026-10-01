"""
Review context, fetched by the job before the agent starts.

The agent used to fetch the PR, its threads and its conversation itself, with
`gh`, during the run. Moving the reads into the job gives the agent a complete,
consistent snapshot, lets these reads be retried cheaply on a transient GitHub
error instead of re-running the whole agent, and means the agent needs no
GitHub credentials at all.

Everything lands in one directory (`CONTEXT_DIR`), described to the agent by
`index_markdown()`:

  pr.md                  title, author, branches, labels, description, file list
  diff.patch             the PR diff as GitHub serves it (what inline comments can target)
  files.json             GitHub `pulls/<n>/files` rows (status, additions, patch)
  threads.md / .json     inline review threads, with who wrote each and their state
  conversation.md        top-level comments and review bodies, in order
  linked_issues.md       issues/PRs referenced in the description
  ci_status.md           check runs on the head commit (Style Check, Fast Test, ...)
  previous_review.md     the last AI review summary on this PR, if any
  since_last_review.md   commits pushed since that review, with their diff
  loom/                  Loom brief and raw answers (see loom.py)
"""

import hashlib
import json
import os
import re
import shlex
import unicodedata

from ci.jobs.scripts.ai_review import units as review_units
from ci.praktika.gh import GH

# The app the job posts as. Its identity appears as `clickhouse-gh` in
# `author.login` and as `clickhouse-gh[bot]` in `resolvedBy.login`.
BOT_LOGINS = {"clickhouse-gh", "clickhouse-gh[bot]"}

# Hidden marker the job appends to each summary it posts, so the next run knows
# which commit the previous review saw.
REVIEWED_SHA_MARKER = "<!-- ai-review-reviewed-sha: {sha} -->"
_REVIEWED_SHA_RE = re.compile(r"<!-- ai-review-reviewed-sha: ([0-9a-f]{7,40}) -->")

# Tag of the updateable summary comment (see GH.post_updateable_comment).
REVIEW_COMMENT_TAG = "review"
_REVIEW_COMMENT_START = "<!-- CI automatic comment start :review: -->"
_REVIEW_COMMENT_END = "<!-- CI automatic comment end :review: -->"

_MAX_LINKED_ISSUES = 8


_HTML_COMMENT_RE = re.compile(r"<!--.*?(-->|$)", re.S)


def untrusted(text):
    """Contributor-written text as the agent should see it: HTML comments and
    invisible format characters (zero-width, bidirectional controls) removed.
    Both are invisible on GitHub, which makes them the usual carrier for
    instructions aimed at an AI reviewer rather than at people."""
    text = _HTML_COMMENT_RE.sub("", text or "")  # an unclosed `<!--` hides the rest on GitHub too
    return "".join(c for c in text if not _invisible(c))


def _invisible(c):
    if c in "\n\t":
        return False
    code = ord(c)
    return (unicodedata.category(c) == "Cf"
            or 0xFE00 <= code <= 0xFE0F or 0xE0100 <= code <= 0xE01EF)  # variation selectors


def gh_json(endpoint, paginate=False, strict=False):
    cmd = f"gh api -H 'Accept: application/vnd.github+json' {shlex.quote(endpoint)}"
    if paginate:
        cmd += " --paginate"
    out = GH.get_output_with_retries(cmd, strict=strict)
    if not out:
        return None
    return GH._json_loads_paginated(out) if paginate else json.loads(out)


def is_bot(login):
    return (login or "") in BOT_LOGINS


def is_automation(login):
    """Any bot account, ours or another's (`[bot]` in REST, or a known name)."""
    login = (login or "").lower()
    return is_bot(login) or login.endswith("[bot]") or login in ("github-actions", "copilot", "coderabbitai", "robot-clickhouse")


def thread_is_ours(thread):
    """A thread the review created: its first comment is by the app."""
    comments = (thread.get("comments") or {}).get("nodes") or []
    if not comments:
        return False
    first = comments[0]
    return bool(first.get("viewerDidAuthor")) or is_bot((first.get("author") or {}).get("login"))


def reviewed_sha(comment_body):
    m = _REVIEWED_SHA_RE.search(comment_body or "")
    return m.group(1) if m else ""


def _write(path, text):
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)


def _render_pr(pr, files):
    labels = ", ".join(l.get("name", "") for l in pr.get("labels") or []) or "none"
    head = pr.get("head") or {}
    base = pr.get("base") or {}
    lines = [
        f"# PR #{pr.get('number')}: {untrusted(pr.get('title'))}",
        "",
        f"- Author: `{(pr.get('user') or {}).get('login')}`",
        f"- Base: `{base.get('ref')}` at `{base.get('sha')}`",
        f"- Head: `{head.get('ref')}` at `{head.get('sha')}` (repository `{(head.get('repo') or {}).get('full_name')}`)",
        f"- Labels: {labels}",
        f"- Draft: {pr.get('draft')}",
        f"- Size: {pr.get('changed_files')} files, +{pr.get('additions')} -{pr.get('deletions')}, {pr.get('commits')} commits",
        "",
        "## Description",
        "",
        untrusted(pr.get("body") or "(empty)").strip(),
        "",
        "## Changed files",
        "",
    ]
    for f in files:
        note = "" if f.get("patch") else "  (no patch from GitHub: binary or too large; read it from the checkout)"
        renamed = f" (renamed from `{f['previous_filename']}`)" if f.get("previous_filename") else ""
        lines.append(f"- {f.get('status')} `{f['filename']}`{renamed} +{f.get('additions')} -{f.get('deletions')}{note}")
    if len(files) >= 3000:
        lines.append("\nGitHub lists at most 3000 files; this PR may have more.")
    return "\n".join(lines) + "\n"


def _render_diff(files):
    parts = []
    for f in files:
        if not f.get("patch"):
            continue
        name = f["filename"]
        old = f.get("previous_filename") or name
        parts.append(f"diff --git a/{old} b/{name}\n--- a/{old}\n+++ b/{name}\n{f['patch']}\n")
    return "".join(parts)


def _render_threads(threads, repo, pr_number):
    if not threads:
        return "No inline review threads.\n"
    out = []
    for t in threads:
        comments = (t.get("comments") or {}).get("nodes") or []
        state = "resolved" if t.get("isResolved") else "open"
        if t.get("isResolved") and t.get("resolvedBy"):
            state += f" by `{t['resolvedBy'].get('login')}`"
        if t.get("isOutdated"):
            state += ", outdated (the code it was on has changed)"
        owner = "yours" if thread_is_ours(t) else "not yours"
        out.append(f"## Thread `{t.get('id')}` on `{t.get('path')}:{t.get('line') or '?'}` ({state}; {owner})\n")
        if comments and comments[0].get("databaseId"):
            out.append(f"Link: https://github.com/{repo}/pull/{pr_number}#discussion_r{comments[0]['databaseId']}\n")
        for c in comments:
            who = (c.get("author") or {}).get("login") or "?"
            you = " (you)" if (c.get("viewerDidAuthor") or is_bot(who)) else ""
            out.append(f"**{who}{you}** at {c.get('createdAt')}:\n\n{untrusted(c.get('body')).strip()}\n")
        out.append("")
    return "\n".join(out)


def _render_conversation(issue_comments, reviews):
    events = []
    for c in issue_comments or []:
        body = c.get("body") or ""
        if _REVIEW_COMMENT_START in body and is_bot((c.get("user") or {}).get("login")):
            continue  # the previous AI summary is in previous_review.md
        events.append((c.get("created_at") or "", (c.get("user") or {}).get("login"), "comment", body))
    for r in reviews or []:
        if not (r.get("body") or "").strip():
            continue
        events.append((r.get("submitted_at") or "", (r.get("user") or {}).get("login"),
                       f"review ({r.get('state', '').lower()})", r.get("body") or ""))
    if not events:
        return "No top-level comments.\n"
    events.sort(key=lambda e: e[0])
    out = []
    for at, who, kind, body in events:
        you = " (you)" if is_bot(who) else ""
        out.append(f"**{who}{you}**, {kind}, at {at}:\n\n{untrusted(body).strip()}\n")
    return "\n".join(out)


def _linked_numbers(pr, repo):
    text = pr.get("body") or ""
    numbers = []
    for m in re.finditer(r"(?:(?<![\w/])#|github\.com/" + re.escape(repo) + r"/(?:issues|pull)/)(\d{2,7})\b", text):
        n = int(m.group(1))
        if n != pr.get("number") and n not in numbers:
            numbers.append(n)
    return numbers[:_MAX_LINKED_ISSUES]


def _render_linked(repo, numbers):
    out = []
    for n in numbers:
        item = gh_json(f"/repos/{repo}/issues/{n}")
        if not item:
            continue
        kind = "PR" if item.get("pull_request") else "Issue"
        out.append(f"## {kind} #{n}: {untrusted(item.get('title'))} [{item.get('state')}]\n\n{untrusted(item.get('body')).strip()}\n")
    return "\n".join(out) if out else "No linked issues.\n"


def _render_ci_status(repo, sha):
    pages = gh_json(f"/repos/{repo}/commits/{sha}/check-runs?per_page=100", paginate=True) or []
    runs = [r for page in pages if isinstance(page, dict) for r in page.get("check_runs") or []]
    if not runs:
        return "No check runs reported yet.\n"
    out = ["Check runs on the head commit when the review started:", ""]
    for r in sorted(runs, key=lambda r: r.get("name") or ""):
        out.append(f"- {r.get('name')}: {r.get('conclusion') or r.get('status')}")
    return "\n".join(out) + "\n"


def _previous_review(issue_comments):
    """The review section of the CI comment. CI keeps several tagged sections
    (report, summary, review, ...) in one comment."""
    for c in reversed(issue_comments or []):
        if not is_bot((c.get("user") or {}).get("login")):
            continue  # only the app's own comment; anyone can paste the tags
        body = c.get("body") or ""
        start = body.find(_REVIEW_COMMENT_START)
        if start < 0:
            continue
        end = body.rfind(_REVIEW_COMMENT_END)
        return body[start + len(_REVIEW_COMMENT_START):end if end > start else len(body)].strip()
    return ""


def _patch_content(patch):
    """The added and removed lines of a patch. Hunk line numbers and context
    lines change whenever the base branch moves; these do not."""
    if not patch:
        return patch
    return "\n".join(line for line in patch.split("\n") if line[:1] in ("+", "-"))


def _render_since_last_review(repo, pr_number, base_ref, last_sha, head_sha, files):
    """What changed in the PR since the previous review saw `last_sha`: the PR's
    own commits pushed since (merges of the base branch only counted), and the
    files whose PR diff differs from the PR diff at `last_sha`. Comparing the
    two PR diffs instead of `last_sha...head` keeps changes that arrived with a
    merge of the base branch out of it."""
    if not last_sha or head_sha.startswith(last_sha):
        return ""
    commits = gh_json(f"/repos/{repo}/pulls/{pr_number}/commits?per_page=100", paginate=True) or []
    shas = [c.get("sha") or "" for c in commits]
    position = next((i for i, sha in enumerate(shas) if sha.startswith(last_sha)), None)
    then = gh_json(f"/repos/{repo}/compare/{base_ref}...{last_sha}") if position is not None else None
    if position is None or not then:
        reason = ("the PR has more commits than GitHub lists" if len(commits) >= 250
                  else "the branch was force-pushed or rebased")
        return (f"The previous review saw `{last_sha[:12]}`, which is not in the PR's commit list ({reason}). "
                f"The units in scope are still exact; this list of commits is just not available.\n")

    new_commits = commits[position + 1:]
    own = [c for c in new_commits if len(c.get("parents") or []) < 2]
    merges = len(new_commits) - len(own)
    out = [f"The previous review saw `{last_sha[:12]}`."]
    if own:
        out += ["", "Commits pushed since:", ""]
        for c in own:
            out.append(f"- `{c.get('sha', '')[:12]}` {(untrusted((c.get('commit') or {}).get('message')).splitlines() or [''])[0]}")
    if merges:
        out += ["", f"{merges} merge commit(s) since; what they brought in from other branches is not part of the PR's diff."]

    def content(f):  # a file without a patch (binary, too large) compares by its blob
        return _patch_content(f.get("patch")) if f.get("patch") else f"blob:{f.get('sha')}"

    then_patches = {f["filename"]: content(f) for f in then.get("files") or []}
    now_patches = {f["filename"]: content(f) for f in files}
    changed = sorted(n for n in set(then_patches) | set(now_patches) if then_patches.get(n) != now_patches.get(n))
    if len(then.get("files") or []) >= 300:
        out += ["", "The PR had too many files at the previous review to compare them; review the whole PR."]
    elif changed:
        out += ["", "Files whose part of the PR diff changed since then (current diff in `diff.patch`):", ""]
        for n in changed:
            note = " (no longer changed by the PR)" if n not in now_patches else (" (new in the PR)" if n not in then_patches else "")
            out.append(f"- `{n}`{note}")
    else:
        out += ["", "The PR diff is the same as at the previous review."]
    return "\n".join(out) + "\n"


def _sanitized_threads(threads):
    out = []
    for t in threads or []:
        t = dict(t)
        comments = dict(t.get("comments") or {})
        comments["nodes"] = [{**c, "body": untrusted(c.get("body"))} for c in comments.get("nodes") or []]
        t["comments"] = comments
        out.append(t)
    return out


def discussion_fingerprint(threads, issue_comments, reviews):
    """A digest of what people have said and decided on the PR: the text of
    every comment and review by a person (so an edit counts), and which threads
    a person resolved (so a silent resolution counts). Resolutions by the
    review itself are left out, or every run that resolves a thread would make
    the next one look like new discussion."""
    items = []
    for t in threads or []:
        resolver = (t.get("resolvedBy") or {}).get("login") or ""
        by_person = bool(t.get("isResolved")) and not is_automation(resolver)
        items.append(f"t {t.get('id')} {int(by_person)}")
        for c in (t.get("comments") or {}).get("nodes") or []:
            if not (c.get("viewerDidAuthor") or is_automation((c.get("author") or {}).get("login"))):
                items.append(f"c {c.get('databaseId')} {c.get('body') or ''}")
    for c in issue_comments or []:
        if (c.get("user") or {}).get("type") != "Bot" and not is_automation((c.get("user") or {}).get("login")):
            items.append(f"i {c.get('id')} {c.get('body') or ''}")
    for r in reviews or []:
        if (r.get("user") or {}).get("type") != "Bot" and not is_automation((r.get("user") or {}).get("login")):
            items.append(f"r {r.get('id')} {r.get('state')} {r.get('body') or ''}")
    return hashlib.sha256("\0".join(sorted(items)).encode()).hexdigest()[:16]


class Context:
    """The fetched context. Holds what the job needs again after the run."""

    def __init__(self, directory, repo, pr, files, threads, previous_review, units=None, previous_state=None,
                 activity=""):
        self.directory = directory
        self.repo = repo
        self.pr = pr
        self.files = files
        self.threads = threads
        self.previous_review = previous_review
        self.units = units or []
        self.previous_state = previous_state
        # `discussion_fingerprint` of the PR when the context was fetched.
        self.activity = activity

    @property
    def nothing_new(self):
        """Every unit unchanged since the previous review and nobody wrote
        since: a run would only repeat the previous review."""
        return (self.previous_state is not None
                and not any(review_units.in_scope(u) for u in self.units)
                and self.activity == self.previous_state.get("activity"))

    @property
    def head_sha(self):
        return (self.pr.get("head") or {}).get("sha") or ""

    @property
    def last_reviewed_sha(self):
        return reviewed_sha(self.previous_review)


def fetch(directory, repo, pr_number):
    """Fetch everything into `directory`. The PR and its files are required
    (raise on failure); everything else is best effort."""
    os.makedirs(directory, exist_ok=True)
    # The files listing is always of the current head, so a push between the
    # two requests would pair one commit's metadata (and checkout) with
    # another's diff. Re-read the PR until both are of the same head.
    for attempt in range(3):
        pr = gh_json(f"/repos/{repo}/pulls/{pr_number}", strict=True)
        files = gh_json(f"/repos/{repo}/pulls/{pr_number}/files?per_page=100", paginate=True, strict=True) or []
        head_sha = (pr.get("head") or {}).get("sha") or ""
        after = (gh_json(f"/repos/{repo}/pulls/{pr_number}", strict=True).get("head") or {}).get("sha") or ""
        if after == head_sha:
            break
        print(f"PR head moved from {head_sha[:12]} to {after[:12]} while fetching the diff; fetching again")
    else:
        raise RuntimeError("the PR head kept moving while its diff was fetched")

    try:
        threads = GH.list_pr_review_threads(pr=pr_number, repo=repo)
    except Exception as e:  # noqa: BLE001
        print(f"WARNING: failed to list review threads: {e}")
        threads = []
    issue_comments = gh_json(f"/repos/{repo}/issues/{pr_number}/comments?per_page=100", paginate=True) or []
    reviews = gh_json(f"/repos/{repo}/pulls/{pr_number}/reviews?per_page=100", paginate=True) or []
    previous = _previous_review(issue_comments)

    _write(os.path.join(directory, "pr.md"), _render_pr(pr, files))
    _write(os.path.join(directory, "diff.patch"), _render_diff(files))
    _write(os.path.join(directory, "files.json"), json.dumps(files, indent=1))
    _write(os.path.join(directory, "threads.json"), json.dumps(_sanitized_threads(threads), indent=1))
    _write(os.path.join(directory, "threads.md"), _render_threads(threads, repo, pr_number))
    _write(os.path.join(directory, "conversation.md"), _render_conversation(issue_comments, reviews))
    _write(os.path.join(directory, "linked_issues.md"), _render_linked(repo, _linked_numbers(pr, repo)))
    if head_sha:
        _write(os.path.join(directory, "ci_status.md"), _render_ci_status(repo, head_sha))
    previous_state = review_units.decode_state(previous)
    units = review_units.scope(review_units.build(files), previous_state)
    incremental = previous_state is not None
    _write(os.path.join(directory, "units.md"), review_units.render(units, incremental))
    if previous_state and previous_state.get("contract"):
        _write(os.path.join(directory, "previous_contract.md"), previous_state["contract"])
    if previous:
        _write(os.path.join(directory, "previous_review.md"),
               re.sub(r"<!-- ai-review-[a-z-]+: [^>]*-->\n?", "", previous).rstrip() + "\n")
        since = _render_since_last_review(
            repo, pr_number, (pr.get("base") or {}).get("ref") or "master", reviewed_sha(previous), head_sha, files)
        if since:
            _write(os.path.join(directory, "since_last_review.md"), since)
    return Context(directory, repo, pr, files, threads, previous, units, previous_state,
                   discussion_fingerprint(threads, issue_comments, reviews))


def index_markdown(directory):
    """The list of context files for the prompt, with what each holds."""
    descriptions = [
        ("pr.md", "title, author, base and head, labels, description, changed files"),
        ("units.md", "the diff split into review units, riskiest first, with what is in scope for this push"),
        ("diff.patch", "the PR diff as GitHub serves it; inline comments can only target lines in it"),
        ("previous_contract.md", "the PR's intent and invariants as your previous review recorded them"),
        ("threads.md", "inline review threads: who wrote them, open/resolved, every reply"),
        ("conversation.md", "top-level comments and review bodies"),
        ("linked_issues.md", "issues and PRs the description references"),
        ("ci_status.md", "check runs on the head commit when the review started"),
        ("previous_review.md", "your previous summary on this PR"),
        ("since_last_review.md", "what changed in the PR since your previous review"),
        ("memory.md", "earlier review findings on the files this PR changes, and how they ended"),
        ("loom/brief.md", "Loom code index brief (below)"),
    ]
    out = []
    for name, what in descriptions:
        if os.path.exists(os.path.join(directory, name)):
            out.append(f"- `{os.path.join(directory, name)}`: {what}")
    return "\n".join(out)
