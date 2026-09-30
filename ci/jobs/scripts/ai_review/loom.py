"""
Loom code index client for the AI code review job.

Loom indexes ClickHouse master with a compiler-resolved call graph (scip-clang),
a test map, PR/issue history and a tracker. The review uses it for the parts of
a review that the diff does not show: the unchanged callers and sibling
implementations of a changed function, the tests that cover it, and earlier
issues in the same area.

Two entry points:

  * `write_brief()` - called by the job before the agent starts. Writes a short
    Markdown brief (touched functions and their fan-in, blast radius, linked
    tests, maintainer conventions, similar tracker items, index freshness) plus
    the raw JSON answers into the review context directory.
  * `python3 -m ci.jobs.scripts.ai_review.loom <command>` - the CLI the agent
    calls during the review (symbol bodies, callers, grep, ...).

Every call fails soft: when Loom is not configured, unreachable, slow, or
answers with an error, the client returns None / prints a short "unavailable"
line and the review proceeds with plain `git grep`. A Loom outage must never
fail or delay the review.

Configuration comes from the environment (`LOOM_BASE_URL`, `LOOM_TOKEN`,
`LOOM_NAMESPACE`, `LOOM_REPO`, `LOOM_PR_NUMBER`), set by the job for the agent
process from per-repository SSM secrets (`REPO_CONFIG`). A repository without an
entry, or with a missing secret, runs without Loom.

Only master is indexed. Code that exists only on the PR branch is not in the
index, except through the open-PR overlay of `review_brief` / `impact` /
`callers` for public PRs, which the Loom mirror follows.
"""

import argparse
import concurrent.futures
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
import uuid

ORG = "clickhouse"
CONSUMER = "ci-code-review"

# Per-repository Loom configuration. Only the public repository uses Loom; a
# repository without an entry (ClickHouse-private, forks) is reviewed without
# it. Should a private repository be added, its namespace must be a private
# one: `_refuse_cross_boundary` rejects a private repository configured with
# the public namespace, so a misconfiguration cannot send private diffs or code
# to the public index. `pr_overlay` marks repositories whose open PRs the Loom
# mirror follows (the PR-number based ops only work there).
PUBLIC_NAMESPACE = "code-clickhouse"
REPO_CONFIG = {
    "ClickHouse/ClickHouse": {
        "base_url_secret": "/ci/loom/base_url",
        "token_secret": "/ci/loom/api_key",
        "namespace": PUBLIC_NAMESPACE,
        # The review's own memory: one record per review thread and its outcome.
        "memory_namespace": "clickhouse-gh",
        "private": False,
        "pr_overlay": True,
    },
}

# Client-side timeouts per op, in seconds. Loom publishes a p99 budget per op;
# these are a few times that, so a slow answer is dropped instead of stalling
# the review. The brief ops are built on demand for an open PR and take longer.
_TIMEOUTS = {
    "code.review_brief": 30,
    "code.impact": 15,
    "code.test_gate": 15,
    "code.conventions": 20,
    "code.verify_citations": 15,
    "code.search": 10,
    "code.blame_range": 20,
    "code.symbol_history": 10,
    "tracker.similar": 10,
}
_DEFAULT_TIMEOUT = 8

# Per-process call log, appended to LOOM_CALL_LOG when set: one JSON line per
# call (op, status, ms). Attached to the job result so Loom's effect on reviews
# can be measured.
_CALL_LOG_ENV = "LOOM_CALL_LOG"

_MAX_DIFF_BYTES = 200_000
_MAX_FILES = 50


class Config:
    def __init__(self, base_url="", token="", namespace="", repo="", pr_number=0, private=False, pr_overlay=False,
                 memory_namespace=""):
        self.base_url = (base_url or "").rstrip("/")
        self.token = token or ""
        self.namespace = namespace or ""
        self.memory_namespace = memory_namespace or ""
        self.repo = repo or ""
        self.pr_number = int(pr_number or 0)
        self.private = bool(private)
        self.pr_overlay = bool(pr_overlay)

    def available(self):
        return bool(self.base_url and self.token and self.namespace)

    def env(self):
        """Environment for the agent process, so the CLI sees the same config."""
        return {
            "LOOM_BASE_URL": self.base_url,
            "LOOM_TOKEN": self.token,
            "LOOM_NAMESPACE": self.namespace,
            "LOOM_REPO": self.repo,
            "LOOM_PR_NUMBER": str(self.pr_number),
            "LOOM_PRIVATE": "1" if self.private else "0",
            "LOOM_PR_OVERLAY": "1" if self.pr_overlay else "0",
        }

    @classmethod
    def from_env(cls):
        return cls(
            base_url=os.environ.get("LOOM_BASE_URL", ""),
            token=os.environ.get("LOOM_TOKEN", ""),
            namespace=os.environ.get("LOOM_NAMESPACE", ""),
            repo=os.environ.get("LOOM_REPO", ""),
            pr_number=os.environ.get("LOOM_PR_NUMBER", "0") or 0,
            private=os.environ.get("LOOM_PRIVATE", "0") == "1",
            pr_overlay=os.environ.get("LOOM_PR_OVERLAY", "0") == "1",
        )

    @classmethod
    def for_repo(cls, repo, pr_number, get_secret):
        """Resolve the config of `repo` from SSM via `get_secret(name) -> str`.
        Returns an unavailable Config when the repository has no entry or a
        secret cannot be read."""
        entry = REPO_CONFIG.get(repo)
        if not entry:
            print(f"Loom: no configuration for repository [{repo}], reviewing without Loom")
            return cls(repo=repo, pr_number=pr_number)
        try:
            base_url = get_secret(entry["base_url_secret"])
            token = get_secret(entry["token_secret"])
            namespace = entry.get("namespace") or get_secret(entry["namespace_secret"])
        except Exception as e:  # noqa: BLE001 - a missing secret means "no Loom", not a failed review
            # The AWS error names the role and the parameter, never its value.
            print(f"Loom: configuration for [{repo}] is not readable ({type(e).__name__}: {str(e)[:500]}), reviewing without Loom")
            return cls(repo=repo, pr_number=pr_number)
        return cls(
            base_url=(base_url or "").strip(),
            token=(token or "").strip(),
            namespace=(namespace or "").strip(),
            repo=repo,
            pr_number=pr_number,
            private=entry["private"],
            pr_overlay=entry["pr_overlay"],
            memory_namespace=entry.get("memory_namespace", ""),
        )


def _refuse_cross_boundary(config):
    return config.private and config.namespace == PUBLIC_NAMESPACE


def _log_call(op, status, ms):
    path = os.environ.get(_CALL_LOG_ENV)
    if not path:
        return
    try:
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps({"op": op, "status": status, "ms": ms, "at": int(time.time())}) + "\n")
    except OSError:
        pass


def call(config, op, body, namespace=None):
    """POST /v1/<op>. Returns the parsed answer, or None on any failure.
    A 404 (name not found) returns the answer with `_not_found` set, because
    "this symbol does not exist on master" is itself useful. `namespace`
    overrides the code namespace (the memory ops use the memory namespace)."""
    if not config.available():
        return None
    if _refuse_cross_boundary(config):
        print(f"Loom: REFUSED {op}: private repository configured with the public namespace")
        _log_call(op, "refused_cross_boundary", 0)
        return None
    payload = {**body, "org": ORG, "namespace": namespace or config.namespace, "consumer": CONSUMER, "agent": CONSUMER}
    request = urllib.request.Request(
        f"{config.base_url}/v1/{op}",
        data=json.dumps(payload).encode(),
        method="POST",
        headers={
            "Authorization": f"Bearer {config.token}",
            "Content-Type": "application/json",
            "X-Request-Id": uuid.uuid4().hex,
        },
    )
    started = time.time()
    try:
        with urllib.request.urlopen(request, timeout=_TIMEOUTS.get(op, _DEFAULT_TIMEOUT)) as response:
            data = json.loads(response.read().decode() or "{}")
        _log_call(op, "ok", int((time.time() - started) * 1000))
        return data
    except urllib.error.HTTPError as e:
        ms = int((time.time() - started) * 1000)
        if e.code == 404:
            _log_call(op, "not_found", ms)
            try:
                data = json.loads(e.read().decode() or "{}")
            except ValueError:
                data = {}
            data["_not_found"] = True
            return data
        _log_call(op, f"http{e.code}", ms)
        return None
    except Exception as e:  # noqa: BLE001 - timeouts, DNS, TLS, bad JSON: all mean "no answer"
        _log_call(op, f"error:{type(e).__name__}", int((time.time() - started) * 1000))
        return None


# ── Brief written by the job before the agent starts ─────────────────────────


def _diff_text(files):
    """A unified diff assembled from the GitHub `pulls/<n>/files` rows."""
    parts = []
    for f in files:
        patch = f.get("patch")
        if not patch:
            continue
        name = f["filename"]
        parts.append(f"diff --git a/{name} b/{name}\n--- a/{name}\n+++ b/{name}\n{patch}\n")
    return "".join(parts)[:_MAX_DIFF_BYTES]


def source_first(paths):
    return sorted(paths, key=lambda p: (not p.startswith("src/"), p.startswith("tests/"), p))


def _render_index_status(d, base_sha):
    if not d:
        return []
    cov = d.get("commit_coverage") or {}
    lines = [f"- Index: master at `{(d.get('scip_commit') or d.get('git_head') or '?')[:12]}` (C++ call graph)."]
    if base_sha and cov:
        tiers = {t.get("label"): t for t in cov.get("tiers") or []}
        cpp = tiers.get("C++") or {}
        if cpp and not cpp.get("covered", True):
            lines.append(
                f"- The index does not yet include the PR base `{base_sha[:12]}`: code merged to master since the "
                f"indexed commit is missing from Loom answers. Mention this under \"Missing context\" if it matters."
            )
    return lines


_ANON_RE = re.compile(r"\$anonymous_namespace_[^:]*::")


def _name(qualified):
    """`DB::$anonymous_namespace_src/X.cpp::f` -> `DB::f`."""
    return _ANON_RE.sub("", qualified or "?")


def _is_test_path(path):
    path = path or ""
    return path.startswith("tests/") or "/tests/" in path or "gtest_" in path


def _is_runnable_test(path):
    """A test file, as opposed to test configs and harness helpers."""
    path = path or ""
    if path.startswith("tests/queries/"):
        return path.endswith((".sql", ".sh", ".py", ".j2", ".expect"))
    if path.startswith("tests/integration/"):
        return os.path.basename(path).startswith("test") and path.endswith(".py") and "/helpers/" not in path
    return "gtest_" in path and path.endswith(".cpp")


def _render_review_brief(d):
    if not d or d.get("_not_found") or d.get("source") == "missing":
        return []
    out = []
    secs = d.get("sections") or {}
    decisions = d.get("decisions") or {}
    if decisions.get("test_only"):
        out.append("- Test-only change: no source function is touched.")
    symbols = (secs.get("symbols") or {}).get("rows") or []
    if symbols:
        out.append("- Functions the PR changes, with their number of call sites outside tests (fan-in):")
        for r in symbols[:15]:
            out.append(
                f"  - `{_name(r.get('qualified_name'))}` {r.get('path') or '?'}:{r.get('start_line') or '?'}"
                f" (fan-in {r.get('fan_in_nontest', r.get('fan_in', '?'))})"
            )
        if len(symbols) > 15:
            out.append(f"  - ... {len(symbols) - 15} more in `loom/review_brief.json`")
    tests = (secs.get("tests") or {}).get("rows") or []
    covered = [r for r in tests if r.get("verdict") == "covered_by" and r.get("tests")]
    if covered:
        out.append("- Tests that exercise the changed functions:")
        for r in covered[:10]:
            more = (r.get("tests_total") or 0) - len((r.get("tests") or [])[:3])
            out.append(f"  - `{_name(r.get('subject'))}`: " + ", ".join(f"`{t}`" for t in (r.get("tests") or [])[:3])
                       + (f" (+{more})" if more > 0 else ""))
    history = (secs.get("history") or {}).get("rows") or []
    risky = [r for r in history if (r.get("caused_issues") or 0) or (r.get("reverts") or 0) or (r.get("open_issues_naming") or 0)]
    for r in risky[:8]:
        parts = []
        if r.get("caused_issues"):
            parts.append(f"{r['caused_issues']} issue(s) caused by earlier changes")
        if r.get("reverts"):
            parts.append(f"{r['reverts']} revert(s)")
        if r.get("open_issues_naming"):
            parts.append(f"{r['open_issues_naming']} open issue(s) naming it")
        out.append(f"- History of `{_name(r.get('qualified_name'))}`: {', '.join(parts)} (`loom history --name`).")
    if decisions.get("randomized_setting"):
        out.append("- A setting the PR touches is randomized in CI tests, so test runs see different values of it.")
    do_not_flag = (secs.get("do_not_flag") or {}).get("rows") or []
    for r in do_not_flag[:6]:
        out.append(f"- Known false positive here, do not flag: {str(r.get('rule') or r.get('text') or r)[:200]}")
    return out


def _render_impact(d, pr_paths):
    if not d:
        return []
    out = []
    by_file = {}
    for s in d.get("impacted_symbols") or []:
        path = s.get("path")
        if not path or path in pr_paths or _is_test_path(path):
            continue
        entry = by_file.setdefault(path, {"hop": s.get("hop") or 9, "names": []})
        entry["hop"] = min(entry["hop"], s.get("hop") or 9)
        entry["names"].append(_name(s.get("qualified_name")))
    if by_file:
        files = sorted(by_file.items(), key=lambda kv: (kv[1]["hop"], -len(kv[1]["names"]), kv[0]))
        total = sum(len(v["names"]) for v in by_file.values())
        out.append(f"- Unchanged code that calls into the change ({total} functions in {len(by_file)} files, "
                   f"up to 2 calls away; direct callers first). Check that the changed contract still holds there:")
        for path, v in files[:15]:
            names = v["names"]
            shown = ", ".join(f"`{n}`" for n in names[:3]) + (f" (+{len(names) - 3})" if len(names) > 3 else "")
            out.append(f"  - {path} ({'direct' if v['hop'] == 1 else 'indirect'}): {shown}")
        if len(files) > 15:
            out.append(f"  - ... {len(files) - 15} more files in `loom/impact.json`")
    missing = d.get("co_change_missing") or []
    if missing:
        out.append("- Files that usually change together with the touched ones but are not in this PR: " + ", ".join(
            f"`{m.get('path')}` ({m.get('prs')} of {m.get('of')} PRs)" if isinstance(m, dict) else f"`{m}`"
            for m in missing[:8]))
    reverts = d.get("reverts") or []
    if reverts:
        out.append(f"- {len(reverts)} earlier revert(s) touched this code; see `loom/impact.json`.")
    return out


def _render_test_gate(d):
    if not d:
        return []
    out = []
    seen = set()
    tests = []
    for t in d.get("tests") or []:
        if t.get("why") == "changed_test" or not _is_runnable_test(t.get("test_path")):
            continue
        key = t.get("run") or t.get("test_path")
        if key in seen:
            continue
        seen.add(key)
        tests.append(t)
    if tests:
        out.append("- Existing tests that reach the changed code (the PR's own tests excluded):")
        for t in tests[:12]:
            out.append(f"  - `{t.get('test_path')}`")
    untested = [u for u in d.get("untested") or [] if isinstance(u, dict) and not _is_test_path(u.get("path"))]
    if untested:
        out.append("- Changed functions no existing test reaches in the test map (a lower bound, not proof): " + ", ".join(
            f"`{_name(u.get('qualified_name'))}`" for u in untested[:10])
            + (f" (+{len(untested) - 10})" if len(untested) > 10 else ""))
    return out


def _render_conventions(d):
    if not d:
        return []
    out = []
    for r in d.get("rules") or []:
        verdict = r.get("verdict")
        if verdict in ("satisfied", "not_applicable", "", None):
            continue
        where = ""
        ev = r.get("evidence") or []
        if ev and isinstance(ev[0], dict) and ev[0].get("path"):
            where = f" at `{ev[0].get('path')}:{ev[0].get('line')}`"
        out.append(f"- Maintainer convention `{r.get('rule')}`: {verdict}{where}. {r.get('note') or ''}".rstrip())
    return out


# A tracker item whose text is this close to the PR (cosine distance of the
# vector leg) is shown; lexical-only matches are mostly shared vocabulary.
_SIMILAR_MAX_DISTANCE = 0.42


def _render_similar(d, pr_number):
    if not d:
        return []
    items = [i for i in d.get("items") or []
             if not (i.get("kind") == "pr" and i.get("number") == pr_number)
             and ((i.get("relation") or "none") != "none"
                  or (i.get("distance") is not None and i["distance"] <= _SIMILAR_MAX_DISTANCE))]
    if not items:
        return []
    out = ["- Tracker items similar to this PR (reference one when a finding matches it):"]
    for i in items[:6]:
        out.append(f"  - {i.get('kind')} #{i.get('number')} [{i.get('state')}]: {i.get('title')}")
    return out


def write_brief(config, pr, files, out_dir):
    """Fetch the Loom brief for this PR into `out_dir` (brief.md + raw JSON).
    Returns the Markdown brief, or "" when Loom gave nothing."""
    if not config.available():
        return ""
    os.makedirs(out_dir, exist_ok=True)
    paths = source_first([f["filename"] for f in files])[:_MAX_FILES]
    pr_paths = set(paths)
    diff = _diff_text(files)
    author = (pr.get("user") or {}).get("login") or ""
    base_sha = (pr.get("base") or {}).get("sha") or ""
    overlay = config.pr_overlay and config.pr_number > 0

    requests = {
        "index_status": ("code.index_status", {"brief": True, **({"commit": base_sha} if base_sha else {})}),
        "similar": ("tracker.similar", {"text": f"{pr.get('title') or ''}\n\n{(pr.get('body') or '')[:2000]}", "top_k": 8}),
    }
    if paths:
        requests["impact"] = ("code.impact", {
            "files": paths, "diff": diff, "depth": 2,
            **({"exclude_authors": [author]} if author else {}),
            **({"pr_number": config.pr_number} if overlay else {}),
        })
        requests["test_gate"] = ("code.test_gate", {"files": paths, "diff": diff, "depth": 2, "limit": 30})
        requests["conventions"] = ("code.conventions", {"files": paths, "diff": diff})
    if overlay:
        requests["review_brief"] = ("code.review_brief", {
            "pr_number": config.pr_number, "tier": "standard", "open": True,
            "sections": ["paths", "symbols", "tests", "history", "do_not_flag"]})
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(requests)) as pool:
        futures = {name: pool.submit(call, config, op, body) for name, (op, body) in requests.items()}
        answers = {name: f.result() for name, f in futures.items()}

    for name, data in answers.items():
        if data is not None:
            with open(os.path.join(out_dir, f"{name}.json"), "w", encoding="utf-8") as f:
                json.dump(data, f, indent=1)

    lines = []
    lines += _render_index_status(answers.get("index_status"), base_sha)
    lines += _render_review_brief(answers.get("review_brief"))
    lines += _render_impact(answers.get("impact"), pr_paths)
    # review_brief lists tests per changed function; test_gate is the fallback
    # for repositories without the PR overlay. Its untested list is kept.
    has_brief_tests = any("Tests that exercise the changed functions" in l for l in lines)
    gate = _render_test_gate(answers.get("test_gate"))
    if has_brief_tests:
        gate = [l for l in gate if l.startswith("- Changed functions no existing test")]
    lines += gate
    lines += _render_conventions(answers.get("conventions"))
    lines += _render_similar(answers.get("similar"), config.pr_number)
    if not any(answers.values()):
        return ""
    brief = "\n".join(lines) + "\n"
    with open(os.path.join(out_dir, "brief.md"), "w", encoding="utf-8") as f:
        f.write(brief)
    return brief


# ── Review memory ────────────────────────────────────────────────────────────


def _thread_state(thread):
    if not thread.get("isResolved"):
        return "open"
    resolved_by = ((thread.get("resolvedBy") or {}).get("login") or "")
    return "resolved_by_review" if resolved_by.startswith("clickhouse-gh") else "resolved_by_author"


def thread_record(repo, pr_number, thread, is_ours):
    """The memory row for one review thread of ours, or None. Keyed by the
    thread's first comment, so each run upserts the same row as the thread's
    state and replies change (the server skips an unchanged row)."""
    comments = (thread.get("comments") or {}).get("nodes") or []
    if not comments or not is_ours(thread) or not comments[0].get("databaseId"):
        return None
    first = comments[0]
    replies = [c for c in comments[1:] if (c.get("body") or "").strip()]
    others = [c for c in replies if not (c.get("viewerDidAuthor") or ((c.get("author") or {}).get("login") or "").startswith("clickhouse-gh"))]
    state = _thread_state(thread)
    lines = [
        f"Review finding on {repo}#{pr_number} at {thread.get('path')}:{thread.get('line') or first.get('originalLine') or '?'} "
        f"(state: {state}{', outdated' if thread.get('isOutdated') else ''}).",
        "",
        (first.get("body") or "").strip(),
    ]
    for c in replies:
        lines += ["", f"Reply by {(c.get('author') or {}).get('login')}:", (c.get("body") or "").strip()]
    tags = [f"pr:{pr_number}", f"state:{state}", "kind:review_thread"]
    if others:
        tags.append("author_replied")
    if thread.get("path"):
        tags.append(f"path:{thread['path']}")
    return {
        "memory_key": f"review-thread:{repo}:{pr_number}:{first['databaseId']}",
        "value": "\n".join(lines)[:20000],
        "memory_type": "episodic",
        "tags": tags,
        **({"files": [thread["path"]]} if thread.get("path") else {}),
    }


def record_threads(config, repo, pr_number, threads, is_ours):
    """Upsert one memory row per review thread of ours: what was found, what
    the author answered, and how the thread ended. This is the record later
    reviews can learn from (which findings authors fixed, which they
    dismissed and why). Write-only for now; nothing reads it into the prompt
    until that has been evaluated. Returns the number of rows written."""
    if not (config.available() and config.memory_namespace):
        return 0
    rows = [r for r in (thread_record(repo, pr_number, t, is_ours) for t in threads or []) if r]
    if not rows:
        return 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(8, len(rows))) as pool:
        results = list(pool.map(lambda r: call(config, "memory.set", r, namespace=config.memory_namespace), rows))
    return sum(1 for r in results if r is not None)


_STATE_TEXT = {
    "resolved_by_review": "resolved by the review, the issue no longer held",
    "resolved_by_author": "resolved by the author",
    "open": "still open at the last review",
}
_MAX_RECALLED = 15


def recall_outcomes(config, pr_number, paths):
    """Earlier review threads on the files this PR changes, from other PRs:
    what was found, what the author answered, how it ended. Returns
    (markdown, records); records carry `path`, `state`, `author_replied` and
    `finding` for the job's own filter. Empty when Loom has nothing."""
    if not (config.available() and config.memory_namespace and paths):
        return "", []

    def by_path(path):
        answer = call(config, "memory.list", {"tags": [f"path:{path}", "kind:review_thread"], "limit": 10,
                                              "preview_chars": 1500}, namespace=config.memory_namespace)
        return (answer or {}).get("entries") or []

    with concurrent.futures.ThreadPoolExecutor(max_workers=min(8, len(paths))) as pool:
        entries = [e for batch in pool.map(by_path, paths[:20]) for e in batch]
    records, seen = [], set()
    for e in sorted(entries, key=lambda e: e.get("updated_at") or "", reverse=True):
        tags = set(e.get("tags") or [])
        if e.get("memory_key") in seen or f"pr:{pr_number}" in tags:
            continue  # this PR's own threads are in threads.md
        seen.add(e.get("memory_key"))
        state = next((t.split(":", 1)[1] for t in tags if t.startswith("state:")), "")
        path = next((t.split(":", 1)[1] for t in tags if t.startswith("path:")), "")
        pr = next((t.split(":", 1)[1] for t in tags if t.startswith("pr:")), "?")
        value = e.get("value") or ""
        records.append({"path": path, "state": state, "author_replied": "author_replied" in tags, "pr": pr,
                        "finding": value.split("\n\n", 1)[1] if "\n\n" in value else value})
        if len(records) >= _MAX_RECALLED:
            break
    if not records:
        return "", []
    out = []
    for r in records:
        excerpt = " ".join(r["finding"].split())
        if len(excerpt) > 700:
            excerpt = excerpt[:700] + " ..."
        out.append(f"- `{r['path']}`, PR #{r['pr']}, {_STATE_TEXT.get(r['state'], r['state'])}"
                   f"{', the author replied' if r['author_replied'] else ''}: {excerpt}")
    return "\n".join(out) + "\n", records


# ── CLI used by the agent ─────────────────────────────────────────────────────


def _cli_body(args, config):
    overlay = {"pr_number": config.pr_number} if (config.pr_overlay and config.pr_number) else {}
    if args.command == "symbol":
        return "code.symbol", {"name": args.name, "include_body": not args.no_body, "uses_brief": args.uses,
                               "include_examples": False, "include_lessons": False, **overlay}
    if args.command == "callers":
        return "code.callers", {"name": args.name, "depth": max(1, min(args.depth, 3)), "max_nodes": args.limit, **overlay}
    if args.command == "grep":
        body = {"pattern": args.pattern, "regex": args.regex, "mode": args.mode, "limit": args.limit}
        if args.path_prefix:
            body["path_prefix"] = args.path_prefix
        return "code.grep", body
    if args.command == "search":
        body = {"query": args.query, "top_k": args.limit}
        if args.path_prefix:
            body["path_prefix"] = args.path_prefix
        return "code.search", body
    if args.command == "outline":
        return "code.outline", {"path": args.path, "token_budget": 4000}
    if args.command == "enclosing":
        return "code.enclosing", {"locations": list(args.locations)[:200], "include_body": True, "token_budget": 6000}
    if args.command == "tests-for":
        facets = [{"kind": kind, "name": name} for kind, names in (
            ("setting", args.setting), ("function", args.function), ("engine", args.engine),
            ("format", args.format), ("error_code", args.error_code)) for name in names]
        for facet in args.facet:
            kind, _, name = facet.partition("=")
            facets.append({"kind": kind, "name": name})
        if not facets:
            raise SystemExit("tests-for needs at least one facet, e.g. --setting max_block_size")
        return "code.tests_for", {"facets": facets, "closest": not args.all, "limit": args.limit}
    if args.command == "history":
        body = {"limit": args.limit}
        if args.path:
            body["path"] = args.path
        if args.name:
            body["name"] = args.name
        return "code.history", body
    if args.command == "blame":
        return "code.blame_range", {"path": args.path, "start_line": args.start, "end_line": args.end, "limit": 10}
    if args.command == "similar":
        return "tracker.similar", {"text": args.text, "top_k": args.limit}
    if args.command == "issue":
        return "tracker.item", {"numbers": [int(n) for n in args.numbers]}
    if args.command == "verify-citations":
        with open(args.file, "r", encoding="utf-8") as f:
            text = f.read()
        return "code.verify_citations", {"text": text[:60000], "limit": 200, **overlay}
    raise ValueError(args.command)


# Answer fields that only matter to Loom itself; dropped from CLI output so the
# agent does not spend tokens reading them.
_NOISE_KEYS = {"unit_id", "symbol_id", "from_symbol_id", "to_symbol_id", "trace_run_id", "tokens_returned",
               "next_cursor", "took_ms", "ignored_args", "legs", "embedding_model"}


def _compact(value):
    if isinstance(value, dict):
        return {k: _compact(v) for k, v in value.items()
                if k not in _NOISE_KEYS and v not in (None, "", [], {})}
    if isinstance(value, list):
        return [_compact(v) for v in value]
    return value


def _parser():
    p = argparse.ArgumentParser(prog="python3 -m ci.jobs.scripts.ai_review.loom",
                                description="Query the Loom code index of ClickHouse master.")
    sub = p.add_subparsers(dest="command", required=True)
    s = sub.add_parser("symbol", help="definition(s) of a function/class, with the body")
    s.add_argument("name", help="qualified name preferred, e.g. DB::MergeTreeData::loadDataParts")
    s.add_argument("--no-body", action="store_true", help="location only")
    s.add_argument("--uses", action="store_true", help="also list the main use sites")
    s = sub.add_parser("callers", help="who calls this function (call graph, overrides included)")
    s.add_argument("name")
    s.add_argument("--depth", type=int, default=1)
    s.add_argument("--limit", type=int, default=60)
    s = sub.add_parser("grep", help="literal or regex search over master")
    s.add_argument("pattern")
    s.add_argument("--regex", action="store_true")
    s.add_argument("--mode", choices=["matches", "files"], default="matches")
    s.add_argument("--path-prefix", default="")
    s.add_argument("--limit", type=int, default=40)
    s = sub.add_parser("search", help="semantic search over code, e.g. 'where are TTL moves scheduled'")
    s.add_argument("query")
    s.add_argument("--path-prefix", default="")
    s.add_argument("--limit", type=int, default=8)
    s = sub.add_parser("outline", help="the functions/classes in a file, with line ranges")
    s.add_argument("path")
    s = sub.add_parser("enclosing", help="the function around each path:line, with its body")
    s.add_argument("locations", nargs="+", metavar="PATH:LINE")
    s = sub.add_parser("tests-for", help="SQL tests that use these settings/functions/engines/formats/error codes")
    s.add_argument("--setting", action="append", default=[])
    s.add_argument("--function", action="append", default=[], help="SQL function name")
    s.add_argument("--engine", action="append", default=[])
    s.add_argument("--format", action="append", default=[])
    s.add_argument("--error-code", action="append", default=[])
    s.add_argument("--facet", action="append", default=[], metavar="KIND=NAME", help="any other facet kind")
    s.add_argument("--all", action="store_true", help="only tests carrying every facet (default: closest first)")
    s.add_argument("--limit", type=int, default=20)
    s = sub.add_parser("history", help="PRs and issues that touched a path or function")
    s.add_argument("--path", default="")
    s.add_argument("--name", default="")
    s.add_argument("--limit", type=int, default=15)
    s = sub.add_parser("blame", help="which PRs last changed these lines, with their review discussion")
    s.add_argument("path")
    s.add_argument("start", type=int)
    s.add_argument("end", type=int)
    s = sub.add_parser("similar", help="issues/PRs similar to a description")
    s.add_argument("text")
    s.add_argument("--limit", type=int, default=8)
    s = sub.add_parser("issue", help="tracker items by number")
    s.add_argument("numbers", nargs="+")
    s = sub.add_parser("verify-citations", help="check every file:line and name cited in a Markdown file")
    s.add_argument("file")
    return p


def main(argv=None):
    args = _parser().parse_args(argv)
    config = Config.from_env()
    if not config.available():
        print("Loom is not available in this run. Use `git grep` and read files from the checkout.")
        return 0
    op, body = _cli_body(args, config)
    data = call(config, op, body)
    if data is None:
        print(f"Loom did not answer {op} (timeout or error). Use `git grep` and read files from the checkout.")
        return 0
    print(json.dumps(_compact(data), indent=1, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
