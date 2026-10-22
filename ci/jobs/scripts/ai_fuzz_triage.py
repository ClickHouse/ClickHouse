"""
AI triage for fuzzer/stress job failures.

After a fuzz-style job (AST Fuzzer, BuzzHouse, Stress test) fails, an AI agent
inspects the failure evidence and judges it, and this script files or references
a tracking issue. A failure that an open, independent issue already tracks (see
below) is linked to it with no agent run at all. A second, separately budgeted
agent run reduces a minimal reproducer, only for a `related` or `uncertain`
verdict or an issue that has no reproducer yet. Behaviour depends on the run kind:

PR runs - the agent judges whether the failure was introduced by the PR:
  - `related`   -> the job stays red; the agent's reasoning and (best-effort)
                   minimal reproducer are appended to the job report so the
                   author can fix it.
  - `unrelated` -> the failure gets a GitHub issue (found or created), and the
                   job is downgraded to OK with a link to the issue.
  - `uncertain` -> treated as `related`: the job stays red and a human decides.

Post-merge runs (master / release branches) - the job status is never changed;
the failure always gets a tracking issue, attributed to the merged PR when the
agent can name the culprit commit (`Caused by: <PR>` in the issue), and the job
report gets the issue link plus reproducer. Every other stable signature in the
run gets its own issue too (`other_signatures`); a failure with none is left to
the report, which says it is not tracked.

The agent reads logs a PR controls, so it runs with no credentials: no GitHub
token (the one `actions/checkout` leaves in the git config is unset first,
`_drop_checkout_credentials`), an OpenAI key only behind a local proxy
(`_ModelProxy`), and a sandbox with no network that can write only `TRIAGE_DIR`.
It returns JSON drafts; every GitHub read or write, and all deduplication,
happens here in Python.

Fail-close: the downgrade to OK happens only on PR runs, on an explicit
`unrelated` verdict whose GitHub issue is verified to be open in this repository
and to carry the failure's fingerprint and the `fuzz` label, and only when every failure shares the
stable signature (a stack trace ID) that fingerprint is built from; a generic
name such as `Server died` gets no issue at all. Any triage problem
(agent CLI missing, secrets unavailable, malformed verdict, timeout) leaves the
job status exactly as the fuzzer set it and only appends a note that triage
was unavailable.

Relatedness never rests on a record this triage could have written itself: only
an open issue recording a post-merge run on the tested branch before the tested
change counts as proof that the failure is older (`independent_occurrences`). A
fingerprint found only in closed issues is a fixed bug that came back, which the
PR may have reintroduced, so it keeps a PR run red (`fixed_occurrences`), and so
does a lookup of those issues that did not complete.

PRs authored by an agent that already triages its own failures are skipped
(`SELF_TRIAGING_AUTHORS`); their post-merge runs are not.

The agent is the `codex` CLI; unlike `copilot_review_job.py` there is no
`copilot` backend, since Copilot authenticates with the GitHub token itself.
Set `AI_FUZZ_TRIAGE=0` to disable triage entirely.
"""

import contextlib
import hashlib
import http.client
import http.server
import json
import os
import random
import re
import subprocess
import tempfile
import threading
import time
import traceback
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path
from typing import NamedTuple

from ci.jobs.scripts.agent import (
    AGENT_MODEL,
    ROBOT_NAMES,
    openai_api_key,
    repo_from_pr_url,
    robot_token,
)
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.s3 import S3
from ci.praktika.settings import Settings

TRIAGE_DIR = Path("./ci/tmp/ai_fuzz_triage")
VERDICT_FILE = TRIAGE_DIR / "verdict.json"
REPRO_FILE = TRIAGE_DIR / "repro.json"
AGENT_LOG = TRIAGE_DIR / "agent.log"
# Fetched for the agent, which has no GitHub access of its own.
PR_DIFF_FILE = TRIAGE_DIR / "pr.diff"
KNOWN_ISSUES_DIR = TRIAGE_DIR / "known_issues"
# The checkout has depth 1, too shallow to attribute a post-merge failure to a merge.
RECENT_HISTORY_MERGES = 30
GIT_FETCH_TIMEOUT_SEC = 300

OPENAI_API_ORIGIN = "https://api.openai.com"
# Hop-by-hop headers, and the ones the proxy sets itself.
PROXY_DROPPED_HEADERS = {
    "authorization",
    "connection",
    "content-length",
    "host",
    "keep-alive",
    "proxy-connection",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
}
# Names of the job's environment variables kept from the agent: its shell can
# read its own environment.
CREDENTIAL_ENV_RE = re.compile(
    r"KEY|TOKEN|SECRET|PASSW|CREDENTIAL|^AWS_|^GH_|^GITHUB_", re.IGNORECASE
)

MAX_ATTEMPTS = 2
# Only a fast failure (auth, network, CLI start-up) is retried; a slow one, a
# timeout above all, would just spend the budget a second time.
RETRY_IF_FASTER_THAN_SEC = 300
# Exit codes of `timeout -k`: the agent got SIGTERM, then SIGKILL.
TIMEOUT_EXIT_CODES = (124, 137)
# Default budget for all agent runs of one triage. Callers running inside tighter
# job timeouts (e.g. the 2h Upgrade check) pass a smaller `agent_timeout_sec`;
# `AI_FUZZ_TRIAGE_TIMEOUT` overrides either for operational tuning.
AGENT_TIMEOUT_SEC = 2400
# The verdict needs the diff and the logs, not a reproducer: a short, cheap run.
VERDICT_TIMEOUT_SEC = 600
VERDICT_EFFORT = "medium"
# The reproducer gets the rest of the budget, when there is enough left.
REPRO_EFFORT = "xhigh"
MIN_AGENT_RUN_SEC = 120

VERDICTS = ("related", "unrelated", "uncertain")
REPRO_STATUSES = ("reproduced", "not_reproduced", "not_attempted")
ISSUE_URL_RE = re.compile(r"^https://github\.com/[\w.-]+/[\w.-]+/issues/\d+$")
PR_URL_RE = re.compile(r"^https://github\.com/[\w.-]+/[\w.-]+/pull/\d+$")
COMMIT_SHA_RE = re.compile(r"^[0-9a-f]{7,40}$")
MAX_ISSUE_TITLE_CHARS = 200
# Opens the comment `_post_repro` adds; an issue with one needs no other reproducer.
REPRO_COMMENT_PREFIX = "Reduced reproducer, reproduced with the tested build by AI triage:"
MAX_ISSUE_BODY_CHARS = 20000

# Agents that already analyse a failing job on their own PRs. A second pass adds
# nothing and would race the first one into filing a duplicate issue. Compared
# lowercased, since GitHub logins are case-insensitive. A subset of
# `label_external_contributors.INTERNAL_BOTS`: the other bots there do no
# triage of their own and still want this one.
SELF_TRIAGING_AUTHORS = {"groeneai", "oranjeai"}

# Per-failure info is a stack trace plus context; enough to triage, small
# enough to leave room for the instructions.
MAX_FAILURE_INFO_CHARS = 4000
MAX_FAILURES_IN_PROMPT = 5

# Machine-readable dedup key carried in the issue body, on a line of its own.
FINGERPRINT_FIELD = "Fuzz fingerprint"
# The only form of it lookups trust: `_issue_text` writes it, and strips every
# other line that could pass for it from the text it takes from the agent and logs.
FINGERPRINT_LINE_RE = re.compile(
    rf"^{FINGERPRINT_FIELD}: ([0-9a-f]{{12}})\r?$", re.MULTILINE
)
# The `fuzz` label already holds thousands of issues, so listing them all and
# grepping is not an option: the lookup has to be targeted (search) plus a small
# newest-first window (REST). The window only needs to cover the concurrent jobs
# on this commit, because that is the only case search can be too stale to see.
RECENT_ISSUE_WINDOW = 100
# Parallel jobs hitting one new fingerprint must file one issue: the lookup and the
# creation run under an S3 lock. A holder that died loses it after the lease.
ISSUE_LOCK_LEASE_SEC = 600
# The holder stops writing and releasing this long before the lease ends.
ISSUE_LOCK_MARGIN_SEC = 120
ISSUE_LOCK_WAIT_SEC = 900
ISSUE_LOCK_POLL_SEC = 10
# Provenance line of an issue filed by triage of a PR run. Such an issue records
# that run's verdict, not an observation of the failure on a branch.
FOUND_WHILE_TESTING_FIELD = "Found while testing"
FOUND_WHILE_TESTING_RE = re.compile(
    rf"^[\s`>*-]*{FOUND_WHILE_TESTING_FIELD}:", re.MULTILINE
)
# The issue lines lookups parse. Only this script writes them, so logs that
# reach the agent's draft cannot plant a fingerprint of another failure.
# The fingerprint groups failures by a lossy STID (top frames, names cut at `<`), so two bugs can share
# it. This digest of the whole qualified stack is what lets an issue stand in for an agent verdict.
STACK_DIGEST_FIELD = "Fuzz stack digest"
STACK_DIGEST_LINE_RE = re.compile(
    rf"^{STACK_DIGEST_FIELD}: ([0-9a-f]{{16}})\r?$", re.MULTILINE
)
FOUND_BY_LINE_RE = re.compile(r"^Found by [^\n]*? in CI: (\S+)\r?$", re.MULTILINE)
# A post-merge run that hits an issue it did not file records its branch, so an issue first filed by a PR
# run, or on another branch, can still prove the failure exists on that branch:
# `<branch>: <UTC time> <report> [<stack digest>]`. The digest says which stack was seen, since one issue
# tracks every stack sharing its fingerprint.
OBSERVED_ON_FIELD = "Fuzz observed on"
OBSERVED_ON_LINE_RE = re.compile(
    rf"^{OBSERVED_ON_FIELD} (\S+): (\d{{4}}-\d\d-\d\dT\d\d:\d\d:\d\dZ) \S+(?: ([0-9a-f]{{16}}))?\r?$",
    re.MULTILINE,
)
MACHINE_LINE_RE = re.compile(
    rf"^[\s`>*-]*({FINGERPRINT_FIELD}|{STACK_DIGEST_FIELD}|{FOUND_WHILE_TESTING_FIELD}"
    rf"|Found by [^\n]*? in CI|{OBSERVED_ON_FIELD} [^\n:]*|Caused by|Related):[^\n]*\n?",
    re.MULTILINE | re.IGNORECASE,
)
# Frames in the stack digest, below the exception machinery like the STID, but not cut short by it.
STACK_DIGEST_MAX_FRAMES = 16

# Values that differ between two runs of the same crash and so must not reach the
# fingerprint. Order matters: addresses and UUIDs before the bare-number rule.
FINGERPRINT_VOLATILE = (
    (re.compile(r"0x[0-9a-fA-F]+"), "0xADDR"),
    (
        re.compile(
            r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}"
            r"-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b"
        ),
        "UUID",
    ),
    (re.compile(r"\b\d+\b"), "N"),
)

# Only a stack trace ID pins a failure to one crash site. Names without one are
# fallbacks shared by unrelated failures (`Server died`, `Check failed`,
# `<kind> oracle mismatch`, ...), so they must never form a fingerprint.
STID_SUFFIX = " (STID: "
STABLE_SIGNATURE_RE = re.compile(r"\(STID: \d{4}-[0-9a-f]{4}\)")


def _normalize_failure_name(name):
    # The STID is a hash, not a runtime value: the number rule would erase it.
    head, sep, stid = name.partition(STID_SUFFIX)
    for pattern, replacement in FINGERPRINT_VOLATILE:
        head = pattern.sub(replacement, head)
    return re.sub(r"\s+", " ", head).strip() + sep + stid


def _stable_signature(name):
    """The normalized failure name, or "" when it does not identify one bug."""
    if not STABLE_SIGNATURE_RE.search(name or ""):
        return ""
    return _normalize_failure_name(name)


def _tracked_signature(failures):
    """The signature the fingerprint is built from, or "" when there is none."""
    names = sorted(n for n in (_stable_signature(name) for name, _ in failures) if n)
    return names[0] if names else ""


def _qualified_frames(failure_info):
    """The `DB::` frames of the parser's `Stack trace:` section, with template arguments, without arguments."""
    _, sep, stack = (failure_info or "").rpartition("Stack trace:\n")
    if not sep:
        return []
    frames = []
    for line in stack.splitlines():
        start = line.find("DB::")
        if start == -1:
            continue
        name, depth = [], 0
        for ch in line[start:].split(" @ ")[0]:
            if ch == "(" and depth == 0:
                break
            depth += (ch == "<") - (ch == ">")
            name.append(ch)
        frames.append(" ".join("".join(name).split()))
    # Like the STID (`get_stack_trace_id`): the frames that throw are not the bug.
    for i, frame in enumerate(frames):
        if "DB::Exception" in frame:
            frames = frames[i + 1 :]
            break
    frames = [frame for frame in frames if "DB::Exception" not in frame]
    return frames[:STACK_DIGEST_MAX_FRAMES]


def stack_digest(failures):
    """Digest of the qualified stacks of the tracked failures, or "" when one has no stack."""
    stacks = {tuple(_qualified_frames(info)) for _, info in _tracked_failures(failures)}
    if not stacks or () in stacks:
        return ""
    text = "\n\n".join("\n".join(stack) for stack in sorted(stacks))
    return hashlib.sha1(text.encode("utf-8", "replace")).hexdigest()[:16]


def _issue_stack_digest(issue):
    """The digest of the stack the issue was filed for, or "" when it records none."""
    found = STACK_DIGEST_LINE_RE.findall(issue.get("body") or "")
    return found[0] if len(found) == 1 else ""


def _issue_stack_digests(issue):
    """The digests of every stack the issue records: the one it was filed for and the observed ones."""
    observed = {d for _, _, d in OBSERVED_ON_LINE_RE.findall(issue.get("body") or "")}
    return (observed | {_issue_stack_digest(issue)}) - {""}


def _records_other_stack(issue, digest):
    """Whether `issue` records stack digests, none of them `digest`: possibly another bug sharing the fingerprint."""
    digests = _issue_stack_digests(issue)
    return bool(digest) and bool(digests) and digest not in digests


def _tracked_failures(failures):
    """The failures the fingerprint is built from: the group its issue describes."""
    tracked = _tracked_signature(failures)
    return [f for f in failures if tracked and _stable_signature(f[0]) == tracked]


def _tracked_first(failures):
    """`failures` with the tracked group first, so the prompt cap never drops it."""
    tracked = _tracked_failures(failures)
    return tracked + [f for f in failures if f not in tracked]


def uncovered_failure_names(failures):
    """Names of the failures the fingerprint does not track.

    A stress run may hit two different bugs; one issue vouches only for the
    failures sharing its signature.
    """
    tracked = _tracked_signature(failures)
    return [
        name or "(unnamed)"
        for name, _ in failures
        if not tracked or _stable_signature(name) != tracked
    ]


def failure_fingerprint(failures):
    """Stable key for a failure, identical across every job that hits the same bug.

    Derived from the parser's failure name, which is already built from the
    `Format string:` when the log carries one, so it holds no runtime values.
    Empty when no failure has a stable signature: a generic name would make
    unrelated failures duplicates of each other.

    A stress run reports several failed tests and their order is not stable
    between runs, so the key is the smallest normalized name rather than the
    first one - otherwise two jobs hitting the same bug disagree.
    """
    return _signature_fingerprint(_tracked_signature(failures))


def _signature_fingerprint(signature):
    if not signature:
        return ""
    return hashlib.sha1(signature.encode("utf-8", "replace")).hexdigest()[:12]


def other_signatures(failures):
    """`{signature: failures}` for the stable signatures the fingerprint skips.

    A post-merge run files one issue for each of them too, built from the job's
    own failure text, so a second bug in the same run is tracked as well.
    """
    tracked = _tracked_signature(failures)
    groups = {}
    for name, failure_info in failures:
        signature = _stable_signature(name)
        if signature and signature != tracked:
            groups.setdefault(signature, []).append((name, failure_info))
    return dict(sorted(groups.items()))


def _failures_section(failures):
    parts = []
    for name, failure_info in failures[:MAX_FAILURES_IN_PROMPT]:
        failure_info = (failure_info or "").strip()
        if len(failure_info) > MAX_FAILURE_INFO_CHARS:
            failure_info = (
                failure_info[:MAX_FAILURE_INFO_CHARS] + "\n<... truncated ...>"
            )
        parts.append(f"### {name}\n{failure_info or '(no details parsed)'}")
    if len(failures) > MAX_FAILURES_IN_PROMPT:
        parts.append(f"(and {len(failures) - MAX_FAILURES_IN_PROMPT} more failures)")
    return "\n\n".join(parts)


NO_FINGERPRINT_NOTE = """\
- This failure has no stable signature (no stack trace ID), so there is no
  fingerprint to look up known occurrences with. A similar-looking issue is not
  evidence: judge from the code and the logs alone.
"""


def _occurrences_note(fingerprint, occurrences, change, weight, regression_hint):
    if not fingerprint:
        return NO_FINGERPRINT_NOTE
    if occurrences is None:
        return """\
- Known occurrences of this failure could not be looked up. An issue carrying
  its fingerprint may have been written by this same triage, so it is not
  evidence: do not look for one here. Judge from the code and the logs alone.
"""
    if occurrences.older and not occurrences.regression:
        # An issue in `exact` gets here only next to failures it does not cover (`known_issue_verdict`).
        issues = ""
        for url in occurrences.older:
            if url in occurrences.exact:
                stack = f"saw this stack on `{occurrences.branch}` before this {change}"
            elif url in occurrences.same_stack_later:
                stack = f"records this stack, but not seen on `{occurrences.branch}` before this {change}"
            elif url in occurrences.mismatched:
                stack = "records other stacks"
            else:
                stack = "records no stack digest"
            issues += f"  - {url}, text in `{_known_issue_file(url).resolve()}` ({stack})\n"
        return (
            f"- Possible earlier occurrences: these open issues carry this failure's\n"
            f"  fingerprint and record post-merge runs on `{occurrences.branch}`, the\n"
            f"  branch the {change} is tested against, before this {change}. The\n"
            f"  fingerprint keeps only the names of the top stack frames, cut at\n"
            f"  `<`, so two bugs can share it, and none of these is known to be this one:\n{issues}"
            f"  Compare the stack and error in each issue's text with this failure's (an\n"
            f"  optimized and a sanitizer build may inline different frames). Only if one\n"
            f"  is the same failure does it predate the {change}, and that {weight}.\n"
            f"  An issue that describes another failure is not evidence.\n"
        )
    if occurrences.fixed:
        urls = "".join(f"  - {url}\n" for url in occurrences.fixed)
        return (
            f"- This failure's fingerprint is only in closed issues: a bug that was\n"
            f"  fixed before is back, and this {change} may be what reintroduced it.\n"
            f"  A closed issue is not evidence that the failure predates the {change}.\n"
            f"  {regression_hint}\n{urls}"
            f"  Any other issue carrying the fingerprint was written by this same\n"
            f"  triage, so do not look for one here.\n"
        )
    return f"""\
- No open issue with this failure's fingerprint records a post-merge run on the
  branch it is tested against before this {change}; one seen on another
  branch does not show the failure was already on this one. Any other issue
  carrying the fingerprint was written by this same triage (an earlier run, a
  sibling job on this commit, or another PR's verdict): it is not evidence, so
  do not look for one here.
  Judge from the code and the logs alone.
"""


def _history_lines(history_available):
    if not history_available:
        return """\
- Recent history could not be fetched: the checkout holds the tested commit
  only, so no commit can be attributed. Leave `culprit_commit` empty.
"""
    return f"""\
- The checkout is at the tested commit, with its recent merges fetched. List
  them with `git log --first-parent --oneline -{RECENT_HISTORY_MERGES}`, and see what one
  merged with `git diff <sha>^ <sha>`.
- Compare the crash stack / error signature with what those merges touch.
- Confident attribution to one commit -> verdict `related`, and set
  `culprit_commit` to its full sha; this job maps it to the merged PR.
"""


def _relatedness_task(info, pr_mode, fingerprint, occurrences, history_available):
    if pr_mode:
        occurrences = _occurrences_note(
            fingerprint,
            occurrences,
            "PR",
            "is strong evidence for `unrelated`",
            "Check first whether the PR touches the code of the fix in these issues:",
        )
        return f"""\
## Task 1 - relatedness

Decide whether this failure was likely INTRODUCED BY THIS PR or already exists
in master:

- Read the PR diff: `{PR_DIFF_FILE.resolve()}`.
- Compare the crash stack / error signature with the code the PR touches. A
  failure in code the PR modifies, or plausibly reachable through its changes
  (new setting values, changed defaults, touched interpreters/storages), is
  `related`. A failure in an untouched subsystem with no plausible connection
  is `unrelated`.
{occurrences}\
- If you cannot decide, answer `uncertain`. The job then stays red and a human
  decides - never guess `unrelated` to make CI green.
"""
    occurrences = _occurrences_note(
        fingerprint,
        occurrences,
        "commit",
        "means verdict `unrelated`",
        "Look for the culprit among recent commits that touch the code of the fix:",
    )
    return f"""\
## Task 1 - attribution

This is a post-merge run on `{info.git_branch}`; there is no PR to blame.
Decide whether this failure is a REGRESSION from a recently merged change or a
pre-existing bug:

{_history_lines(history_available)}{occurrences}\
- If you cannot decide, answer `uncertain`.
"""


def _issue_task(pr_mode, fingerprint, signature, job_kind):
    if not fingerprint:
        # Without a dedup key every job would file its own issue, or point at
        # an unrelated one, so the failure is left to the job report.
        return """\
## Task 2 - issue draft (SKIP)

This failure has no stable signature, so no issue is filed for it. Leave
`issue_title` and `issue_body` empty; the job report carries your reasoning.
"""
    scope = (
        "ONLY when your verdict is `unrelated`"
        if pr_mode
        else "ALWAYS - a post-merge fuzz failure must be tracked"
    )
    return f"""\
## Task 2 - issue draft ({scope})

The fingerprint, and so the issue, tracks one failure: `{signature}`. Describe
that one, even when you judge another failure more interesting; the job files
the others itself. This job looks up the issues carrying the fingerprint and
files a new one only when none tracks it, from your draft. Fill in:

- `issue_title`: short signature, e.g. `{job_kind}: Logical error: '<assertion>' in <function>`.
- `issue_body`: markdown with what failed, the error/stack excerpt (trimmed),
  and the raw failing query from the logs (a later step adds a reduced
  reproducer when it finds one). Use full GitHub URLs when linking anything.
  The job adds the fingerprint, CI link, provenance lines and the failure as
  the job reported it itself.
"""


def _build_prompt(
    info,
    job_kind,
    failures,
    evidence_paths,
    pr_mode,
    fingerprint,
    occurrences,
    timeout_sec,
    history_available=False,
):
    report_url = _report_url(info)
    if pr_mode:
        run_kind = "on a ClickHouse pull request"
    else:
        run_kind = f"on the `{info.git_branch}` branch (post-merge run)"
    if pr_mode and fingerprint and occurrences is None:
        outcome_note = (
            "The job stays red whatever your verdict: earlier issues with this\n"
            "fingerprint could not be looked up, so a regression cannot be ruled out."
        )
    elif pr_mode and fingerprint and occurrences.regression:
        outcome_note = (
            "The job stays red whatever your verdict: a fixed bug that came back\n"
            "needs a human to rule out this PR."
        )
    elif pr_mode and fingerprint:
        outcome_note = (
            "The job downgrades to green only for verdict `unrelated`, once the issue\n"
            "it files or finds for the fingerprint is verified."
        )
    elif pr_mode:
        outcome_note = (
            "The job stays red whatever your verdict: without a fingerprint no\n"
            "issue can vouch for an `unrelated` verdict."
        )
    else:
        outcome_note = (
            "The job status is not changed by your verdict; your output and the\n"
            "issue link land in the job report."
        )

    return f"""\
You are triaging a failed `{job_kind}` CI job {run_kind}.

{_evidence_sections(_tracked_first(failures), evidence_paths)}
## Context

{_change_line(info, pr_mode)}
- CI job: `{info.job_name}`, report: {report_url or "(unavailable)"}
{_sandbox_lines()}
- Do not reduce a reproducer or replay queries: a separate step does that when
  it is needed. Read the code and the logs, and finish within {timeout_sec // 60} minutes.

{_relatedness_task(info, pr_mode, fingerprint, occurrences, history_available)}
{_issue_task(pr_mode, fingerprint, _tracked_signature(failures), job_kind)}
## Task 3 - verdict file (MANDATORY)

Write a single JSON object to `{VERDICT_FILE.resolve()}`:

{{
  "verdict": "related" | "unrelated" | "uncertain",
  "reasoning": "<= 1200 chars, plain text",
  "signature": "one-line failure signature",
  "culprit_commit": "full commit sha when attributed, else empty string",
  "issue_title": "see Task 2, else empty string",
  "issue_body": "see Task 2, else empty string"
}}

{outcome_note}
"""


def _build_repro_prompt(
    info, job_kind, failures, evidence_paths, repro_hint, pr_mode, verdict, timeout_sec
):
    signature = _tracked_signature(failures)
    # The reproducer goes to the issue of this failure, not of any other one.
    target = (
        f"\nReproduce `{signature}`, and no other failure of the job.\n"
        if signature
        else ""
    )
    return f"""\
You are reducing a minimal reproducer for a failed `{job_kind}` CI job. An
earlier step judged the failure `{verdict["verdict"]}`:
{(verdict.get("reasoning") or "(no reasoning given)").strip()[:1200]}
{target}
{_evidence_sections(_tracked_first(failures), evidence_paths)}
## Context

{_change_line(info, pr_mode)}
- CI job: `{info.job_name}`
{_sandbox_lines()}
- With no network, not even to localhost, `clickhouse server` cannot run: use
  `clickhouse local`, with any scratch files under the working directory.

## Task 1 - minimal reproducer (finish within {timeout_sec // 60} minutes)

{repro_hint}

- Extract the failing query/queries from the logs, replay them, and shrink the
  case (drop irrelevant settings, columns, and clauses) while the failure still
  reproduces.
- Set `repro_status` to `reproduced` ONLY if you observed the failure yourself
  with the final snippet. Otherwise use `not_reproduced` or `not_attempted`. A
  reproduced snippet is posted to the failure's issue by this job.

## Task 2 - result file (MANDATORY)

Write a single JSON object to `{REPRO_FILE.resolve()}`:

{{
  "repro": "SQL or shell snippet, or empty string",
  "repro_status": "reproduced" | "not_reproduced" | "not_attempted"
}}
"""


def _report_url(info):
    """This job's report at the tested sha: issues keep it, so it must not follow later pushes."""
    try:
        return info.get_job_report_url(latest=False)
    except Exception:  # noqa: BLE001 - the link is a convenience
        return ""


def _change_line(info, pr_mode):
    if pr_mode:
        return f"- PR: {info.pr_url} (head sha `{info.sha}`)"
    return f"- Commit: {info.commit_url} (sha `{info.sha}`)"


def _sandbox_lines():
    return f"""\
- The repository checkout is at `{Path.cwd()}`; relative paths in this prompt
  are relative to it. You can only read it: your working directory,
  `{TRIAGE_DIR.resolve()}`, is the one place you can write.
- You have no network and no GitHub access; everything you need is on disk.
  This job does all GitHub reads and writes itself, from the file you write."""


def _evidence_sections(failures, evidence_paths):
    evidence = "\n".join(f"- {p}" for p in evidence_paths) or "- (none collected)"
    return f"""\
## Failure reported by the job

{_failures_section(failures)}

## Evidence on disk (read as needed; directories may hold several logs)

{evidence}
"""


def _drop_stale_output(output_file):
    if output_file.exists():
        try:
            output_file.unlink()
        except OSError as e:
            print(f"WARNING: cannot remove stale {output_file}: {e}")


class _ModelProxy:
    """Local endpoint for the codex CLI that forwards to OpenAI, adding the key.

    The key stays in this process: the agent's shell can read `CODEX_HOME` and
    its environment, so neither holds it, and the sandbox cannot reach this.
    """

    def __init__(self, api_key, origin=OPENAI_API_ORIGIN):
        self.api_key = api_key
        self.origin = urllib.parse.urlsplit(origin)
        self.server = None

    def __enter__(self):
        proxy = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                proxy.forward(self)

            do_GET = do_POST

            def log_message(self, *args):
                pass

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        return f"http://127.0.0.1:{self.server.server_port}/v1"

    def __exit__(self, *exc):
        self.server.shutdown()
        self.server.server_close()

    def forward(self, request):
        body = request.rfile.read(int(request.headers.get("Content-Length") or 0))
        headers = {
            k: v
            for k, v in request.headers.items()
            if k.lower() not in PROXY_DROPPED_HEADERS
        }
        headers["Authorization"] = f"Bearer {self.api_key}"
        if self.origin.scheme == "https":
            upstream = http.client.HTTPSConnection(self.origin.netloc, timeout=900)
        else:
            upstream = http.client.HTTPConnection(self.origin.netloc, timeout=900)
        try:
            upstream.request(request.command, request.path, body=body, headers=headers)
            response = upstream.getresponse()
            request.send_response(response.status)
            for k, v in response.getheaders():
                if k.lower() not in PROXY_DROPPED_HEADERS:
                    request.send_header(k, v)
            request.send_header("Connection", "close")
            request.end_headers()
            # Passed on as it arrives: the Responses API streams server-sent events.
            while chunk := response.read1(65536):
                request.wfile.write(chunk)
                request.wfile.flush()
        except Exception as e:  # noqa: BLE001 - codex sees a failed request and retries
            print(f"WARNING: AI fuzz triage: model proxy: {type(e).__name__}: {e}")
        finally:
            upstream.close()


def _agent_env():
    return {k: v for k, v in os.environ.items() if not CREDENTIAL_ENV_RE.search(k)}


def _run_to_agent_log(command, env):
    """Run the agent command, teeing all output into AGENT_LOG."""
    with open(AGENT_LOG, "ab") as log:
        return subprocess.run(
            command,
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
        ).returncode


def _run_codex_once(prompt, timeout_sec, effort, output_file):
    _drop_stale_output(output_file)
    workdir = TRIAGE_DIR.resolve()
    tmpdir = workdir / "tmp"
    tmpdir.mkdir(exist_ok=True)
    # CODEX_HOME stays outside `workdir`, so the agent cannot rewrite its config.
    with tempfile.TemporaryDirectory(dir="./ci/tmp") as codex_home, _ModelProxy(
        openai_api_key()
    ) as base_url:
        provider = f'{{name="ci_proxy",base_url="{base_url}",wire_api="responses"}}'
        # `timeout` bounds the attempt so the job always gets to upload its artifacts.
        return _run_to_agent_log(
            [
                "timeout",
                "-k",
                "60",
                str(timeout_sec),
                "codex",
                "exec",
                "-m",
                AGENT_MODEL,
                "-c",
                f"model_reasoning_effort={effort}",
                "-c",
                "model_provider=ci_proxy",
                "-c",
                f"model_providers.ci_proxy={provider}",
                "-C",
                str(workdir),
                "-s",
                "workspace-write",
                "-c",
                "sandbox_workspace_write.network_access=false",
                "-c",
                "sandbox_workspace_write.exclude_slash_tmp=true",
                "-c",
                "sandbox_workspace_write.exclude_tmpdir_env_var=true",
                # Runs on OpenAI's side, so outside the sandbox.
                "-c",
                "web_search=disabled",
                "-c",
                "approval_policy=never",
                "--color",
                "never",
                prompt,
            ],
            {**_agent_env(), "CODEX_HOME": codex_home, "TMPDIR": str(tmpdir)},
        )


def _load_json_object(path):
    if not path.exists():
        raise ValueError(f"{path} was not written")
    with open(path, encoding="utf-8") as fh:
        value = json.load(fh)
    if not isinstance(value, dict):
        raise ValueError(f"{path} is not a JSON object")
    return value


def _text_field(value, name):
    if not isinstance(value.get(name) or "", str):
        raise ValueError(f"{name} is not a string")
    return value.get(name) or ""


def _load_verdict(path=VERDICT_FILE):
    """Parse and validate the verdict file; returns dict or raises ValueError.

    Only the fields the agent drafts are kept: the issue fields are set by
    `_track_issue`, never taken from the agent.
    """
    verdict = _load_json_object(path)
    if verdict.get("verdict") not in VERDICTS:
        raise ValueError(f"invalid verdict value: {verdict.get('verdict')!r}")
    fields = (
        "reasoning",
        "signature",
        "culprit_commit",
        "issue_title",
        "issue_body",
    )
    return {"verdict": verdict["verdict"]} | {
        name: _text_field(verdict, name) for name in fields
    }


def _load_repro(path=REPRO_FILE):
    repro = _load_json_object(path)
    if repro.get("repro_status") not in REPRO_STATUSES:
        raise ValueError(f"invalid repro_status: {repro.get('repro_status')!r}")
    return {
        "repro": _text_field(repro, "repro"),
        "repro_status": repro["repro_status"],
    }


def _run_agent(prompt, deadline, cap_sec, effort, output_file, load):
    """Run the agent and return `load(output_file)`.

    Each attempt gets `cap_sec`, cut to what is left before `deadline`
    (`time.monotonic()`). Only a fast failure is retried.
    """
    last_error = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        timeout_sec = int(min(cap_sec, deadline - time.monotonic()))
        if timeout_sec < MIN_AGENT_RUN_SEC:
            raise RuntimeError(
                f"triage budget exhausted after {attempt - 1} attempts: {last_error}"
            )
        started = time.monotonic()
        try:
            exit_code = _run_codex_once(prompt, timeout_sec, effort, output_file)
            # A valid output counts even on non-zero exit (e.g. the timeout
            # fired after the file was already written).
            try:
                output = load(output_file)
                if exit_code != 0:
                    print(
                        f"WARNING: codex exited with {exit_code}, "
                        f"but wrote a valid {output_file.name} - accepting it"
                    )
                return output
            except (ValueError, json.JSONDecodeError) as e:
                if exit_code in TIMEOUT_EXIT_CODES:
                    last_error = f"codex timed out after {timeout_sec}s"
                else:
                    last_error = f"codex exit code {exit_code}; output: {e}"
        except Exception as e:  # noqa: BLE001 - any failure here may be retried
            last_error = f"{type(e).__name__}: {e}"
            traceback.print_exc()
        print(f"WARNING: triage attempt {attempt}/{MAX_ATTEMPTS} failed: {last_error}")
        if time.monotonic() - started >= RETRY_IF_FASTER_THAN_SEC:
            raise RuntimeError(f"triage failed, not retried: {last_error}")
        if attempt < MAX_ATTEMPTS:
            time.sleep(min(2**attempt * 5, 60))
    raise RuntimeError(f"triage failed after {MAX_ATTEMPTS} attempts: {last_error}")


def _remaining_sec(deadline):
    """Seconds left before `deadline` (`time.monotonic()`); raises once none are."""
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("the triage budget is used up")
    return remaining


# `{robot name: token}`, each fetched once. Kept in memory, never in a gh config the agent could read.
_ROBOT_TOKENS = {}


def _robot_gh(*args, deadline=None):
    """Run a `gh` command as a robot and return its stdout.

    Fuzz-style jobs run without `enable_gh_auth`, so a bare `gh` in the job is
    unauthenticated. The command gets at most what is left before `deadline`.
    Raises on any failure.
    """
    timeout = 120 if deadline is None else min(120, _remaining_sec(deadline))
    robot_name = random.choice(ROBOT_NAMES)
    if robot_name not in _ROBOT_TOKENS:
        print(f"AI fuzz triage: using robot {robot_name}")
        _ROBOT_TOKENS[robot_name] = robot_token(robot_name)
    # An empty config dir, so a stale `GH_CONFIG_DIR` of the job cannot get in the way.
    with tempfile.TemporaryDirectory() as gh_config_dir:
        return subprocess.run(
            ["gh", *args],
            env={
                **os.environ,
                "GH_CONFIG_DIR": gh_config_dir,
                "GH_TOKEN": _ROBOT_TOKENS[robot_name],
            },
            capture_output=True,
            text=True,
            timeout=timeout,
            check=True,
        ).stdout


def _github_reachable():
    """Probe used only to explain a fail-close red, never to decide job status."""
    try:
        return bool(_robot_gh("api", "rate_limit", "--jq", ".rate.limit").strip())
    except Exception:  # noqa: BLE001 - a failed probe just means "cannot tell"
        return False


def _triage_repo(info):
    return repo_from_pr_url(info.pr_url) or info.repo_name


def _parse_github_time(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


class Occurrences(NamedTuple):
    """What GitHub knew about a fingerprint before the tested change."""

    older: list  # `independent_occurrences`
    fixed: list  # `fixed_occurrences`
    # Among `older`, the issues that saw this failure's stack on `branch` before the change: only they
    # stand in for the agent.
    exact: list = []
    # Among `older`, the issues that record only other stack digests: possibly another bug sharing the fingerprint.
    mismatched: list = []
    # `{url: body}` of `older`, written to disk for the agent, which cannot reach GitHub.
    bodies: dict = {}
    # The branch `older` was observed on: the PR's target, or the branch of a post-merge run.
    branch: str = ""
    # Among `older`, the issues that record this stack too, but did not see it on `branch` before the change.
    same_stack_later: list = []

    @property
    def regression(self):
        """A fixed bug is back and nothing shows it was back before the change."""
        return bool(self.fixed) and not set(self.older) - set(self.mismatched)


def fixed_occurrences(issues, fingerprint):
    """URLs of the closed issues with this fingerprint: the bug was fixed once.

    A closed issue says nothing about whether the failure predates the change;
    the change may be what brought it back. One recording another stack digest
    counts too: like `_tracking_issues`, it may be the same bug reached another way.
    """
    return sorted(
        issue["url"]
        for issue in issues
        if (issue.get("state") or "").upper() == "CLOSED"
        and _tracks_fingerprint(issue, fingerprint)
    )


def _report_branch(report_url):
    """The branch of a post-merge run's report URL (`REF=<branch>`), or "" for a PR run's or none."""
    query = urllib.parse.parse_qs(urllib.parse.urlsplit(report_url or "").query)
    return (query.get("REF") or [""])[0]


def _issue_branch(issue):
    """The branch of the run that filed the issue, from its `Found by` line, or "" if unknown.

    Only `_issue_text` writes that line, with the report URL of a post-merge run (`REF=<branch>`).
    """
    found = FOUND_BY_LINE_RE.findall(issue.get("body") or "")
    return _report_branch(found[0]) if len(found) == 1 else ""


def _branch_observations(issue, digest=None):
    """`{branch: earliest time}` of the post-merge runs that hit the issue's failure.

    The run that filed it counts unless it was a PR run, which records that run's verdict
    rather than an observation; the runs that reused it later add `OBSERVED_ON_FIELD` lines.
    With `digest`, only the runs that hit that stack count.
    """
    body = issue.get("body") or ""
    runs = []
    filed_on = _issue_branch(issue)
    if filed_on and not FOUND_WHILE_TESTING_RE.search(body):
        runs.append(
            (filed_on, _parse_github_time(issue["createdAt"]), _issue_stack_digest(issue))
        )
    for branch, when, seen in OBSERVED_ON_LINE_RE.findall(body):
        runs.append((branch, _parse_github_time(when), seen))
    observed = {}
    for branch, when, seen in runs:
        if digest is not None and seen != digest:
            continue
        if branch not in observed or when < observed[branch]:
            observed[branch] = when
    return observed


def _has_fuzz_label(issue):
    return "fuzz" in [label.get("name", "") for label in (issue.get("labels") or [])]


def _tracks_fingerprint(issue, fingerprint):
    """Whether the issue's one `FINGERPRINT_LINE_RE` line names `fingerprint`.

    The fingerprint quoted anywhere else, e.g. in a query, does not count, and
    neither does a body with two such lines: it cannot say which one it tracks.
    """
    found = FINGERPRINT_LINE_RE.findall(issue.get("body") or "")
    return bool(fingerprint) and found == [fingerprint]


def independent_occurrences(issues, fingerprint, cutoff, branch):
    """URLs of the open issues that prove the failure predates the tested change.

    Only a record this triage cannot have written counts: a post-merge run's
    observation (`_branch_observations`) made before the change existed. A PR run's
    issue holds that run's verdict rather than an observation on a branch, and a
    later observation could come from an earlier run of this change or a sibling job
    on the same commit, which would confirm its own verdict.

    It must have been observed on `branch`, the one the change goes to: a bug seen
    on master is new to a release branch that a backport brings it to.

    It must also be a tracking issue `_verify_issue_marker` accepts, `fuzz` label
    included: linking one that is not would skip the agent that files a real one.
    """
    urls = []
    for issue in issues:
        observed = _branch_observations(issue) if branch else {}
        if (
            (issue.get("state") or "").upper() == "OPEN"
            and _tracks_fingerprint(issue, fingerprint)
            and _has_fuzz_label(issue)
            and branch in observed
            and observed[branch] < cutoff
        ):
            urls.append(issue["url"])
    return sorted(urls)


def _prior_occurrences(info, repo_name, pr_mode, fingerprint, digest):
    """`Occurrences` from GitHub, or None when they cannot be read.

    The cutoff is when the change came to exist: the PR's opening, or the
    tested commit for a post-merge run.
    """
    try:
        if pr_mode:
            path, field = f"repos/{repo_name}/pulls/{info.pr_number}", ".created_at"
        else:
            path, field = (
                f"repos/{repo_name}/commits/{info.sha}",
                ".commit.committer.date",
            )
        cutoff = _parse_github_time(_robot_gh("api", path, "--jq", field).strip())
        # Index lag does not matter here: only issues older than the change count.
        # The bare token, since `gh` mangles a quoted phrase with a colon.
        output = _robot_gh(
            "search",
            "issues",
            fingerprint,
            "--repo",
            repo_name,
            "--limit",
            "30",
            "--json",
            "url,state,createdAt,body,labels",
        )
        issues = json.loads(output or "[]")
        branch = info.base_branch if pr_mode else info.git_branch
        older = independent_occurrences(issues, fingerprint, cutoff, branch)
        older_issues = [issue for issue in issues if issue["url"] in older]
        # This stack itself must have been seen on the branch before the change, not just the issue.
        exact = sorted(
            issue["url"]
            for issue in older_issues
            if digest
            and _branch_observations(issue, digest).get(branch, cutoff) < cutoff
        )
        same_stack_later = sorted(
            issue["url"]
            for issue in older_issues
            if digest
            and issue["url"] not in exact
            and digest in _issue_stack_digests(issue)
        )
        mismatched = sorted(
            issue["url"]
            for issue in older_issues
            if _records_other_stack(issue, digest)
        )
        bodies = {issue["url"]: issue.get("body") or "" for issue in older_issues}
        return Occurrences(
            older,
            fixed_occurrences(issues, fingerprint),
            exact,
            mismatched,
            bodies,
            branch,
            same_stack_later,
        )
    except Exception as e:  # noqa: BLE001 - no lookup means no evidence, never more
        print(f"WARNING: AI fuzz triage: cannot look up prior occurrences: {e}")
        return None


def _known_issue_file(issue_url):
    return KNOWN_ISSUES_DIR / f"{issue_url.rstrip('/').rsplit('/', 1)[-1]}.md"


def _write_known_issues(occurrences):
    """Put the text of each `older` issue where the agent can compare its stack."""
    if not occurrences or not occurrences.older:
        return
    KNOWN_ISSUES_DIR.mkdir(parents=True, exist_ok=True)
    for url in occurrences.older:
        _known_issue_file(url).write_text(
            f"{url}\n\n{occurrences.bodies.get(url, '')}", encoding="utf-8"
        )


def _fetch_recent_history(sha, deadline):
    """Deepen the checkout to the recent merges for post-merge attribution; False if that failed."""
    if not COMMIT_SHA_RE.match(sha or ""):
        return False
    # At most a quarter of what is left, so the agents keep most of the budget.
    timeout_sec = min(GIT_FETCH_TIMEOUT_SEC, (deadline - time.monotonic()) / 4)
    if timeout_sec <= 0:
        return False
    try:
        subprocess.run(
            [
                "git",
                "fetch",
                "--no-tags",
                "--quiet",
                f"--depth={RECENT_HISTORY_MERGES + 1}",
                "origin",
                sha,
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=timeout_sec,
        )
        return True
    except (OSError, subprocess.SubprocessError) as e:
        print(f"WARNING: AI fuzz triage: cannot fetch recent history: {e}")
        return False


def _checkout_credential_headers():
    """`(origin, key)` of the HTTP extra headers in the checkout's git config, includes followed.

    `actions/checkout` keeps the job's token in one, in `.git/config` or (v6) a file it includes.
    """
    listed = subprocess.run(
        [
            "git",
            "config",
            "--local",
            "--includes",
            "--show-origin",
            "--get-regexp",
            r"^http\..*\.extraheader$",
        ],
        capture_output=True,
        text=True,
        timeout=60,
    )
    if listed.returncode == 1:  # No such key.
        return []
    listed.check_returncode()
    headers = []
    for line in listed.stdout.splitlines():
        origin, _, entry = line.partition("\t")
        headers.append((origin, entry.split(" ", 1)[0]))
    return headers


def _drop_checkout_credentials():
    """Unset the job token `actions/checkout` left in the git config before any agent run.

    The agent can read every file, and what it drafts is published. Raises if one is left.
    """
    for origin, key in _checkout_credential_headers():
        if origin.startswith("file:"):
            subprocess.run(
                ["git", "config", "--file", origin[len("file:") :], "--unset-all", key],
                capture_output=True,
                timeout=60,
            )
    left = _checkout_credential_headers()
    if left:
        raise RuntimeError(
            f"cannot unset the git credentials in {sorted({o for o, _ in left})}"
        )


def _verify_issue_marker(issue_url, fingerprint, repo_name):
    """Check the issue is open in `repo_name` with the dedup marker and `fuzz` label.

    Taking the agent's word for this is how duplicates get back in: an issue
    missing the marker line, or missing the label the lookup filters on, is
    invisible to the next job that hits the same bug. Verifying also catches the
    other failure modes - referencing a similar-looking issue that has a
    different fingerprint, or a closed one, which means the bug regressed and
    needs a new issue.

    Returns `(verified, problems)`; `verified` is False when the check itself
    could not run, which must not be reported as a missing marker.
    """
    if not issue_url or not fingerprint:
        return False, []
    match = re.match(r"^https://github\.com/([\w.-]+/[\w.-]+)/issues/(\d+)$", issue_url)
    if not match:
        return False, [f"AI triage: cannot verify {issue_url}: unrecognized issue URL"]
    repo, number = match.group(1), match.group(2)
    if not repo_name:
        return False, [f"AI triage: cannot verify {issue_url}: repository unknown"]
    if repo.lower() != repo_name.lower():
        return True, [
            f"AI triage WARNING: {issue_url} is not in `{repo_name}`, where "
            f"this failure is tracked"
        ]
    try:
        output = _robot_gh(
            "issue", "view", number, "--repo", repo, "--json", "body,labels,state"
        )
        issue = json.loads(output) if output else None
    except Exception as e:  # noqa: BLE001 - never lose a valid verdict over this
        return False, [f"AI triage: could not verify {issue_url} ({e})"]
    if not isinstance(issue, dict):
        # Auth, rate limit or network - unknown, not proven wrong.
        return False, [f"AI triage: could not verify the dedup marker on {issue_url}"]

    problems = []
    if issue.get("state") != "OPEN":
        problems.append(
            f"AI triage WARNING: {issue_url} is not open, so the bug regressed "
            f"and needs a new issue carrying this fingerprint"
        )
    if not _tracks_fingerprint(issue, fingerprint):
        problems.append(
            f"AI triage WARNING: {issue_url} does not carry "
            f"`{FINGERPRINT_FIELD}: {fingerprint}` as its only fingerprint line - "
            f"the next job hitting this bug will file a duplicate; fix the line by hand"
        )
    if not _has_fuzz_label(issue):
        problems.append(
            f"AI triage WARNING: {issue_url} has no `fuzz` label, so the "
            f"fingerprint lookup cannot reach it - label it by hand"
        )
    return True, problems


def _write_pr_diff(info, repo_name):
    """The PR diff at the tested head, not the live PR.

    A commit pushed while the job ran, e.g. one removing the faulty change, must
    not make the failing code look untouched by the PR.
    """
    if not info.base_branch or not COMMIT_SHA_RE.match(info.sha or ""):
        raise ValueError(
            f"no base branch or tested sha: {info.base_branch!r}, {info.sha!r}"
        )
    compare = f"repos/{repo_name}/compare/{info.base_branch}...{info.sha}"
    try:
        diff = _robot_gh("api", compare, "-H", "Accept: application/vnd.github.diff")
    except subprocess.CalledProcessError as e:
        # E.g. a diff too large for the API: the paths still narrow it down.
        print(f"WARNING: AI fuzz triage: cannot fetch the PR diff: {e.stderr}")
        files = _robot_gh("api", compare, "--jq", ".files[].filename")
        diff = f"(the diff could not be fetched; the PR changes these files)\n{files}"
    PR_DIFF_FILE.write_text(diff, encoding="utf-8")


def _culprit_pr(repo_name, commit, deadline):
    """The PR that merged `commit`, or "" when there is none or it cannot be read."""
    if not COMMIT_SHA_RE.match(commit):
        return ""
    try:
        url = _robot_gh(
            "api",
            f"repos/{repo_name}/commits/{commit}/pulls",
            "--jq",
            ".[0].html_url // empty",
            deadline=deadline,
        ).strip()
    except Exception as e:  # noqa: BLE001 - the attribution is a convenience
        print(f"WARNING: AI fuzz triage: cannot map {commit} to its PR: {e}")
        return ""
    return url if PR_URL_RE.match(url) else ""


def _fingerprint_issues(repo_name, fingerprint, deadline):
    """The issues carrying the fingerprint marker, oldest first.

    It also finds issues this triage filed itself, so it must not feed the
    relatedness judgment; `_prior_occurrences` does that. Two queries, because
    neither alone is enough: search reaches the whole (thousands-strong) `fuzz`
    backlog but its index lags minutes behind, and the REST list is current but
    bounded to the newest issues - where one a sibling job just filed is.
    """
    fields = "number,url,state,body,labels"
    issues = json.loads(
        _robot_gh(
            "search",
            "issues",
            fingerprint,
            "--repo",
            repo_name,
            "--limit",
            "30",
            "--json",
            fields,
            deadline=deadline,
        )
        or "[]"
    )
    issues += json.loads(
        _robot_gh(
            "issue",
            "list",
            "--repo",
            repo_name,
            "--label",
            "fuzz",
            "--state",
            "all",
            "--limit",
            str(RECENT_ISSUE_WINDOW),
            "--json",
            fields,
            deadline=deadline,
        )
        or "[]"
    )
    by_url = {i["url"]: i for i in issues if _tracks_fingerprint(i, fingerprint)}
    return sorted(by_url.values(), key=lambda i: i["number"])


def _issue_comments(issue_url, deadline):
    """The bodies of the comments on `issue_url`."""
    output = _robot_gh("issue", "view", issue_url, "--json", "comments", deadline=deadline)
    return [c.get("body") or "" for c in json.loads(output).get("comments") or []]


def _has_repro(comments):
    return any(body.startswith(REPRO_COMMENT_PREFIX) for body in comments)


def _comment_once(repo_name, issue, job_kind, found_in, deadline):
    """Link this job from `issue`: one comment per job kind, not one per job.

    Returns the comments the issue had before.
    """
    comments = _issue_comments(issue["url"], deadline)
    # Only this job's own comment: any other may name the job kind in passing.
    prefix = f"Also hit by {job_kind} in CI: "
    if any(body.startswith(prefix) for body in comments):
        return comments
    _robot_gh(
        "issue",
        "comment",
        str(issue["number"]),
        "--repo",
        repo_name,
        "--body",
        f"{prefix}{found_in}",
        deadline=deadline,
    )
    return comments


def _record_observation(repo_name, issue, found_in, digest, deadline):
    """Add this post-merge run's branch and stack to `issue`, unless an earlier observation has both.

    The body is read again rather than taken from the lookup, whose search results can lag
    behind an earlier edit. Runs under `_issue_lock`, so no other job edits it meanwhile.
    """
    branch = _report_branch(found_in)
    if not branch:
        return
    output = _robot_gh(
        "issue",
        "view",
        str(issue["number"]),
        "--repo",
        repo_name,
        "--json",
        "body,createdAt",
        deadline=deadline,
    )
    current = json.loads(output)
    if branch in _branch_observations(current, digest or None):
        return
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    line = f"{OBSERVED_ON_FIELD} {branch}: {now} {found_in} {digest}".rstrip()
    body = f"{(current.get('body') or '').rstrip()}\n{line}"
    _robot_gh(
        "issue",
        "edit",
        str(issue["number"]),
        "--repo",
        repo_name,
        "--body",
        body,
        deadline=deadline,
    )


def _issue_text(
    verdict, failures, *, job_kind, fingerprint, found_in, tested_pr, related
):
    """Title and body of a new issue: the agent's draft plus the lines lookups parse.

    `failures` is the group the fingerprint tracks. The body always carries the
    job's own text for it, so it holds the right diagnostics whatever the draft says.
    """
    # Not the agent's `signature`: it may name another failure of the job.
    title = (
        " ".join(verdict["issue_title"].split())
        or f"{job_kind}: {_tracked_signature(failures)}"
    )
    draft = verdict["issue_body"].strip()[:MAX_ISSUE_BODY_CHARS]
    reported = _failures_section(failures)
    if draft:
        reported = (
            f"<details><summary>Failure as reported by the job</summary>\n\n"
            f"{reported}\n\n</details>"
        )
    text = "\n\n".join(part for part in (draft, reported) if part)
    digest = stack_digest(failures)
    lines = [
        # Agent drafts and logs may quote any fingerprint; only the lines below are ours.
        MACHINE_LINE_RE.sub("", text).strip(),
        "",
        f"{FINGERPRINT_FIELD}: {fingerprint}",
    ]
    if digest:
        lines.append(f"{STACK_DIGEST_FIELD}: {digest}")
    lines.append(f"Found by {job_kind} in CI: {found_in}")
    if tested_pr:
        lines.append(f"{FOUND_WHILE_TESTING_FIELD}: {tested_pr}")
        lines.append("AI triage judged the failure unrelated to that PR.")
    if verdict.get("culprit_pr"):
        lines.append(f"Caused by: {verdict['culprit_pr']}")
    if related:
        lines.append(f"Related: {related}")
    return title[:MAX_ISSUE_TITLE_CHARS], "\n".join(lines)


def _lock_age_sec(head):
    modified = datetime.strptime(head.LastModified, "%a, %d %b %Y %H:%M:%S GMT")
    return (
        datetime.now(timezone.utc) - modified.replace(tzinfo=timezone.utc)
    ).total_seconds()


@contextlib.contextmanager
def _issue_lock(repo_name, fingerprint, deadline):
    """Hold the lock on filing the issue for `fingerprint`, shared by all CI runners.

    The lock is an S3 object created with `If-None-Match: *`, so one job holds it at
    a time. One left by a holder that died is taken over with `If-Match` once it is
    older than `ISSUE_LOCK_LEASE_SEC`. Waiting stops at `deadline` at the latest.
    Yields the deadline of the work done under the lock: `deadline`, or the end of
    the lease minus `ISSUE_LOCK_MARGIN_SEC` if that comes first.
    """
    if not Settings.CACHE_S3_PATH:
        raise RuntimeError("no CACHE_S3_PATH for the issue lock")
    path = (
        f"{Settings.CACHE_S3_PATH}/ai_fuzz_triage/issue_locks/{repo_name}/{fingerprint}"
    )
    wait_until = min(time.monotonic() + ISSUE_LOCK_WAIT_SEC, deadline)
    with tempfile.NamedTemporaryFile("w", suffix=".lock") as token:
        token.write(f"{datetime.now(timezone.utc).isoformat()}\n")
        token.flush()
        while True:
            # Sent no later than S3 stamps `LastModified`, which other jobs time the lease from.
            sent = time.monotonic()
            if S3.put(
                s3_path=path, local_path=token.name, text=True, if_none_matched=True
            ):
                break
            head = S3.head_object(path)
            if head and _lock_age_sec(head) > ISSUE_LOCK_LEASE_SEC:
                sent = time.monotonic()
                if S3.put(
                    s3_path=path,
                    local_path=token.name,
                    text=True,
                    if_match=head.ETag,
                    no_strict=True,
                ):
                    print(f"AI fuzz triage: took over the expired issue lock {path}")
                    break
            if time.monotonic() > wait_until:
                raise TimeoutError(f"issue lock {path} still held")
            if head:  # Otherwise it was released meanwhile: retry at once.
                time.sleep(
                    max(0, min(ISSUE_LOCK_POLL_SEC, wait_until - time.monotonic()))
                )
    # Ours until this point, with a margin for clock skew between runners: nobody can take it over before.
    owned_until = sent + ISSUE_LOCK_LEASE_SEC - ISSUE_LOCK_MARGIN_SEC
    head = S3.head_object(path)
    etag = head.ETag if head else None
    try:
        yield min(deadline, owned_until)
    finally:
        # Only this job's instance, and only while nobody can have taken it over: past that it is left to expire.
        if etag and time.monotonic() < owned_until:
            head = S3.head_object(path)
            if head and head.ETag == etag:
                S3.delete(path)


def _track_issue(
    repo_name, fingerprint, verdict, failures, job_kind, found_in, pr_url, deadline
):
    """Find or file the issue tracking `fingerprint`; returns the verdict's issue fields.

    The lookup and the creation run under `_issue_lock`, so two jobs that hit the
    same new fingerprint at once file one issue, and the second one comments on it.
    All of it stops at `deadline`, the end of the triage budget.
    """
    _remaining_sec(deadline)
    with _issue_lock(repo_name, fingerprint, deadline) as locked_deadline:
        # Bounded by the lease too: no write may land after another job can take the lock over.
        return _find_or_file_issue(
            repo_name,
            fingerprint,
            verdict,
            failures,
            job_kind,
            found_in,
            pr_url,
            locked_deadline,
        )


def _stack_rank(issue, digest):
    """0 for an issue recording `digest`, 1 for one recording none, 2 for one recording another."""
    if digest and _issue_stack_digest(issue) == digest:
        return 0
    return 2 if _records_other_stack(issue, digest) else 1


def _tracking_issues(issues, digest):
    """The open issues with the `fuzz` label among `_fingerprint_issues`, best stack match first.

    The digest covers the call path and the build's inlining, so one recording another digest may
    be the same bug reached another way: it still tracks the failure, after the closer matches.
    Oldest first within each `_stack_rank`.
    """
    candidates = [
        i
        for i in issues
        if (i.get("state") or "").upper() == "OPEN" and _has_fuzz_label(i)
    ]
    return sorted(candidates, key=lambda i: _stack_rank(i, digest))


def _find_or_file_issue(
    repo_name, fingerprint, verdict, failures, job_kind, found_in, pr_url, deadline
):
    """An open issue with the `fuzz` label tracks `fingerprint`, whatever stack it records.

    A closed one is a fixed bug that came back, and an unlabelled one is out of the
    lookup's reach, so either gets a comment and a new issue that links it.
    `pr_url` is empty on post-merge runs.
    """
    digest = stack_digest(failures)
    issues = _fingerprint_issues(repo_name, fingerprint, deadline)
    tracking = _tracking_issues(issues, digest)
    if tracking:
        comments = _comment_once(repo_name, tracking[0], job_kind, found_in, deadline)
        if not pr_url:
            try:
                _record_observation(repo_name, tracking[0], found_in, digest, deadline)
            except Exception as e:  # noqa: BLE001 - only later lookups lose this evidence
                print(f"WARNING: AI fuzz triage: cannot record the observation: {e}")
        return {
            "issue_url": tracking[0]["url"],
            "issue_created": False,
            "label_applied": True,
            "issue_has_repro": _has_repro(comments),
        }
    # The newest of the closest stack match.
    previous = min(
        issues, key=lambda i: (_stack_rank(i, digest), -i["number"]), default=None
    )
    related = previous["url"] if previous else ""
    if previous:
        _comment_once(repo_name, previous, job_kind, found_in, deadline)
    title, body = _issue_text(
        verdict,
        failures,
        job_kind=job_kind,
        fingerprint=fingerprint,
        found_in=found_in,
        tested_pr=pr_url,
        related=related,
    )
    create = ("issue", "create", "--repo", repo_name, "--title", title, "--body", body)
    label_applied = True
    try:
        output = _robot_gh(*create, "--label", "fuzz", deadline=deadline)
    except subprocess.CalledProcessError as e:
        print(f"WARNING: AI fuzz triage: issue create with `fuzz` failed: {e.stderr}")
        # GitHub may have filed it and only the response failed: look again first.
        tracking = _tracking_issues(
            _fingerprint_issues(repo_name, fingerprint, deadline), digest
        )
        if tracking:
            return {
                "issue_url": tracking[0]["url"],
                "issue_created": True,
                "label_applied": True,
            }
        # Only a refused label is worth an unlabelled issue; the report flags the gap.
        if "label" not in (e.stderr or "").lower():
            raise
        output = _robot_gh(*create, deadline=deadline)
        label_applied = False
    issue_url = (output.strip().splitlines() or [""])[-1]
    if not ISSUE_URL_RE.match(issue_url):
        raise ValueError(f"unexpected `gh issue create` output: {output!r}")
    return {
        "issue_url": issue_url,
        "issue_created": True,
        "label_applied": label_applied,
    }


def _track_other_signatures(repo_name, failures, job_kind, found_in, deadline):
    """`_track_issue` for each of `other_signatures`; a failure only skips its own."""
    tracked = []
    for signature, group in other_signatures(failures).items():
        fingerprint = _signature_fingerprint(signature)
        other = {"signature": signature, "fingerprint": fingerprint}
        draft = {"signature": signature, "issue_title": "", "issue_body": ""}
        try:
            other |= _track_issue(
                repo_name, fingerprint, draft, group, job_kind, found_in, "", deadline
            )
        except Exception as e:  # noqa: BLE001 - the report says it is untracked
            print(
                f"WARNING: AI fuzz triage: cannot file the issue for {signature}: {e}"
            )
            other["issue_error"] = f"{type(e).__name__}: {e}"
        tracked.append(other)
    return tracked


def _post_repro(issue_url, repro, deadline):
    """Post `repro` to `issue_url`, unless a sibling job posted one meanwhile."""
    if _has_repro(_issue_comments(issue_url, deadline)):
        return
    fence = "````" if "```" in repro else "```"
    _robot_gh(
        "issue",
        "comment",
        issue_url,
        "--body",
        f"{REPRO_COMMENT_PREFIX}\n\n"
        f"{fence}\n{repro[:MAX_ISSUE_BODY_CHARS]}\n{fence}\n",
        deadline=deadline,
    )


def _apply_verdict(
    result,
    verdict,
    pr_mode,
    fingerprint="",
    uncovered=(),
    repo_name="",
    regression=False,
    occurrences_checked=False,
):
    kind = verdict["verdict"]
    reasoning = (verdict.get("reasoning") or "").strip()[:1200]
    signature = (verdict.get("signature") or "").strip()
    issue_url = verdict.get("issue_url") or ""
    culprit_pr = (verdict.get("culprit_pr") or "").strip()
    repro = (verdict.get("repro") or "").strip()
    repro_status = verdict.get("repro_status") or "not_attempted"
    issue_note = " (new issue)" if verdict.get("issue_created") else " (existing issue)"

    # The description always carries the failure and the reproducer state, no
    # matter the verdict; the issue link joins them whenever one exists.
    lines = []
    if signature:
        lines.append(f"AI triage failure: {signature}")
    if fingerprint:
        lines.append(f"AI triage fingerprint: {fingerprint}")
    # Check the issue itself rather than trusting the verdict; fall back to the
    # agent's own claim only when the check could not run. A missing key counts as
    # not applied: silence must not read as success.
    verified, marker_problems = _verify_issue_marker(issue_url, fingerprint, repo_name)
    lines.extend(marker_problems)
    if verdict.get("issue_error"):
        lines.append(f"AI triage: filing the issue failed ({verdict['issue_error']})")
    if (
        not verified
        and verdict.get("issue_created")
        and not verdict.get("label_applied", False)
    ):
        lines.append(
            "AI triage WARNING: the issue was reported as created without the "
            "`fuzz` label, so fingerprint dedup will not find it - label it by hand"
        )
    # The issue tracks the fingerprinted failure only; going green on it would
    # also hide every failure that fingerprint does not cover.
    untracked = not fingerprint or bool(uncovered)
    if pr_mode and kind == "unrelated" and untracked:
        names = ", ".join(f"`{name}`" for name in uncovered) or "the failure"
        lines.append(
            f"AI triage: verdict was `unrelated`, but {names} is not covered by "
            f"the issue's fingerprint (no stack trace ID, or a different bug) - "
            f"keeping the job red (fail-close)"
        )
        if issue_url:
            lines.append(f"AI triage: related tracking issue: {issue_url}{issue_note}")
    elif pr_mode and kind == "unrelated" and regression:
        # The new issue Task 2 files for the regression would vouch for the very
        # verdict in doubt.
        lines.append(
            "AI triage: verdict was `unrelated`, but this failure's fingerprint is "
            "only in closed issues: a fixed bug came back, and this PR may have "
            "reintroduced it - keeping the job red (fail-close)"
        )
        if issue_url:
            lines.append(f"AI triage: related tracking issue: {issue_url}{issue_note}")
    elif pr_mode and kind == "unrelated" and not occurrences_checked:
        # Without the lookup a fixed bug that came back looks like a new one.
        lines.append(
            "AI triage: verdict was `unrelated`, but the lookup of earlier issues "
            "with this fingerprint did not complete, so a fixed bug that came back "
            "cannot be ruled out - keeping the job red (fail-close); re-run the job "
            "once GitHub is reachable"
        )
        if issue_url:
            lines.append(f"AI triage: related tracking issue: {issue_url}{issue_note}")
    elif pr_mode and kind == "unrelated" and issue_url and not verified:
        # A timed-out or partial agent run may name any issue; only the issue
        # itself can vouch for the verdict.
        lines.append(
            f"AI triage: verdict was `unrelated`, but {issue_url} could not be "
            f"checked for this failure's fingerprint - keeping the job red "
            f"(fail-close); re-run the job once GitHub is reachable"
        )
    elif pr_mode and kind == "unrelated" and issue_url and marker_problems:
        lines.append(
            f"AI triage: verdict was `unrelated`, but {issue_url} is not an open "
            f"issue tracking this failure's fingerprint - keeping the job red "
            f"(fail-close)"
        )
    elif pr_mode and kind == "unrelated" and issue_url:
        # The only path that turns the job green, mirroring the established
        # OOM-downgrade pattern: overall status flips, sub-results stay as-is.
        result.set_status(Result.Status.OK)
        # The verdict rests on the PR and the issue's state, which no digest input captures:
        # a later run with the same digest must triage again, not reuse this as a success.
        result.ext["do_not_cache"] = True
        lines.append(
            f"AI triage: failure judged UNRELATED to this PR - tracked in "
            f"{issue_url}{issue_note}"
        )
    elif pr_mode and kind == "unrelated":
        # Same fail-close outcome either way, but say which it was: an outage-red
        # only needs a re-run, while a missing URL from a reachable GitHub is a
        # triage bug worth looking at.
        if _github_reachable():
            lines.append(
                "AI triage: verdict was `unrelated` but no valid issue URL was "
                "provided - keeping the job red (fail-close)"
            )
        else:
            lines.append(
                "AI triage: verdict was `unrelated` but the issue could not be "
                "filed - GitHub was unreachable or `gh` was unauthenticated from "
                "this runner, so the failure is untracked and the job stays red "
                "(fail-close); re-run the job once GitHub is reachable"
            )
    elif pr_mode:
        lines.append(f"AI triage: failure judged {kind.upper()} - the job stays red")
        if issue_url:
            lines.append(f"AI triage: related tracking issue: {issue_url}{issue_note}")
    else:
        # Post-merge run: the verdict never changes the job status; it exists
        # to file/track the issue and attribute the regression. The issue
        # counts as tracking only under the same checks as a PR downgrade.
        if issue_url and not fingerprint:
            not_tracked = "this failure has no stable signature to match it on"
        elif issue_url and not verified:
            not_tracked = "it could not be checked for this failure's fingerprint"
        elif issue_url and marker_problems:
            not_tracked = "it is not an open issue tracking this failure's fingerprint"
        else:
            not_tracked = ""
        if issue_url and not not_tracked:
            lines.append(f"AI triage (post-merge): tracked in {issue_url}{issue_note}")
        elif issue_url:
            lines.append(
                f"AI triage (post-merge): {issue_url}{issue_note} is not confirmed "
                f"as tracking this failure ({not_tracked}) - the failure is NOT "
                f"fully tracked yet"
            )
        elif not fingerprint:
            lines.append(
                "AI triage (post-merge): no stable failure signature, so no issue "
                "was filed - the failure is NOT tracked"
            )
        else:
            lines.append(
                "AI triage (post-merge): no issue URL provided - "
                "the failure is NOT tracked yet"
            )
        for other in verdict.get("other_issues") or []:
            lines.append(_other_issue_line(other, repo_name))
        generic = [name for name in uncovered if not _stable_signature(name)]
        if fingerprint and generic:
            names = ", ".join(f"`{name}`" for name in generic)
            lines.append(
                f"AI triage (post-merge): {names} has no stable signature, so no "
                f"issue was filed for it - NOT tracked"
            )
        if culprit_pr:
            lines.append(f"AI triage: attributed to {culprit_pr}")
    if reasoning:
        lines.append(f"AI triage reasoning: {reasoning}")
    if repro and repro_status == "reproduced":
        lines.append(f"AI triage reproducer (verified):\n{repro}")
    elif repro:
        lines.append(f"AI triage reproducer (NOT verified):\n{repro}")
    elif verdict.get("repro_error"):
        lines.append(f"AI triage reproducer: unavailable ({verdict['repro_error']})")
    elif repro_status == "not_attempted":
        lines.append("AI triage reproducer: not attempted")
    else:
        lines.append("AI triage reproducer: none produced")
    result.set_info("\n".join(lines))

    for path in (VERDICT_FILE, REPRO_FILE, AGENT_LOG):
        if path.exists() and path.stat().st_size > 0:
            result.set_files([path])


def _other_issue_line(other, repo_name):
    """Report line for the issue of one of `other_signatures`."""
    signature, issue_url = other["signature"], other.get("issue_url") or ""
    if not issue_url:
        error = other.get("issue_error") or "no issue URL"
        return (
            f"AI triage (post-merge): `{signature}` is NOT tracked yet "
            f"(filing its issue failed: {error})"
        )
    verified, problems = _verify_issue_marker(
        issue_url, other["fingerprint"], repo_name
    )
    note = " (new issue)" if other.get("issue_created") else " (existing issue)"
    if verified and not problems:
        return f"AI triage (post-merge): `{signature}` tracked in {issue_url}{note}"
    reason = "; ".join(problems) or "it could not be checked for its fingerprint"
    return (
        f"AI triage (post-merge): `{signature}`: {issue_url}{note} is not confirmed "
        f"as tracking it ({reason}) - NOT fully tracked yet"
    )


def known_issue_verdict(failures, uncovered, occurrences, pr_mode):
    """The verdict for a failure an independent open issue already tracks, or None.

    Such an issue settles relatedness without an agent: the failure predates the
    change. It vouches only for the failures its fingerprint covers, and only when it
    saw the same qualified stack on the branch before the change: the fingerprint
    alone can be shared by two bugs.
    """
    if uncovered or not occurrences or not occurrences.exact:
        return None
    issue_url = occurrences.exact[0]
    change = "PR" if pr_mode else "commit"
    return {
        "verdict": "unrelated",
        "reasoning": (
            f"Known issue: {issue_url} is open, carries this failure's fingerprint, "
            f"and records a post-merge run on `{occurrences.branch}` that hit this stack "
            f"before this {change}, so the failure predates the {change}. No agent was run."
        ),
        "signature": _tracked_signature(failures),
        "issue_url": issue_url,
        "issue_created": False,
        "label_applied": True,
        "culprit_pr": "",
        "repro": "",
        "repro_status": "not_attempted",
    }


def _needs_repro(verdict):
    """Someone reads a reproducer only on a red verdict or in an issue that has none.

    `uncertain` counts as red: a human has to decide, and a reproducer helps most there.
    """
    return (
        verdict["verdict"] in ("related", "uncertain")
        or bool(verdict.get("issue_created"))
        or verdict.get("issue_has_repro") is False
    )


def _should_post_repro(verdict):
    """Whether the reproducer goes to the issue: a new one, or a reused one that has none."""
    return (
        bool(verdict.get("issue_url"))
        and verdict.get("repro_status") == "reproduced"
        and (
            bool(verdict.get("issue_created"))
            or verdict.get("issue_has_repro") is False
        )
    )


def _reproduce(
    info, job_kind, failures, evidence_paths, repro_hint, pr_mode, verdict, deadline
):
    """The reproducer fields for `verdict`; a failure here never loses the verdict."""
    timeout_sec = int(deadline - time.monotonic())
    if timeout_sec < MIN_AGENT_RUN_SEC:
        error = "the verdict used up the triage budget"
        return {"repro": "", "repro_status": "not_attempted", "repro_error": error}
    try:
        prompt = _build_repro_prompt(
            info,
            job_kind,
            failures,
            evidence_paths,
            repro_hint,
            pr_mode,
            verdict,
            timeout_sec,
        )
        return _run_agent(
            prompt, deadline, timeout_sec, REPRO_EFFORT, REPRO_FILE, _load_repro
        )
    except Exception as e:  # noqa: BLE001 - the verdict stands without a reproducer
        print(f"WARNING: AI fuzz triage: reproducer unavailable: {e}")
        return {"repro": "", "repro_status": "not_attempted", "repro_error": str(e)}


def triage_and_apply(
    result,
    *,
    job_kind,
    failures,
    evidence_paths,
    repro_hint,
    agent_timeout_sec=AGENT_TIMEOUT_SEC,
):
    """Triage a failed fuzz-style job result and adjust it in place.

    PR runs may be downgraded to OK on an `unrelated` verdict with an issue
    URL. Post-merge runs (master/release branches) keep their status; triage
    only files/references the tracking issue and attributes the regression.
    No-op when triage is disabled, this is a local run, the result is not a
    plain failure, or triage itself breaks in any way.
    """
    if os.environ.get("AI_FUZZ_TRIAGE", "1") == "0":
        print("AI fuzz triage: disabled via AI_FUZZ_TRIAGE=0")
        return
    if not result.is_failure():
        # ERROR means infra/harness trouble - nothing for the AI to judge.
        return

    try:
        info = Info()
        if info.is_local_run:
            print("AI fuzz triage: local run, skipping")
            return
        pr_mode = bool(info.pr_number)
        # Only on the PR itself: after the merge the author is whoever merged it,
        # and a post-merge regression still needs its tracking issue.
        if pr_mode and (info.user_name or "").lower() in SELF_TRIAGING_AUTHORS:
            print(
                f"AI fuzz triage: PR by self-triaging agent '{info.user_name}', skipping"
            )
            return
        TRIAGE_DIR.mkdir(parents=True, exist_ok=True)
        fingerprint = failure_fingerprint(failures)
        uncovered = uncovered_failure_names(failures)
        print(f"AI fuzz triage: failure fingerprint {fingerprint or '(none)'}")
        if uncovered:
            print(
                f"AI fuzz triage: failures the fingerprint does not cover: {uncovered}"
            )
        repo_name = _triage_repo(info)
        occurrences = (
            _prior_occurrences(
                info, repo_name, pr_mode, fingerprint, stack_digest(failures)
            )
            if fingerprint
            else Occurrences([], [])
        )
        # Not the tuple itself: its `bodies` would dump whole issues into the log.
        if occurrences is None:
            print("AI fuzz triage: prior occurrences unknown")
        else:
            print(
                f"AI fuzz triage: prior occurrences on `{occurrences.branch}`: "
                f"older {occurrences.older}, fixed {occurrences.fixed}, "
                f"exact {occurrences.exact}, mismatched {occurrences.mismatched}"
            )
        verdict = known_issue_verdict(failures, uncovered, occurrences, pr_mode)
        if verdict:
            print(f"AI fuzz triage: known issue {verdict['issue_url']}, no agent run")
        else:
            if pr_mode:
                _write_pr_diff(info, repo_name)
            budget = int(os.environ.get("AI_FUZZ_TRIAGE_TIMEOUT", agent_timeout_sec))
            deadline = time.monotonic() + budget
            history_available = not pr_mode and _fetch_recent_history(
                info.sha, deadline
            )
            # After the fetch, which a private repository needs them for.
            _drop_checkout_credentials()
            _write_known_issues(occurrences)
            verdict_timeout = min(VERDICT_TIMEOUT_SEC, int(_remaining_sec(deadline)))
            prompt = _build_prompt(
                info,
                job_kind,
                failures,
                evidence_paths,
                pr_mode,
                fingerprint,
                occurrences,
                verdict_timeout,
                history_available,
            )
            verdict = _run_agent(
                prompt,
                deadline,
                verdict_timeout,
                VERDICT_EFFORT,
                VERDICT_FILE,
                _load_verdict,
            )
            if not pr_mode and verdict["verdict"] == "related":
                verdict["culprit_pr"] = _culprit_pr(
                    repo_name, verdict["culprit_commit"], deadline
                )
            found_in = _report_url(info) or info.pr_url or info.commit_url
            if fingerprint and (not pr_mode or verdict["verdict"] == "unrelated"):
                try:
                    verdict.update(
                        _track_issue(
                            repo_name,
                            fingerprint,
                            verdict,
                            _tracked_failures(failures),
                            job_kind,
                            found_in,
                            info.pr_url if pr_mode else "",
                            deadline,
                        )
                    )
                except Exception as e:  # noqa: BLE001 - the report says it is untracked
                    print(f"WARNING: AI fuzz triage: cannot file the issue: {e}")
                    traceback.print_exc()
                    verdict["issue_error"] = f"{type(e).__name__}: {e}"
            if not pr_mode:
                verdict["other_issues"] = _track_other_signatures(
                    repo_name, failures, job_kind, found_in, deadline
                )
            if _needs_repro(verdict):
                verdict.update(
                    _reproduce(
                        info,
                        job_kind,
                        failures,
                        evidence_paths,
                        repro_hint,
                        pr_mode,
                        verdict,
                        deadline,
                    )
                )
                if _should_post_repro(verdict):
                    try:
                        _post_repro(verdict["issue_url"], verdict["repro"], deadline)
                    except Exception as e:  # noqa: BLE001 - the report still has it
                        print(f"WARNING: AI fuzz triage: cannot post reproducer: {e}")
        _apply_verdict(
            result,
            verdict,
            pr_mode,
            fingerprint,
            uncovered,
            repo_name,
            regression=bool(occurrences and occurrences.regression),
            occurrences_checked=occurrences is not None,
        )
    except Exception as e:  # noqa: BLE001 - triage must never break the job
        print(f"WARNING: AI fuzz triage unavailable: {e}")
        traceback.print_exc()
        result.set_info(f"AI triage unavailable ({e}); job status left unchanged")
        if AGENT_LOG.exists() and AGENT_LOG.stat().st_size > 0:
            result.set_files([AGENT_LOG])
