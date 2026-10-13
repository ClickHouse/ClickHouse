"""
AI triage for fuzzer/stress job failures.

After a fuzz-style job (AST Fuzzer, BuzzHouse, Stress test) fails, an AI agent
inspects the failure evidence and files or references a tracking issue. A
failure that an open, independent issue already tracks (see below) is linked to
it with no agent run at all. A second, separately budgeted agent run reduces a
minimal reproducer, only for a `related` verdict or a newly filed issue.
Behaviour depends on the run kind:

PR runs - the agent judges whether the failure was introduced by the PR:
  - `related`   -> the job stays red; the agent's reasoning and (best-effort)
                   minimal reproducer are appended to the job report so the
                   author can fix it.
  - `unrelated` -> the agent finds or creates a GitHub issue for the failure,
                   and the job is downgraded to OK with a link to the issue.
  - `uncertain` -> treated as `related`: the job stays red and a human decides.

Post-merge runs (master / release branches) - the job status is never changed;
the agent always finds or creates a tracking issue, attributes the regression
to the merged PR when it can (`Caused by: <PR>` in the issue), and puts the
issue link plus reproducer into the job report.

Fail-close: the downgrade to OK happens only on PR runs, on an explicit
`unrelated` verdict whose GitHub issue is verified to be open in this repository
and to carry the failure's fingerprint and the `fuzz` label, and only when every failure shares the
stable signature (a stack trace ID) that fingerprint is built from; a generic
name such as `Server died` gets no issue at all. Any triage problem
(agent CLI missing, secrets unavailable, malformed verdict, timeout) leaves the
job status exactly as the fuzzer set it and only appends a note that triage
was unavailable.

Relatedness never rests on a record this triage could have written itself: only
an open issue opened before the tested change, and not filed by triage of a PR
run, counts as proof that the failure is older (`independent_occurrences`). A
fingerprint found only in closed issues is a fixed bug that came back, which the
PR may have reintroduced, so it keeps a PR run red (`fixed_occurrences`), and so
does a lookup of those issues that did not complete.

PRs authored by an agent that already triages its own failures are skipped
(`SELF_TRIAGING_AUTHORS`); their post-merge runs are not.

The agent backend mirrors `copilot_review_job.py`: `codex` by default,
`copilot` via `AI_FUZZ_TRIAGE_BACKEND=copilot`. Set `AI_FUZZ_TRIAGE=0` to
disable triage entirely.
"""

import hashlib
import json
import os
import random
import re
import shlex
import subprocess
import tempfile
import time
import traceback
from datetime import datetime
from pathlib import Path
from typing import NamedTuple

from ci.jobs.scripts.agent import (
    AGENT_MODEL,
    ROBOT_NAMES,
    codex_login,
    gh_auth_with_robot_token,
    repo_from_pr_url,
)
from ci.praktika.info import Info
from ci.praktika.result import Result

TRIAGE_DIR = Path("./ci/tmp/ai_fuzz_triage")
VERDICT_FILE = TRIAGE_DIR / "verdict.json"
REPRO_FILE = TRIAGE_DIR / "repro.json"
AGENT_LOG = TRIAGE_DIR / "agent.log"

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

# Machine-readable dedup key carried in the issue body. Kept in sync with
# `Issue.parse_issue_body_fields`, which reads it back out of existing issues.
FINGERPRINT_FIELD = "Fuzz fingerprint"
# The `fuzz` label already holds thousands of issues, so listing them all and
# grepping is not an option: the lookup has to be targeted (search) plus a small
# newest-first window (REST). The window only needs to cover the concurrent jobs
# on this commit, because that is the only case search can be too stale to see.
RECENT_ISSUE_WINDOW = 100
# Provenance line of an issue filed by triage of a PR run. Such an issue records
# that run's verdict, not an observation of the failure on a branch.
FOUND_WHILE_TESTING_FIELD = "Found while testing"
FOUND_WHILE_TESTING_RE = re.compile(
    rf"^[\s`>*-]*{FOUND_WHILE_TESTING_FIELD}:", re.MULTILINE
)

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
    tracked = _tracked_signature(failures)
    if not tracked:
        return ""
    return hashlib.sha1(tracked.encode("utf-8", "replace")).hexdigest()[:12]


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
    if occurrences.older:
        urls = "".join(f"  - {url}\n" for url in occurrences.older)
        return (
            f"- Known occurrences: these open issues carry this failure's fingerprint\n"
            f"  and were filed before this {change}, not by triage of a PR run, so the\n"
            f"  failure predates the {change}. That {weight}:\n{urls}"
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
- No open issue with this failure's fingerprint was filed before this {change},
  other than by triage of a PR run. Any other issue carrying the fingerprint
  was written by this same triage (an earlier run, a sibling job on this commit,
  or another PR's verdict): it is not evidence, so do not look for one here.
  Judge from the code and the logs alone.
"""


def _relatedness_task(info, repo_name, pr_mode, fingerprint, occurrences):
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

- Read the PR diff: `gh pr diff {info.pr_number} --repo {repo_name}`.
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

- The checkout is at the tested commit. List recent history:
  `git log --oneline -30`.
- Compare the crash stack / error signature with what those commits touch. Map
  a suspect commit to its PR:
  `gh api repos/{repo_name}/commits/<commit sha>/pulls --jq '.[].html_url'`.
- Confident attribution to one merged PR -> verdict `related`, and set
  `culprit_pr` to that PR's full URL.
{occurrences}\
- If you cannot decide, answer `uncertain`.
"""


def _fingerprint_lookup(repo_name, fingerprint):
    """The one authoritative duplicate check, for the issue task only.

    It also finds issues this triage filed itself, so it must not feed the
    relatedness judgment; `_prior_occurrences` does that.

    Two queries, because neither alone is sufficient: search reaches the whole
    (thousands-strong) `fuzz` backlog but its index lags, and the REST list is
    current but too large to scan, so it is bounded to the newest issues.
    """
    marker = f"{FINGERPRINT_FIELD}: {fingerprint}"
    return (
        f"run BOTH of these and treat a hit from either as a duplicate -\n"
        f"  (a) `gh search issues {fingerprint} --repo {repo_name}"
        f" --json number,url,title,state,body` and keep the issues whose body"
        f" contains the line `{marker}`, which covers the whole backlog but can"
        f" be minutes stale;\n"
        f"  (b) `gh issue list --repo {repo_name} --label fuzz --state all"
        f" --limit {RECENT_ISSUE_WINDOW} --json number,url,title,state,body`"
        f" (newest first, immediately consistent) and keep the issues whose body"
        f" contains the line `{marker}`  - this is the one that catches an issue"
        f" a sibling CI job opened moments ago, which (a) cannot yet see"
    )


def _issue_task(job_kind, repo_name, found_in, tested_change, pr_mode, fingerprint):
    if not fingerprint:
        # Without a dedup key every job would file its own issue, or point at
        # an unrelated one, so the failure is left to the job report.
        return """\
## Task 2 - GitHub issue (SKIP)

This failure has no stable signature: its name is a generic fallback shared by
unrelated failures, so no fingerprint can tell whether an issue already exists.
Do not create or comment on any issue. Leave `issue_url` empty and
`issue_created` false; the job report carries your reasoning.
"""
    if pr_mode:
        scope = "ONLY when your verdict is `unrelated`"
        caused_by = ""
    else:
        scope = "ALWAYS - a post-merge fuzz failure must be tracked"
        caused_by = (
            "  - When the verdict is `related` with a culprit PR, add the line\n"
            "    `Caused by: <full culprit PR URL>` on its own line.\n"
        )
    if pr_mode:
        provenance = (
            f"    `Found by {job_kind} in CI: {found_in}`\n"
            f"    `{FOUND_WHILE_TESTING_FIELD}: {tested_change}` (state in the issue that\n"
            f"    the failure does not look related to that PR)"
        )
    else:
        provenance = f"    `Found by {job_kind} in CI: {found_in}`"
    return f"""\
## Task 2 - GitHub issue ({scope})

This failure's fingerprint is `{fingerprint}`. Every job that hits the same bug
computes the same value, so the fingerprint - not your own wording, and not how
similar two titles look - decides whether an issue already exists.

- Duplicate check (do this first, and only this):
  {_fingerprint_lookup(repo_name, fingerprint)}.
  Query (b) is not optional: around 20 fuzz-style jobs triage in parallel on one
  commit, so assume a sibling job filed this issue moments ago and that search
  has not indexed it yet. Never decide "no duplicate" from (a) alone.
- An OPEN match with the `fuzz` label -> reference that issue and stop; never
  open a second one. Comment on it only if no existing comment already mentions
  this job name (`{job_kind}`); the point is one comment per failure, not one
  per job. Link: {found_in}
- An OPEN match without the `fuzz` label is not tracking the failure: the
  lookup of later jobs cannot reach it, and you cannot add the label (that is an
  edit). Handle it like a CLOSED match below.
- A CLOSED match means the bug was fixed and has regressed. Do not silently
  point at it - that would leave the regression untracked, and you cannot reopen
  it (reopening is an edit, and edits are forbidden here). Comment on the closed
  issue with this CI link, then create a new issue as below carrying the same
  fingerprint line and `Related: <URL of the matched issue>`.
- No match -> create exactly one issue with
  `gh issue create --repo {repo_name} --label fuzz`. For the new issue:
  - Title: short signature, e.g. `Fuzzer: Logical error: '<assertion>' in <function>`.
  - Body must contain this line verbatim, on its own line, or every later job
    will file a duplicate:
    `{FINGERPRINT_FIELD}: {fingerprint}`
  - Body also carries what failed, the error/stack excerpt (trimmed), the raw
    failing query from the logs (a later step comments with a reduced
    reproducer when it finds one), and these provenance lines, each on its own
    line:
{provenance}
{caused_by}\
  - Use full GitHub URLs when linking anything.
  - The `fuzz` label is what makes the lookup above find this issue later. If
    `--label fuzz` is rejected, retry once; if it still fails, create the issue
    anyway (tracking the bug matters more) and set `label_applied` to false so
    the report can flag that this issue is invisible to future dedup.
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
):
    repo_name = _triage_repo(info)
    report_url = _report_url(info)
    if pr_mode:
        run_kind = "on a ClickHouse pull request"
    else:
        run_kind = f"on the `{info.git_branch}` branch (post-merge run)"
    tested_change = info.pr_url if pr_mode else info.commit_url
    found_in = report_url or tested_change
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
            "The job downgrades to green only for verdict `unrelated` with a valid\n"
            "`issue_url`."
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

{_evidence_sections(failures, evidence_paths)}
## Context

{_change_line(info, pr_mode)}
- CI job: `{info.job_name}`, report: {report_url or "(unavailable)"}
- GitHub repository: `{repo_name}`. Pass `--repo {repo_name}` to gh where applicable.
- The `gh` CLI is pre-authenticated. Never run destructive gh commands (close,
  edit, delete); you may only search/read and create ONE issue as described below.
- Do not reduce a reproducer or replay queries: a separate step does that when
  it is needed. Read the code and the logs, and finish within {timeout_sec // 60} minutes.

{_relatedness_task(info, repo_name, pr_mode, fingerprint, occurrences)}
{_issue_task(job_kind, repo_name, found_in, tested_change, pr_mode, fingerprint)}
## Task 3 - verdict file (MANDATORY)

Write a single JSON object to `{VERDICT_FILE}`:

{{
  "verdict": "related" | "unrelated" | "uncertain",
  "reasoning": "<= 1200 chars, plain text",
  "signature": "one-line failure signature",
  "issue_url": "https://github.com/owner/repo/issues/N or empty string",
  "issue_created": true | false,
  "label_applied": true | false,
  "culprit_pr": "full PR URL when attributed, else empty string"
}}

{outcome_note} Do not post PR comments; the job report carries your output.
"""


def _build_repro_prompt(
    info, job_kind, failures, evidence_paths, repro_hint, pr_mode, verdict, timeout_sec
):
    issue_url = verdict.get("issue_url") or ""
    if verdict.get("issue_created") and issue_url:
        comment_task = f"""\
- Only when `repro_status` is `reproduced`: post the final snippet as ONE
  comment on {issue_url} (`gh issue comment {issue_url} --body-file <file>`).
"""
        allowed = "post at most the one issue comment described below"
    else:
        comment_task = ""
        allowed = "only read"
    return f"""\
You are reducing a minimal reproducer for a failed `{job_kind}` CI job. An
earlier step judged the failure `{verdict["verdict"]}`:
{(verdict.get("reasoning") or "(no reasoning given)").strip()[:1200]}

{_evidence_sections(failures, evidence_paths)}
## Context

{_change_line(info, pr_mode)}
- CI job: `{info.job_name}`
- The `gh` CLI is pre-authenticated. Never create, edit, close or delete
  anything on GitHub; you may {allowed}.

## Task 1 - minimal reproducer (finish within {timeout_sec // 60} minutes)

{repro_hint}

- Extract the failing query/queries from the logs, replay them, and shrink the
  case (drop irrelevant settings, columns, and clauses) while the failure still
  reproduces.
- Set `repro_status` to `reproduced` ONLY if you observed the failure yourself
  with the final snippet. Otherwise use `not_reproduced` or `not_attempted`.
{comment_task}
## Task 2 - result file (MANDATORY)

Write a single JSON object to `{REPRO_FILE}`:

{{
  "repro": "SQL or shell snippet, or empty string",
  "repro_status": "reproduced" | "not_reproduced" | "not_attempted"
}}
"""


def _report_url(info):
    try:
        return info.get_report_url(latest=True)
    except Exception:  # noqa: BLE001 - the link is a convenience
        return ""


def _change_line(info, pr_mode):
    if pr_mode:
        return f"- PR: {info.pr_url} (head sha `{info.sha}`)"
    return f"- Commit: {info.commit_url} (sha `{info.sha}`)"


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


def _run_shell_to_agent_log(command):
    """Run the agent command, teeing all output into AGENT_LOG."""
    with open(AGENT_LOG, "ab") as log:
        return subprocess.run(
            command,
            shell=True,
            stdout=log,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
        ).returncode


def _run_codex_once(prompt, robot_name, timeout_sec, effort, output_file):
    _drop_stale_output(output_file)
    with tempfile.TemporaryDirectory() as gh_config_dir, tempfile.TemporaryDirectory(
        dir="./ci/tmp"
    ) as codex_home:
        gh_auth_with_robot_token(gh_config_dir, robot_name)
        codex_login(codex_home)
        # Same flags as copilot_review_job.py; `timeout` bounds the attempt so
        # the job always gets to upload its artifacts.
        return _run_shell_to_agent_log(
            f"CODEX_HOME={shlex.quote(codex_home)} "
            f"GH_CONFIG_DIR={shlex.quote(gh_config_dir)} "
            f"timeout -k 60 {timeout_sec} "
            f"codex exec "
            f"-m {AGENT_MODEL} -c 'model_reasoning_effort={effort}' "
            f"-s workspace-write "
            f"-c sandbox_workspace_write.network_access=true "
            f"-c approval_policy=never "
            f"--color never "
            f"{shlex.quote(prompt)}"
        )


def _run_copilot_once(prompt, robot_name, timeout_sec, effort, output_file):
    _drop_stale_output(output_file)
    with tempfile.TemporaryDirectory() as gh_config_dir:
        gh_auth_with_robot_token(gh_config_dir, robot_name)
        return _run_shell_to_agent_log(
            f"GH_CONFIG_DIR={shlex.quote(gh_config_dir)} "
            f"timeout -k 60 {timeout_sec} "
            f"copilot -p {shlex.quote(prompt)} --allow-all --no-ask-user "
            f"--add-dir . --model {AGENT_MODEL} --effort {effort}"
        )


def _load_json_object(path):
    if not path.exists():
        raise ValueError(f"{path} was not written")
    with open(path, encoding="utf-8") as fh:
        value = json.load(fh)
    if not isinstance(value, dict):
        raise ValueError(f"{path} is not a JSON object")
    return value


def _load_verdict(path=VERDICT_FILE):
    """Parse and validate the verdict file; returns dict or raises ValueError."""
    verdict = _load_json_object(path)
    if verdict.get("verdict") not in VERDICTS:
        raise ValueError(f"invalid verdict value: {verdict.get('verdict')!r}")
    issue_url = verdict.get("issue_url") or ""
    if issue_url and not ISSUE_URL_RE.match(issue_url):
        raise ValueError(f"invalid issue_url: {issue_url!r}")
    return verdict


def _load_repro(path=REPRO_FILE):
    repro = _load_json_object(path)
    if repro.get("repro_status") not in REPRO_STATUSES:
        raise ValueError(f"invalid repro_status: {repro.get('repro_status')!r}")
    return {"repro": repro.get("repro") or "", "repro_status": repro["repro_status"]}


def _run_agent(prompt, deadline, cap_sec, effort, output_file, load):
    """Run the configured backend and return `load(output_file)`.

    Each attempt gets `cap_sec`, cut to what is left before `deadline`
    (`time.monotonic()`). Only a fast failure is retried.
    """
    backend = os.environ.get("AI_FUZZ_TRIAGE_BACKEND", "codex")
    run_once = {"codex": _run_codex_once, "copilot": _run_copilot_once}.get(backend)
    if run_once is None:
        raise ValueError(f"unknown AI_FUZZ_TRIAGE_BACKEND: {backend!r}")

    last_error = None
    robots = ROBOT_NAMES.copy()
    random.shuffle(robots)
    for attempt in range(1, MAX_ATTEMPTS + 1):
        timeout_sec = int(min(cap_sec, deadline - time.monotonic()))
        if timeout_sec < MIN_AGENT_RUN_SEC:
            raise RuntimeError(
                f"triage budget exhausted after {attempt - 1} attempts: {last_error}"
            )
        started = time.monotonic()
        try:
            exit_code = run_once(
                prompt,
                robots[(attempt - 1) % len(robots)],
                timeout_sec,
                effort,
                output_file,
            )
            # A valid output counts even on non-zero exit (e.g. the timeout
            # fired after the file was already written).
            try:
                output = load(output_file)
                if exit_code != 0:
                    print(
                        f"WARNING: {backend} exited with {exit_code}, "
                        f"but wrote a valid {output_file.name} - accepting it"
                    )
                return output
            except (ValueError, json.JSONDecodeError) as e:
                if exit_code in TIMEOUT_EXIT_CODES:
                    last_error = f"{backend} timed out after {timeout_sec}s"
                else:
                    last_error = f"{backend} exit code {exit_code}; output: {e}"
        except Exception as e:  # noqa: BLE001 - any failure here may be retried
            last_error = f"{type(e).__name__}: {e}"
            traceback.print_exc()
        print(f"WARNING: triage attempt {attempt}/{MAX_ATTEMPTS} failed: {last_error}")
        if time.monotonic() - started >= RETRY_IF_FASTER_THAN_SEC:
            raise RuntimeError(f"triage failed, not retried: {last_error}")
        if attempt < MAX_ATTEMPTS:
            time.sleep(min(2**attempt * 5, 60))
    raise RuntimeError(f"triage failed after {MAX_ATTEMPTS} attempts: {last_error}")


def _robot_gh(*args):
    """Run a read-only `gh` command as a robot and return its stdout.

    Fuzz-style jobs run without `enable_gh_auth`, so a bare `gh` in the job is
    unauthenticated. Raises on any failure.
    """
    with tempfile.TemporaryDirectory() as gh_config_dir:
        gh_auth_with_robot_token(gh_config_dir, random.choice(ROBOT_NAMES))
        return subprocess.run(
            ["gh", *args],
            env={**os.environ, "GH_CONFIG_DIR": gh_config_dir},
            capture_output=True,
            text=True,
            timeout=120,
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

    @property
    def regression(self):
        """A fixed bug is back and nothing shows it was back before the change."""
        return bool(self.fixed) and not self.older


def fixed_occurrences(issues, fingerprint):
    """URLs of the closed issues with this fingerprint: the bug was fixed once.

    A closed issue says nothing about whether the failure predates the change;
    the change may be what brought it back.
    """
    marker = f"{FINGERPRINT_FIELD}: {fingerprint}"
    return sorted(
        issue["url"]
        for issue in issues
        if (issue.get("state") or "").upper() == "CLOSED"
        and marker in (issue.get("body") or "")
    )


def _has_fuzz_label(issue):
    return "fuzz" in [label.get("name", "") for label in (issue.get("labels") or [])]


def independent_occurrences(issues, fingerprint, cutoff):
    """URLs of the open issues that prove the failure predates the tested change.

    Only a record this triage cannot have written counts: one opened before the
    change existed and not filed by triage of a PR run, which holds that run's
    verdict rather than an observation on a branch. Otherwise an earlier run, or
    a sibling job on the same commit, would confirm its own verdict.

    It must also be a tracking issue `_verify_issue_marker` accepts, `fuzz` label
    included: linking one that is not would skip the agent that files a real one.
    """
    marker = f"{FINGERPRINT_FIELD}: {fingerprint}"
    urls = []
    for issue in issues:
        body = issue.get("body") or ""
        if (
            (issue.get("state") or "").upper() == "OPEN"
            and marker in body
            and _has_fuzz_label(issue)
            and not FOUND_WHILE_TESTING_RE.search(body)
            and _parse_github_time(issue["createdAt"]) < cutoff
        ):
            urls.append(issue["url"])
    return sorted(urls)


def _prior_occurrences(info, repo_name, pr_mode, fingerprint):
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
        return Occurrences(
            independent_occurrences(issues, fingerprint, cutoff),
            fixed_occurrences(issues, fingerprint),
        )
    except Exception as e:  # noqa: BLE001 - no lookup means no evidence, never more
        print(f"WARNING: AI fuzz triage: cannot look up prior occurrences: {e}")
        return None


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
    if f"{FINGERPRINT_FIELD}: {fingerprint}" not in (issue.get("body") or ""):
        problems.append(
            f"AI triage WARNING: {issue_url} does not carry "
            f"`{FINGERPRINT_FIELD}: {fingerprint}` - the next job hitting this "
            f"bug will file a duplicate; add the line by hand"
        )
    if not _has_fuzz_label(issue):
        problems.append(
            f"AI triage WARNING: {issue_url} has no `fuzz` label, so the "
            f"fingerprint lookup cannot reach it - label it by hand"
        )
    return True, problems


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
        elif issue_url and uncovered:
            names = ", ".join(f"`{name}`" for name in uncovered)
            not_tracked = f"its fingerprint does not cover {names}"
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


def known_issue_verdict(failures, uncovered, occurrences, pr_mode):
    """The verdict for a failure an independent open issue already tracks, or None.

    Such an issue settles relatedness without an agent: the failure predates the
    change. It vouches only for the failures its fingerprint covers.
    """
    if uncovered or not occurrences or not occurrences.older:
        return None
    issue_url = occurrences.older[0]
    change = "PR" if pr_mode else "commit"
    return {
        "verdict": "unrelated",
        "reasoning": (
            f"Known issue: {issue_url} is open, carries this failure's fingerprint "
            f"and was filed before this {change}, not by triage of a PR run, so "
            f"the failure predates the {change}. No agent was run."
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
    """Someone reads a reproducer only on a red verdict or in a new issue."""
    return verdict["verdict"] == "related" or bool(verdict.get("issue_created"))


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
            _prior_occurrences(info, repo_name, pr_mode, fingerprint)
            if fingerprint
            else Occurrences([], [])
        )
        print(f"AI fuzz triage: prior occurrences {occurrences}")
        verdict = known_issue_verdict(failures, uncovered, occurrences, pr_mode)
        if verdict:
            print(f"AI fuzz triage: known issue {verdict['issue_url']}, no agent run")
        else:
            budget = int(os.environ.get("AI_FUZZ_TRIAGE_TIMEOUT", agent_timeout_sec))
            deadline = time.monotonic() + budget
            verdict_timeout = min(VERDICT_TIMEOUT_SEC, budget)
            prompt = _build_prompt(
                info,
                job_kind,
                failures,
                evidence_paths,
                pr_mode,
                fingerprint,
                occurrences,
                verdict_timeout,
            )
            verdict = _run_agent(
                prompt,
                deadline,
                verdict_timeout,
                VERDICT_EFFORT,
                VERDICT_FILE,
                _load_verdict,
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
