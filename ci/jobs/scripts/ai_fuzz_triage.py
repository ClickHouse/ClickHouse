"""
AI triage for fuzzer/stress job failures.

After a fuzz-style job (AST Fuzzer, BuzzHouse, Stress test) fails, an AI agent
inspects the failure evidence, attempts a minimal reproducer, and files or
references a tracking issue. Behaviour depends on the run kind:

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
`unrelated` verdict that carries a valid GitHub issue URL. Any triage problem
(agent CLI missing, secrets unavailable, malformed verdict, timeout) leaves the
job status exactly as the fuzzer set it and only appends a note that triage
was unavailable.

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
from pathlib import Path

from ci.jobs.scripts.agent_cli import (
    AGENT_MODEL,
    ROBOT_NAMES,
    codex_login,
    gh_auth_with_robot_token,
    repo_from_pr_url,
)
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.utils import Shell

TRIAGE_DIR = Path("./ci/tmp/ai_fuzz_triage")
VERDICT_FILE = TRIAGE_DIR / "verdict.json"
AGENT_LOG = TRIAGE_DIR / "agent.log"

MAX_ATTEMPTS = 2
# Default hard cap on one agent attempt. Callers running inside tighter job
# timeouts (e.g. the 2h Upgrade check) pass a smaller `agent_timeout_sec`;
# `AI_FUZZ_TRIAGE_TIMEOUT` overrides either for operational tuning.
AGENT_TIMEOUT_SEC = 2400

VERDICTS = ("related", "unrelated", "uncertain")
ISSUE_URL_RE = re.compile(r"^https://github\.com/[\w.-]+/[\w.-]+/issues/\d+$")

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


def _normalize_failure_name(name):
    for pattern, replacement in FINGERPRINT_VOLATILE:
        name = pattern.sub(replacement, name)
    return re.sub(r"\s+", " ", name).strip()


def failure_fingerprint(failures):
    """Stable key for a failure, identical across every job that hits the same bug.

    Derived from the parser's failure name, which is already built from the
    `Format string:` when the log carries one, so it holds no runtime values.

    A stress run reports several failed tests and their order is not stable
    between runs, so the key is the smallest normalized name rather than the
    first one - otherwise two jobs hitting the same bug disagree.
    """
    names = sorted(
        n for n in (_normalize_failure_name(name or "") for name, _ in failures) if n
    )
    if not names:
        return ""
    return hashlib.sha1(names[0].encode("utf-8", "replace")).hexdigest()[:12]


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


def _relatedness_task(info, repo_name, pr_mode, fingerprint):
    if pr_mode:
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
- Check for known occurrences: {_fingerprint_lookup(repo_name, fingerprint)}.
  A pre-existing issue with the same fingerprint, opened before this PR, is
  strong evidence for `unrelated`.
- If you cannot decide, answer `uncertain`. The job then stays red and a human
  decides - never guess `unrelated` to make CI green.
"""
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
- Check for known occurrences: {_fingerprint_lookup(repo_name, fingerprint)}.
  A pre-existing issue with the same fingerprint means verdict `unrelated`.
- If you cannot decide, answer `uncertain`.
"""


def _fingerprint_lookup(repo_name, fingerprint):
    """The one authoritative duplicate check, shared by both tasks that need it.

    Two queries, because neither alone is sufficient: search reaches the whole
    (thousands-strong) `fuzz` backlog but its index lags, and the REST list is
    current but too large to scan, so it is bounded to the newest issues.
    """
    marker = f"{FINGERPRINT_FIELD}: {fingerprint}"
    return (
        f"run BOTH of these and treat a hit from either as a duplicate -\n"
        f"  (a) `gh search issues --repo {repo_name} --state all"
        f' "{marker}" --json number,url,title,state`, which covers the whole'
        f" backlog but can be minutes stale;\n"
        f"  (b) `gh issue list --repo {repo_name} --label fuzz --state all"
        f" --limit {RECENT_ISSUE_WINDOW} --json number,url,title,state,body`"
        f" (newest first, immediately consistent) and keep the issues whose body"
        f' contains the line `{marker}`  - this is the one that catches an issue'
        f" a sibling CI job opened moments ago, which (a) cannot yet see"
    )


def _issue_task(job_kind, repo_name, found_in, tested_change, pr_mode, fingerprint):
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
            f"    `Found while testing: {tested_change}` (state in the issue that\n"
            f"    the failure does not look related to that PR)"
        )
    else:
        provenance = f"    `Found by {job_kind} in CI: {found_in}`"
    return f"""\
## Task 3 - GitHub issue ({scope})

This failure's fingerprint is `{fingerprint}`. Every job that hits the same bug
computes the same value, so the fingerprint - not your own wording, and not how
similar two titles look - decides whether an issue already exists.

- Duplicate check (do this first, and only this):
  {_fingerprint_lookup(repo_name, fingerprint)}.
  Query (b) is not optional: around 20 fuzz-style jobs triage in parallel on one
  commit, so assume a sibling job filed this issue moments ago and that search
  has not indexed it yet. Never decide "no duplicate" from (a) alone.
- An OPEN match -> reference that issue and stop; never open a second one.
  Comment on it only if no existing comment already mentions this job name
  (`{job_kind}`); the point is one comment per failure, not one per job.
  Link: {found_in}
- A CLOSED match means the bug was fixed and has regressed. Do not silently
  point at it - that would leave the regression untracked, and you cannot reopen
  it (reopening is an edit, and edits are forbidden here). Comment on the closed
  issue with this CI link, then create a new issue as below carrying the same
  fingerprint line and `Related: <closed issue URL>`.
- No match -> create exactly one issue with
  `gh issue create --repo {repo_name} --label fuzz`. For the new issue:
  - Title: short signature, e.g. `Fuzzer: Logical error: '<assertion>' in <function>`.
  - Body must contain this line verbatim, on its own line, or every later job
    will file a duplicate:
    `{FINGERPRINT_FIELD}: {fingerprint}`
  - Body also carries what failed, the error/stack excerpt (trimmed), the
    reproducer from Task 2 (or the raw failing query when not reduced), and
    these provenance lines, each on its own line:
{provenance}
{caused_by}\
  - Use full GitHub URLs when linking anything.
  - The `fuzz` label is what makes the lookup above find this issue later. If
    `--label fuzz` is rejected, retry once; if it still fails, create the issue
    anyway (tracking the bug matters more) and set `label_applied` to false so
    the report can flag that this issue is invisible to future dedup.
"""


def _build_prompt(
    info, job_kind, failures, evidence_paths, repro_hint, pr_mode, fingerprint
):
    repo_name = repo_from_pr_url(info.pr_url) or info.repo_name
    try:
        report_url = info.get_report_url(latest=True)
    except Exception:
        report_url = ""
    evidence = "\n".join(f"- {p}" for p in evidence_paths) or "- (none collected)"

    if pr_mode:
        run_kind = "on a ClickHouse pull request"
        change_line = f"- PR: {info.pr_url} (head sha `{info.sha}`)"
    else:
        run_kind = f"on the `{info.git_branch}` branch (post-merge run)"
        change_line = f"- Commit: {info.commit_url} (sha `{info.sha}`)"
    tested_change = info.pr_url if pr_mode else info.commit_url
    found_in = report_url or tested_change
    outcome_note = (
        "The job downgrades to green only for verdict `unrelated` with a valid\n"
        "`issue_url`."
        if pr_mode
        else "The job status is not changed by your verdict; your output and the\n"
        "issue link land in the job report."
    )

    return f"""\
You are triaging a failed `{job_kind}` CI job {run_kind}.

## Failure reported by the job

{_failures_section(failures)}

## Evidence on disk (read as needed; directories may hold several logs)

{evidence}

## Context

{change_line}
- CI job: `{info.job_name}`, report: {report_url or "(unavailable)"}
- GitHub repository: `{repo_name}`. Pass `--repo {repo_name}` to gh where applicable.
- The `gh` CLI is pre-authenticated. Never run destructive gh commands (close,
  edit, delete); you may only search/read and create ONE issue as described below.

{_relatedness_task(info, repo_name, pr_mode, fingerprint)}
## Task 2 - minimal reproducer (best effort, <= 15 minutes)

{repro_hint}

- Extract the failing query/queries from the logs, replay them, and shrink the
  case (drop irrelevant settings, columns, and clauses) while the failure still
  reproduces.
- Set `repro_status` to `reproduced` ONLY if you observed the failure yourself
  with the final snippet. Otherwise use `not_reproduced` or `not_attempted`.

{_issue_task(job_kind, repo_name, found_in, tested_change, pr_mode, fingerprint)}
## Task 4 - verdict file (MANDATORY)

Write a single JSON object to `{VERDICT_FILE}`:

{{
  "verdict": "related" | "unrelated" | "uncertain",
  "reasoning": "<= 1200 chars, plain text",
  "signature": "one-line failure signature",
  "issue_url": "https://github.com/owner/repo/issues/N or empty string",
  "issue_created": true | false,
  "label_applied": true | false,
  "culprit_pr": "full PR URL when attributed, else empty string",
  "repro": "SQL or shell snippet, or empty string",
  "repro_status": "reproduced" | "not_reproduced" | "not_attempted"
}}

{outcome_note} Do not post PR comments; the job report carries your output.
"""


def _drop_stale_verdict():
    if VERDICT_FILE.exists():
        try:
            VERDICT_FILE.unlink()
        except OSError as e:
            print(f"WARNING: cannot remove stale {VERDICT_FILE}: {e}")


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


def _run_codex_once(prompt, robot_name, timeout_sec):
    _drop_stale_verdict()
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
            f"-m {AGENT_MODEL} -c 'model_reasoning_effort=xhigh' "
            f"-s workspace-write "
            f"-c sandbox_workspace_write.network_access=true "
            f"-c approval_policy=never "
            f"--color never "
            f"{shlex.quote(prompt)}"
        )


def _run_copilot_once(prompt, robot_name, timeout_sec):
    _drop_stale_verdict()
    with tempfile.TemporaryDirectory() as gh_config_dir:
        gh_auth_with_robot_token(gh_config_dir, robot_name)
        return _run_shell_to_agent_log(
            f"GH_CONFIG_DIR={shlex.quote(gh_config_dir)} "
            f"timeout -k 60 {timeout_sec} "
            f"copilot -p {shlex.quote(prompt)} --allow-all --no-ask-user "
            f"--add-dir . --model {AGENT_MODEL} --effort xhigh"
        )


def _load_verdict():
    """Parse and validate the verdict file; returns dict or raises ValueError."""
    if not VERDICT_FILE.exists():
        raise ValueError(f"{VERDICT_FILE} was not written")
    with open(VERDICT_FILE, encoding="utf-8") as fh:
        verdict = json.load(fh)
    if not isinstance(verdict, dict):
        raise ValueError("verdict is not a JSON object")
    if verdict.get("verdict") not in VERDICTS:
        raise ValueError(f"invalid verdict value: {verdict.get('verdict')!r}")
    issue_url = verdict.get("issue_url") or ""
    if issue_url and not ISSUE_URL_RE.match(issue_url):
        raise ValueError(f"invalid issue_url: {issue_url!r}")
    return verdict


def _run_agent(prompt, timeout_sec):
    """Run the configured backend with retries; returns a validated verdict."""
    backend = os.environ.get("AI_FUZZ_TRIAGE_BACKEND", "codex")
    run_once = {"codex": _run_codex_once, "copilot": _run_copilot_once}.get(backend)
    if run_once is None:
        raise ValueError(f"unknown AI_FUZZ_TRIAGE_BACKEND: {backend!r}")

    last_error = None
    robots = ROBOT_NAMES.copy()
    random.shuffle(robots)
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            exit_code = run_once(
                prompt, robots[(attempt - 1) % len(robots)], timeout_sec
            )
            # A valid verdict counts even on non-zero exit (e.g. the timeout
            # fired after the file was already written).
            try:
                verdict = _load_verdict()
                if exit_code != 0:
                    print(
                        f"WARNING: {backend} exited with {exit_code}, "
                        f"but wrote a valid verdict - accepting it"
                    )
                return verdict
            except (ValueError, json.JSONDecodeError) as e:
                last_error = f"{backend} exit code {exit_code}; verdict: {e}"
        except Exception as e:  # noqa: BLE001 - any failure here is retryable
            last_error = f"{type(e).__name__}: {e}"
            traceback.print_exc()
        print(f"WARNING: triage attempt {attempt}/{MAX_ATTEMPTS} failed: {last_error}")
        if attempt < MAX_ATTEMPTS:
            time.sleep(min(2**attempt * 5, 60))
    raise RuntimeError(f"triage failed after {MAX_ATTEMPTS} attempts: {last_error}")


def _github_reachable():
    """Probe used only to explain a fail-close red, never to decide job status."""
    try:
        return bool(Shell.get_output("gh api rate_limit --jq .rate.limit"))
    except Exception:  # noqa: BLE001 - a failed probe just means "cannot tell"
        return False


def _verify_issue_marker(issue_url, fingerprint):
    """Check the issue really carries the dedup marker and the `fuzz` label.

    Taking the agent's word for this is how duplicates get back in: an issue
    missing the marker line, or missing the label the lookup filters on, is
    invisible to the next job that hits the same bug. Verifying also catches the
    other failure mode - referencing a similar-looking issue that has a
    different fingerprint.

    Returns `(verified, problems)`; `verified` is False when the check itself
    could not run, which must not be reported as a missing marker.
    """
    if not issue_url or not fingerprint:
        return False, []
    match = re.match(r"^https://github\.com/([\w.-]+/[\w.-]+)/issues/(\d+)$", issue_url)
    if not match:
        return False, [f"AI triage: cannot verify {issue_url}: unrecognized issue URL"]
    repo, number = match.group(1), match.group(2)
    try:
        output = Shell.get_output(
            f"gh issue view {number} --repo {repo} --json body,labels", verbose=True
        )
        issue = json.loads(output) if output else None
    except Exception as e:  # noqa: BLE001 - never lose a valid verdict over this
        return False, [f"AI triage: could not verify {issue_url} ({e})"]
    if not isinstance(issue, dict):
        # Auth, rate limit or network - unknown, not proven wrong.
        return False, [f"AI triage: could not verify the dedup marker on {issue_url}"]

    problems = []
    if f"{FINGERPRINT_FIELD}: {fingerprint}" not in (issue.get("body") or ""):
        problems.append(
            f"AI triage WARNING: {issue_url} does not carry "
            f"`{FINGERPRINT_FIELD}: {fingerprint}` - the next job hitting this "
            f"bug will file a duplicate; add the line by hand"
        )
    labels = [label.get("name", "") for label in (issue.get("labels") or [])]
    if "fuzz" not in labels:
        problems.append(
            f"AI triage WARNING: {issue_url} has no `fuzz` label, so the "
            f"fingerprint lookup cannot reach it - label it by hand"
        )
    return True, problems


def _apply_verdict(result, verdict, pr_mode, fingerprint=""):
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
    verified, marker_problems = _verify_issue_marker(issue_url, fingerprint)
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
    if pr_mode and kind == "unrelated" and issue_url:
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
        # to file/track the issue and attribute the regression.
        if issue_url:
            lines.append(f"AI triage (post-merge): tracked in {issue_url}{issue_note}")
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
    else:
        lines.append("AI triage reproducer: none produced")
    result.set_info("\n".join(lines))

    for path in (VERDICT_FILE, AGENT_LOG):
        if path.exists() and path.stat().st_size > 0:
            result.set_files([path])


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
        TRIAGE_DIR.mkdir(parents=True, exist_ok=True)
        fingerprint = failure_fingerprint(failures)
        print(f"AI fuzz triage: failure fingerprint {fingerprint or '(none)'}")
        prompt = _build_prompt(
            info, job_kind, failures, evidence_paths, repro_hint, pr_mode, fingerprint
        )
        timeout_sec = int(os.environ.get("AI_FUZZ_TRIAGE_TIMEOUT", agent_timeout_sec))
        verdict = _run_agent(prompt, timeout_sec)
        _apply_verdict(result, verdict, pr_mode, fingerprint)
    except Exception as e:  # noqa: BLE001 - triage must never break the job
        print(f"WARNING: AI fuzz triage unavailable: {e}")
        traceback.print_exc()
        result.set_info(f"AI triage unavailable ({e}); job status left unchanged")
        if AGENT_LOG.exists() and AGENT_LOG.stat().st_size > 0:
            result.set_files([AGENT_LOG])
