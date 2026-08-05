"""Guards for the auto-retry's "has never been retried" selector.

``.github/workflows/retry_infra_failures.yml`` retries infra-killed PR runs hourly.
A fork PR's approval gate concludes ``run_attempt`` 1 as ``action_required`` with zero
jobs, so its first attempt that actually runs anything is 2, and a selector keyed on
``attempt == 1`` can never see it. The invariant is "exactly one attempt executed":
attempt 1 for a same-repo run, attempt 2 behind an approval gate.

The step is read as text (like ``test_create_release.py``) so no YAML parser is needed.
The listing filter's coverage is static (its three ``jq`` stages are pinned by exact
equality); the ``jq``-gated behavioural rows cover eligibility and rerun. ``jq`` is absent
from the CI Tests image, so in CI only the static assertions execute and they are what
must pin the step's semantics.
"""

import datetime
import json
import os
import re
import shutil
import stat
import subprocess
import textwrap

import pytest

WORKFLOW = os.path.join(
    os.path.dirname(__file__), "../../.github/workflows/retry_infra_failures.yml"
)

RUN_ID = 42

# The probe, normalized (line continuations and runs of whitespace collapsed). Pinning the
# whole command at once covers the run identifier, the attempt read, the field, and the
# fail-closed fallback: a substring assertion on any one of them leaves the others free.
PROBE_CMD = (
    'attempt1_conclusion=$(gh api "repos/$GH_REPO/actions/runs/$run_id/attempts/1" '
    "--jq '.conclusion' 2>/dev/null || echo \"\")"
)
REJECT_COND = 'if [ "$attempt1_conclusion" != "action_required" ]; then'

# Every command of the candidate's data path, from the listing to the reject condition,
# normalized. A census of ASSIGNMENTS has to enumerate the separators one can follow, and
# that enumeration keeps leaking: `&&`, `||`, `&`, a `{ ... }` group, `read -r var`,
# `printf -v var` and `declare "${v}=2"` all introduce one without a separator the census
# looks for, and each forges run_id, run_attempt or attempt1_conclusion -- either removing
# the never-retried invariant or making the gate inert. A closed whitelist of the region's
# commands refuses every such form, including the ones no regex can name.
# The listing command is matched as a PATTERN: its semantics are pinned by the stage-equality
# assert and by the `--json ... attempt` assert above, so re-pinning its flag spelling here
# only refuses harmless edits (`--limit`, field order, flag order).
LISTING_RE = re.compile(r"^run_entries=\$\(gh run list .*\)$")
# One simple `echo`: no separator (`;` `&` `|`), no grouping or substitution. Such a line
# cannot assign to the shell, so dropping it admits a log line without admitting an
# assignment. Narrowing this pattern can only cause a false refusal, never a false accept.
ECHO_RE = re.compile(r"^echo (?:[^;&|(){}`]*)$")
EXPECTED_DATA_PATH = [
    "<LISTING>",
    'if [ -z "$run_entries" ]; then',
    "exit 0",
    "fi",
    "for run_entry in $run_entries; do",
    'if [ "$rerun_count" -ge "$MAX_RERUNS" ]; then',
    "break",
    "fi",
    'run_id="${run_entry%%:*}"',
    'run_attempt="${run_entry##*:}"',
    'if [ "$run_attempt" != "1" ]; then',
    'attempt1_conclusion=$(gh api "repos/$GH_REPO/actions/runs/$run_id/attempts/1" '
    "--jq '.conclusion' 2>/dev/null || echo \"\")",
    'if [ "$attempt1_conclusion" != "action_required" ]; then',
]

GH_STUB = """#!/usr/bin/env bash
# The caller's own --jq expression, so the stub answers the field that was asked for rather
# than a fixed string: requesting anything but .conclusion must be observable.
_jq_expr() {
    while [ $# -gt 0 ]; do
        if [ "$1" = "--jq" ]; then echo "$2"; return; fi
        shift
    done
}
if [ "$1" = "run" ] && [ "$2" = "list" ]; then
    jq -r "$(_jq_expr "$@")" "$FIXTURE"
    exit 0
fi
if [ "$1" = "run" ] && [ "$2" = "rerun" ]; then
    # Record the argv so a test can assert the rerun really happened.
    echo "$*" >> "$RERUN_LOG"
    exit 0
fi
case "$*" in
    *attempts/1*)
        if [ -n "${ATTEMPT1_FAIL:-}" ]; then
            echo "stub: simulated attempt-1 API error" >&2
            exit 1
        fi
        echo "$ATTEMPT1_JSON" | jq -r "$(_jq_expr "$@")"
        ;;
    *'/jobs?'*) echo "$JOBS_JSON" ;;
    *) echo '{}' ;;
esac
"""

NO_JOBS = '{"jobs": []}'
# A failed "Config Workflow" job: the workflow's unchanged job-level heuristic treats
# that name as infrastructure outright, so no timestamp arithmetic is needed.
INFRA_JOBS = '{"jobs": [{"name": "Config Workflow", "conclusion": "failure", "steps": []}]}'


def _retry_step():
    """The shell body of the ``retry-failed`` job's only step, read as text so that no
    YAML parser is needed (``test_create_release.py`` reads its workflow the same way)."""
    with open(WORKFLOW, encoding="utf-8") as f:
        lines = f.read().splitlines()
    start = next(i for i, l in enumerate(lines) if l.strip() == "run: |") + 1
    indent = len(lines[start]) - len(lines[start].lstrip())
    body = []
    for line in lines[start:]:
        if line.strip() and len(line) - len(line.lstrip()) < indent:
            break
        body.append(line)
    step = textwrap.dedent("\n".join(body))
    # A silently empty extraction would make every assertion below vacuous.
    assert "MAX_RERUNS" in step and "gh run list" in step, step
    return step


def _listing_jq(step):
    """The listing ``--jq`` argument, read from the NORMALIZED invocation so a re-wrap
    across continuation lines is not a false refusal. Returns (expr, invocation)."""
    assert "gh run list" in step, step
    invocation = " ".join(
        step.split("gh run list", 1)[1].split("\n\n", 1)[0].replace("\\\n", " ").split()
    )
    m = re.search(r'--jq\s+"(.*)"\)\s*$', invocation)
    assert m, invocation
    return m.group(1), invocation


def _jq_stages(expr):
    """``expr`` split on top-level ``|``: parenthesised groups and ``\\"``-quoted jq
    strings do not separate stages."""
    stages, cur, depth, i, in_str = [], "", 0, 0, False
    while i < len(expr):
        if expr[i : i + 2] == '\\"':
            in_str = not in_str
            cur += expr[i : i + 2]
            i += 2
            continue
        c = expr[i]
        if not in_str:
            if c == "(":
                depth += 1
            elif c == ")":
                depth -= 1
            elif c == "|" and depth == 0:
                stages.append(cur.strip())
                cur = ""
                i += 1
                continue
        cur += c
        i += 1
    stages.append(cur.strip())
    return stages


_CONTINUE = re.compile(r"^continue\b(?!=)\s*[0-9]*\s*(?:;\s*:\s*)?$")


def _reject_branch_body(step):
    """The significant lines of the already-retried branch's body, comments dropped."""
    lines = step.splitlines()
    idx = next(i for i, l in enumerate(lines) if '!= "action_required" ]; then' in l)
    indent = len(lines[idx]) - len(lines[idx].lstrip())
    body = []
    for l in lines[idx + 1 :]:
        if l.strip() and len(l) - len(l.lstrip()) <= indent:
            break  # the branch's own `fi`
        s = l.strip()
        if s and not s.startswith("#"):
            body.append(s)
    return body


def _data_path_commands(step):
    """The significant commands of the candidate's data path, comments dropped.

    The region runs from the ``run_entries`` assignment to the reject condition inclusive:
    starting at the loop header instead would leave the listing and the empty-listing guard
    outside it, where an assignment can neutralize the selector unseen. Continuations are
    joined and whitespace runs collapsed BEFORE splitting, so a re-wrap is not a refusal.
    """
    lines = step.splitlines()
    start = next(i for i, l in enumerate(lines) if "run_entries=$(gh run list" in l)
    end = next(i for i, l in enumerate(lines) if '!= "action_required" ]; then' in l)
    assert start < end, (start, end)
    joined = "\n".join(lines[start : end + 1]).replace("\\\n", " ")
    body = []
    for l in joined.splitlines():
        s = " ".join(l.split())
        if not s or s.startswith("#"):
            continue
        if ECHO_RE.match(s):
            continue
        body.append("<LISTING>" if LISTING_RE.match(s) else s)
    return body


def test_selector_admits_gated_fork_runs():
    step = _retry_step()
    # The probe spans continuation lines, so its assertions below read this NORMALIZED
    # form (continuations joined, whitespace runs collapsed). A raw-substring form
    # refuses a harmless re-wrap while the probe still reads .conclusion and still
    # fails closed.
    normalized = " ".join(step.replace("\\\n", " ").split())
    listing_jq, listing_invocation = _listing_jq(step)
    # Pin all THREE stages exactly. A shape-only check (three stages, stage 2 starts with
    # `select(`) leaves the projection free, and a filter nested INSIDE it -- `("\(.databaseId):
    # \(.attempt)" | select(false))` -- keeps the stage count, the stage-1 prefix and the
    # projection substring all intact while jq emits nothing at all, so the listing yields no
    # candidates and the whole fix is inert.
    assert _jq_stages(listing_jq) == [
        ".[]",
        r"select(.attempt <= 2 and .createdAt >= \"$cutoff\")",
        r"\"\(.databaseId):\(.attempt)\"",
    ], f"the listing filter must be exactly these three stages: {_jq_stages(listing_jq)}"
    # `.attempt` steers the guard, so the field must be requested. Without it jq yields
    # null, `null <= 2` still admits the row, and run_attempt becomes the string "null":
    # a same-repo attempt-1 run is then skipped and a fork run aborts the step at
    # `$((run_attempt + 1))`.
    m = re.search(r"--json\s+([A-Za-z0-9_,]+)", listing_invocation)
    assert m and "attempt" in m.group(1).split(","), listing_invocation
    # The `<LISTING>` sentinel intentionally does not pin the invocation's flag SPELLING, but
    # the three flags below are semantics, not spelling: `--status success`, another
    # `--workflow`, or another `--repo` all leave the whole static test green while the job
    # retries the wrong runs. Asserted against the normalized invocation, so a re-wrap or a
    # flag reorder is not a false refusal. `--limit` is deliberately not pinned: it is a
    # tuning knob, and the 6 hour cutoff bounds the window independently.
    for flag in ('--repo "$GH_REPO"', "--workflow pull_request.yml", "--status failure"):
        assert flag in listing_invocation, (flag, listing_invocation)
    assert 'attempt1_conclusion" != "action_required"' in step, "missing attempt-1 probe"
    # The probe must interrogate attempt 1: reading any other attempt makes it meaningless.
    # Against `normalized` (hoisted above), so a re-wrap is not a false refusal.
    assert "/attempts/1" in normalized, "the probe must read attempt 1"
    # It must be reached only for a candidate that is NOT attempt 1.
    GUARD = 'if [ "$run_attempt" != "1" ]; then'
    assert GUARD in step, "probe must guard on attempt != 1"
    # Text presence is not reachability: wrapping the whole probe block in `if false; then`
    # keeps every assertion above true while the never-retried invariant stops holding.
    # So pin that the guard IS the condition gating the probe -- loop-body level, with no
    # enclosing conditional still open.
    lines = step.splitlines()
    loop = next(i for i, l in enumerate(lines) if l.strip().startswith("for run_entry in"))
    guard = next(i for i, l in enumerate(lines) if GUARD in l)
    assert loop < guard, (loop, guard)
    depth = 0
    for l in lines[loop + 1 : guard]:
        s = l.strip()
        if re.match(r"^(if|while|until)\b", s):
            depth += 1
        elif s in ("fi", "done"):
            depth -= 1
    assert depth == 0, f"probe is nested inside an enclosing conditional (depth {depth})"
    body_indent = min(
        len(l) - len(l.lstrip()) for l in lines[loop + 1 : guard] if l.strip()
    )
    assert len(lines[guard]) - len(lines[guard].lstrip()) == body_indent, (
        "the probe's guard must sit at loop-body level, not nested one level deeper"
    )
    # The scan above spans only the lines BEFORE the guard, so it cannot see a conditional
    # opened INSIDE it: wrapping the probe in `if false; then` immediately after the guard
    # line keeps every assertion so far true while the probe never runs. So pin that the
    # probe IS the guard's body -- its first significant line, at depth 0.
    guard_indent = len(lines[guard]) - len(lines[guard].lstrip())
    guard_body = []
    for l in lines[guard + 1 :]:
        if l.strip() and len(l) - len(l.lstrip()) <= guard_indent:
            break  # the guard's own matching `fi`
        guard_body.append(l)
    significant = [
        l.strip() for l in guard_body if l.strip() and not l.strip().startswith("#")
    ]
    body_depth = 0
    for s in significant:
        if '!= "action_required" ]; then' in s:
            break
        if re.match(r"^(if|while|until|case)\b", s):
            body_depth += 1
        elif s in ("fi", "done", "esac"):
            body_depth -= 1
    assert (
        significant
        and significant[0].startswith("attempt1_conclusion=$(gh api")
        and body_depth == 0
    ), "the attempt-1 probe must BE the guard's body, not sit behind another conditional"
    # The branch must SKIP the run, not just log. Depth-tracking a `continue` has to
    # enumerate every construct that can nest one, and the enumeration keeps leaking: a
    # `{ if false; then` / `fi; }` block reads as top-level because the brace opens no
    # tracked depth while `fi; }` closes one. Whitelist the body instead -- log lines, then
    # a `continue` as the last command. `(?!=)` still rejects `continue=false`, an
    # assignment, and `continue 1` / `continue ; :` stay accepted, both being equivalent.
    reject_body = _reject_branch_body(step)
    assert reject_body and _CONTINUE.match(reject_body[-1]), (
        "the reject branch must end in a top-level `continue`, or the gate is inert",
        reject_body,
    )
    assert all(
        l.startswith("echo ") for l in reject_body[:-1]
    ), f"the reject branch must only log before it continues: {reject_body}"
    # The projection (pinned as stage 3 above) and the two splits must agree on field order:
    # transposing either makes run_id and run_attempt swap, so the probe reads a nonexistent
    # run and rejects every gated candidate while every assert above still passes.
    assert 'run_id="${run_entry%%:*}"' in step, "run id must come from the head field"
    assert 'run_attempt="${run_entry##*:}"' in step, "attempt must come from the tail field"
    # The candidate's data path, whitelisted whole. Censusing the ASSIGNMENTS in it has to
    # enumerate the separators one can follow, and that enumeration leaks the same way the
    # `continue` depth-tracking above did: an assignment after `&&`, `||` or `&`, inside a
    # `{ ... }` group, or via `read`/`printf -v`/`declare` carries no separator the census
    # looks for. Forging run_attempt (to 1) makes EVERY attempt-2 candidate eligible, so the
    # never-retried contract this PR adds goes unpinned; forging run_id or
    # attempt1_conclusion makes the gate inert instead. So pin the region's commands.
    assert _data_path_commands(step) == EXPECTED_DATA_PATH, (
        "the candidate's data path must be exactly EXPECTED_DATA_PATH",
        _data_path_commands(step),
    )
    # A gated attempt 1 has status "completed" and conclusion "action_required", so a probe
    # reading any other field rejects every fork candidate while the asserts above still pass.
    assert (
        "--jq '.conclusion'" in normalized
    ), "the probe must read the conclusion field"
    # Without the fallback a transient API error aborts the step under "set -euo pipefail",
    # taking down the whole hourly retry rather than skipping one run.
    assert (
        '2>/dev/null || echo ""' in normalized
    ), "the probe must fail closed on an API error"
    # The assertions above pin the probe's PARTS but not the whole: swapping the run
    # identifier ($run_id -> $run_attempt) keeps "/attempts/1" present while the probe reads
    # a nonexistent run (404 -> empty -> every gated fork candidate skipped), and extending
    # the reject condition with an always-true disjunct keeps its substring present while
    # skipping every attempt-2 candidate. Both leave the fix inert with CI green.
    assert (
        PROBE_CMD in normalized
    ), "the attempt-1 probe command must be exactly PROBE_CMD"
    # ...and it must be the SOLE assignment: appending `attempt1_conclusion="failure"`
    # after the correct command satisfies the substring check above while the reject
    # branch then skips every attempt-2 candidate.
    assigns = re.findall(r"^\s*attempt1_conclusion=(.*)$", step, re.M)
    assert len(assigns) == 1, assigns
    # ...and it must be the SOLE test of the probe's result: an added disjunct would
    # otherwise satisfy the equality above while making the branch unconditional.
    conds = [
        l.strip()
        for l in step.splitlines()
        if "$attempt1_conclusion" in l and l.strip().startswith("if ")
    ]
    assert conds == [REJECT_COND], conds
    # The asserts above pin the gate and everything up to the reject condition, but the region
    # from the gate's own `fi` to `gh run rerun` is unconstrained: re-emitting the guard there
    # with a bare `continue`, or forcing `should_rerun=false` for an attempt-2 candidate just
    # before the rerun, skips every gated fork run while every assertion above still passes.
    # So pin that exactly ONE line branches on run_attempt -- the gate -- and that run_attempt
    # is assigned exactly once. Other reads (the rerun log line) are free.
    attempt_reads = [
        s
        for s in (" ".join(l.split()) for l in step.splitlines())
        if s
        and not s.startswith("#")
        and not re.match(r"^run_attempt=", s)
        and re.search(r"\brun_attempt\b", s)
    ]
    branching = [
        s
        for s in attempt_reads
        if re.search(r"^(if|while|until|case|elif)\b|&&|\|\||\bcontinue\b|\bbreak\b|\breturn\b", s)
    ]
    assert branching == [GUARD], (
        "exactly one line may branch on run_attempt (the gate); a second one makes it inert",
        branching,
    )
    assert (
        len(re.findall(r"^\s*run_attempt=", step, re.M)) == 1
    ), "run_attempt must be assigned once"


def _run_step(
    tmp_path, attempt, attempt1_conclusion, age_hours, jobs_json, attempt1_fail=False
):
    """Run the real step over a one-row fixture. Returns (proc, rerun_log_text).

    ``attempt1_conclusion`` becomes the ``conclusion`` of a full attempt payload whose
    ``status`` is always ``completed`` (what the real API returns for a gated attempt), so a
    probe reading the wrong field gets ``completed`` rather than the expected value.
    """
    created_at = (
        datetime.datetime.now(datetime.timezone.utc)
        - datetime.timedelta(hours=age_hours)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")
    fixture = tmp_path / "runs.json"
    fixture.write_text(
        json.dumps([{"databaseId": RUN_ID, "attempt": attempt, "createdAt": created_at}])
    )
    rerun_log = tmp_path / "reruns.log"
    rerun_log.write_text("")
    gh = tmp_path / "gh"
    gh.write_text(GH_STUB)
    gh.chmod(gh.stat().st_mode | stat.S_IEXEC)

    proc = subprocess.run(
        ["bash", "-c", _retry_step()],
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "PATH": f"{tmp_path}:{os.environ['PATH']}",
            "GH_REPO": "ClickHouse/ClickHouse",
            "GH_TOKEN": "stub",
            "FIXTURE": str(fixture),
            "ATTEMPT1_JSON": json.dumps(
                {
                    "status": "completed",
                    "conclusion": attempt1_conclusion,
                    "run_attempt": 1,
                }
            ),
            "ATTEMPT1_FAIL": "1" if attempt1_fail else "",
            "JOBS_JSON": jobs_json,
            "RERUN_LOG": str(rerun_log),
        },
    )
    assert proc.returncode == 0, proc.stderr
    return proc, rerun_log.read_text()


@pytest.mark.skipif(
    not shutil.which("jq"),
    reason="needs jq; absent from the CI Tests image, so CI relies on the static asserts",
)
@pytest.mark.parametrize(
    "attempt, attempt1_conclusion, age_hours, selected",
    [
        (1, "failure", 1, True),  # same-repo run, first real attempt
        (2, "action_required", 1, True),  # fork run, gate consumed attempt 1
        (2, "failure", 1, False),  # genuinely retried once already
        (3, "failure", 1, False),  # retried twice
        (3, "action_required", 1, False),  # gated, then retried: past the attempt bound
        (1, "failure", 9, False),  # first attempt but older than the cutoff
    ],
)
def test_eligibility(tmp_path, attempt, attempt1_conclusion, age_hours, selected):
    """Assert whether the step considers a run at all (selector coverage)."""
    proc, _ = _run_step(tmp_path, attempt, attempt1_conclusion, age_hours, NO_JOBS)
    considered = f"actions/runs/{RUN_ID} " in proc.stdout
    assert considered is selected, proc.stdout


@pytest.mark.skipif(
    not shutil.which("jq"),
    reason="needs jq; absent from the CI Tests image, so CI relies on the static asserts",
)
@pytest.mark.parametrize(
    "attempt1_conclusion, rerun_expected",
    [
        ("action_required", True),  # gated fork run: the case this gate exists for
        ("failure", False),  # already retried: rejected despite an infra-looking failure
    ],
)
def test_gated_fork_run_is_actually_rerun(tmp_path, attempt1_conclusion, rerun_expected):
    """An attempt-2 candidate with an infra failure must reach ``gh run rerun`` iff the
    attempt-1 probe says its first attempt was the approval gate."""
    proc, rerun_log = _run_step(tmp_path, 2, attempt1_conclusion, 1, INFRA_JOBS)
    assert (f"run rerun {RUN_ID}" in rerun_log) is rerun_expected, (
        proc.stdout,
        rerun_log,
    )
    if rerun_expected:
        # The fixture is attempt 2, so the rerun it triggers is attempt 3.
        assert "/attempts/3" in proc.stdout, proc.stdout


@pytest.mark.skipif(
    not shutil.which("jq"),
    reason="needs jq; absent from the CI Tests image, so CI relies on the static asserts",
)
def test_attempt1_probe_fails_closed_on_api_error(tmp_path):
    """A failing attempt-1 API call must skip the run, not abort the whole tick: the empty
    fallback value is not ``action_required``, so the candidate is rejected and the loop
    continues. Without the fallback, ``set -euo pipefail`` would kill the step instead."""
    proc, rerun_log = _run_step(
        tmp_path, 2, "action_required", 1, INFRA_JOBS, attempt1_fail=True
    )
    # _run_step already asserts returncode 0, i.e. the step survived the API error.
    # The empty conclusion in the skip message is the fallback's own value, so this also
    # proves the probe was reached rather than the run being dropped earlier.
    assert f"Skipping run {RUN_ID}: attempt 1 concluded ''" in proc.stdout, proc.stdout
    assert f"run rerun {RUN_ID}" not in rerun_log, rerun_log
