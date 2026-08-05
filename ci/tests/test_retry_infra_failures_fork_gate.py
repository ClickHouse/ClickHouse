"""Guards for the auto-retry's "has never been retried" selector.

``.github/workflows/retry_infra_failures.yml`` retries infra-killed PR runs hourly.
A fork PR's approval gate concludes ``run_attempt`` 1 as ``action_required`` with zero
jobs, so its first attempt that actually runs anything is 2, and a selector keyed on
``attempt == 1`` can never see it. The invariant is "exactly one attempt executed":
attempt 1 for a same-repo run, attempt 2 behind an approval gate.

The step is read as text (like ``test_create_release.py``) so no YAML parser is needed,
and the behavioural test feeds the step's own ``--jq`` expression to real ``jq``, so the
listing filter is covered as well as the per-run probe.
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


def _listing_select_conjuncts(step):
    """The conjuncts of the ``gh run list --jq`` listing filter's ``select()``.

    Anchored to that invocation: the step has nine ``select(`` occurrences and only the
    first belongs to the listing filter.
    """
    assert "gh run list" in step, step
    invocation = step.split("gh run list", 1)[1].split("\n\n", 1)[0]
    m = re.search(r"select\((.*?)\)", invocation)
    assert m, invocation
    return {c.strip() for c in m.group(1).split(" and ")}


def test_selector_admits_gated_fork_runs():
    step = _retry_step()
    # A substring assertion cannot see an ADDED conjunct: `select(.attempt <= 2 and
    # .attempt == 1 ...)` keeps every fork run out while still containing
    # "select(.attempt <= 2".
    assert _listing_select_conjuncts(step) == {
        ".attempt <= 2",
        r".createdAt >= \"$cutoff\"",
    }, "the listing filter must be exactly the attempt bound and the cutoff"
    assert 'attempt1_conclusion" != "action_required"' in step, "missing attempt-1 probe"
    # The probe must interrogate attempt 1: reading any other attempt makes it meaningless.
    assert "/attempts/1" in step, "the probe must read attempt 1"
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
    # And its reject branch must actually skip the run, not merely log. Scope the search to
    # that branch's own body: a "continue" belonging to a later loop would satisfy this too.
    reject_branch = re.split(
        r"\n\s*fi\b", step.split('!= "action_required" ]; then', 1)[1], maxsplit=1
    )[0]
    assert re.search(
        r"\n\s*continue\b", reject_branch
    ), "the reject branch must continue, or the gate is inert"
    # The projection and the two splits must agree on field order: transposing either makes
    # run_id and run_attempt swap, so the probe reads a nonexistent run and rejects every
    # gated candidate while every assert above still passes.
    assert r'\"\(.databaseId):\(.attempt)\"' in step, "projection must emit id:attempt"
    assert 'run_id="${run_entry%%:*}"' in step, "run id must come from the head field"
    assert 'run_attempt="${run_entry##*:}"' in step, "attempt must come from the tail field"
    # Presence is not enough: a later `run_id="${run_entry##*:}"` would overwrite the correct
    # value from the wrong field while every assertion above still passes.
    for var, expansion in (("run_id", "%%:*"), ("run_attempt", "##*:")):
        assigns = re.findall(rf'^\s*{var}="\$\{{run_entry([^}}]*)\}}"', step, re.M)
        assert assigns == [expansion], (var, assigns)
    # A gated attempt 1 has status "completed" and conclusion "action_required", so a probe
    # reading any other field rejects every fork candidate while the asserts above still pass.
    assert "--jq '.conclusion'" in step, "the probe must read the conclusion field"
    # Without the fallback a transient API error aborts the step under "set -euo pipefail",
    # taking down the whole hourly retry rather than skipping one run.
    assert '2>/dev/null || echo ""' in step, "the probe must fail closed on an API error"


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
