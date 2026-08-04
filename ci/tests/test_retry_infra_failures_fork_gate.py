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
if [ "$1" = "run" ] && [ "$2" = "list" ]; then
    # Evaluate the caller's own --jq expression against the fixture listing.
    while [ $# -gt 0 ]; do
        if [ "$1" = "--jq" ]; then jq_expr="$2"; fi
        shift
    done
    jq -r "$jq_expr" "$FIXTURE"
    exit 0
fi
case "$*" in
    *attempts/1*) echo "$ATTEMPT1_CONCLUSION" ;;
    *'/jobs?'*) echo '{"jobs": []}' ;;
    *) echo '{}' ;;
esac
"""


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


def test_selector_admits_gated_fork_runs():
    step = _retry_step()
    assert "select(.attempt <= 2" in step, "listing filter must not exclude attempt 2"
    assert not re.search(r"select\(\.attempt == 1\b", step), "attempt == 1 is fork-blind"
    assert 'attempt1_conclusion" != "action_required"' in step, "missing attempt-1 probe"


@pytest.mark.skipif(not shutil.which("jq"), reason="the workflow step needs jq")
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
    """Run the real step over a one-row fixture and assert whether it is considered."""
    created_at = (
        datetime.datetime.now(datetime.timezone.utc)
        - datetime.timedelta(hours=age_hours)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")
    fixture = tmp_path / "runs.json"
    fixture.write_text(
        json.dumps([{"databaseId": RUN_ID, "attempt": attempt, "createdAt": created_at}])
    )
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
            "ATTEMPT1_CONCLUSION": attempt1_conclusion,
        },
    )
    assert proc.returncode == 0, proc.stderr
    considered = f"actions/runs/{RUN_ID} " in proc.stdout
    assert considered is selected, proc.stdout
