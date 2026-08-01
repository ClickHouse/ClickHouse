"""
Regression coverage for the praktika result contract of the two SQLancer job
scripts, `ci/jobs/sqlancer_job.sh` and `ci/jobs/sqlancer_pp_job.sh`.

Both scripts hand-write their `result_<normalized_job_name>.json` in bash
instead of going through `Result.complete_job`, so they have to satisfy three
invariants by hand. All three were violated, and the combined effect was that a
red SQLancer job reported green in the GitHub Actions conclusion and in the
workflow report, with only CIDB recording the failure:

  - the result's `name` must equal the raw `JOB_NAME`, because
    `Result.update_sub_result` merges the job result into the workflow report by
    NAME. Both scripts hardcoded `"SQLancer"` / `"SQLancerPP"` while the real
    nodes are `SQLancer (arm_asan_ubsan)` / `SQLancerPP (arm_asan_ubsan)`. The
    merge loop has no else branch, so the mismatch was silent and the node kept
    its pre-run status and a `null` duration.
  - the process must exit non-zero when the job did not pass, because
    `runner.py` derives the step result from the exit status (`res = run_code ==
    0`) and not from the result file. Both scripts recorded a failure and then
    ran a server-teardown loop that succeeded, so they exited 0.
  - the `status` field must be a `Result.Status` token.
    `sqlancer_pp_job.sh` emitted lowercase `success` / `failure`, which
    `Result._update_status` does not count as failed, so fixing only the name
    left that job green.

The status-token and merge assertions exercise `ci.praktika.result` directly;
the exit-status assertions run the scripts' own shell text, extracted verbatim,
so that reverting any of the three fixes in the scripts reddens this test.
"""

import json
import os
import re
import subprocess
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.praktika.result import Result
from ci.praktika.utils import Utils

_CI_DIR = os.path.join(os.path.dirname(__file__), "..")
_SQLANCER_JOB = os.path.join(_CI_DIR, "jobs", "sqlancer_job.sh")
_SQLANCER_PP_JOB = os.path.join(_CI_DIR, "jobs", "sqlancer_pp_job.sh")

# The parametrized workflow node names, as `parametrize()` renders them.
_SQLANCER_NODE = "SQLancer (arm_asan_ubsan)"
_SQLANCER_PP_NODE = "SQLancerPP (arm_asan_ubsan)"


def _read(path):
    with open(path, encoding="utf-8") as f:
        return f.read()


def _extract_block(text, start_pattern, path):
    """Lines from the line matching start_pattern up to the next `}` at column 0."""
    lines = text.splitlines()
    start = None
    for i, line in enumerate(lines):
        if re.match(start_pattern, line):
            start = i
            break
    assert start is not None, f"{start_pattern!r} not found in {path}"
    for j in range(start + 1, len(lines)):
        if lines[j] == "}":
            return "\n".join(lines[start : j + 1])
    raise AssertionError(f"no closing brace for {start_pattern!r} in {path}")


def _tail_from(text, marker):
    """Everything from the line starting with marker to the end of the file."""
    lines = text.splitlines()
    for i, line in enumerate(lines):
        if line.startswith(marker):
            return "\n".join(lines[i:])
    raise AssertionError(f"marker {marker!r} not found")


def _workflow_snapshot(node_names):
    """A workflow result as praktika holds it while jobs are still running."""
    return Result.from_dict(
        {
            "name": "NightlySQLancer",
            "status": Result.Status.OK,
            "start_time": 1,
            "duration": 1,
            "results": [
                {
                    "name": name,
                    "status": Result.Status.OK,
                    "start_time": 1,
                    "duration": None,
                    "results": [],
                }
                for name in node_names
            ],
        }
    )


def _merge(node_name, emitted_name, emitted_status):
    """Merge a job result into the report the way `hook_html.post_run` does.

    Returns (workflow_status, node_status, node_duration).
    """
    workflow = _workflow_snapshot([node_name])
    job_result = Result.from_dict(
        {
            "name": emitted_name,
            "status": emitted_status,
            "start_time": 1,
            "duration": 3600,
            "info": "Some SQLancer tests failed",
            "results": [
                {
                    "name": "NoREC",
                    "status": Result.Status.FAIL,
                    "start_time": 1,
                    "duration": 1,
                    "info": "java.lang.AssertionError: ...",
                    "results": [],
                }
            ],
        }
    )
    workflow.update_sub_result(job_result, drop_nested_results=True)
    node = next(r for r in workflow.results if r.name == node_name)
    return workflow.status, node.status, node.duration


# ---------------------------------------------------------------------------
# The name invariant: a mismatched name is silently dropped by the merge.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "node_name, hardcoded_name",
    [(_SQLANCER_NODE, "SQLancer"), (_SQLANCER_PP_NODE, "SQLancerPP")],
)
def test_hardcoded_name_is_silently_dropped(node_name, hardcoded_name):
    """The pre-fix shape: a FAIL under the wrong name leaves the report green."""
    workflow_status, node_status, node_duration = _merge(
        node_name, hardcoded_name, Result.Status.FAIL
    )
    assert workflow_status == Result.Status.OK
    assert node_status == Result.Status.OK
    # `duration: None` is the on-disk fingerprint of a node that was never merged.
    assert node_duration is None


@pytest.mark.parametrize("node_name", [_SQLANCER_NODE, _SQLANCER_PP_NODE])
def test_job_name_as_result_name_propagates_failure(node_name):
    """With `name == JOB_NAME` the failure reaches the report and the rollup."""
    workflow_status, node_status, node_duration = _merge(
        node_name, node_name, Result.Status.FAIL
    )
    assert workflow_status == Result.Status.FAIL
    assert node_status == Result.Status.FAIL
    assert node_duration == 3600


@pytest.mark.parametrize(
    "node_name, expected_file",
    [
        (_SQLANCER_NODE, "result_sqlancer_arm_asan_ubsan.json"),
        (_SQLANCER_PP_NODE, "result_sqlancerpp_arm_asan_ubsan.json"),
    ],
)
def test_result_file_path_is_unchanged_by_the_name_fix(node_name, expected_file):
    """The fix touches the `name` field only; the file praktika reads is the same.

    The expected names are spelled out rather than recomputed from
    `normalize_string`, so that a change to the normalization is caught here
    instead of being mirrored by the assertion.
    """
    assert Utils.normalize_string(node_name) == expected_file[len("result_") : -len(".json")]
    assert Result.file_name_static(node_name).endswith(expected_file)


# ---------------------------------------------------------------------------
# The status-token invariant: lowercase tokens do not count as failed.
# ---------------------------------------------------------------------------


def test_lowercase_status_does_not_fail_the_workflow():
    """Why fixing the name alone left SQLancerPP green."""
    workflow_status, node_status, _ = _merge(
        _SQLANCER_PP_NODE, _SQLANCER_PP_NODE, "failure"
    )
    assert workflow_status == Result.Status.OK
    assert node_status == "failure"


@pytest.mark.parametrize("script", [_SQLANCER_JOB, _SQLANCER_PP_JOB])
def test_scripts_only_assign_valid_status_tokens(script):
    """Every `OVERALL_STATUS=` and `TEST_RESULTS+=` token is a `Result.Status`."""
    valid = {v for k, v in vars(Result.Status).items() if not k.startswith("_")}
    text = _read(script)

    assigned = set(re.findall(r'^\s*OVERALL_STATUS="?([A-Za-z]+)"?\s*$', text, re.M))
    assert assigned, f"no OVERALL_STATUS assignments found in {script}"
    assert assigned <= valid, f"invalid status tokens in {script}: {assigned - valid}"

    leaves = set(re.findall(r'TEST_RESULTS\+=\("\$\{\w+\},([A-Za-z]+),', text))
    assert leaves, f"no TEST_RESULTS rows found in {script}"
    assert leaves <= valid, f"invalid leaf status tokens in {script}: {leaves - valid}"


# ---------------------------------------------------------------------------
# The exit-status invariant, measured by running the scripts' own shell text.
# ---------------------------------------------------------------------------


def _run_sqlancer_write_result(tmp_path, overall_status):
    """Run `sqlancer_job.sh`'s real `write_result` (via its EXIT trap).

    Returns (exit_code, parsed_result_json).
    """
    text = _read(_SQLANCER_JOB)
    harness = "\n".join(
        [
            "set -exu",
            "set -o pipefail",
            f'TMP_PATH="{tmp_path}"',
            f'RESULT_FILE="{tmp_path}/result_test.json"',
            f'OUTPUT_PATH="{tmp_path}/out"',
            'mkdir -p "$OUTPUT_PATH"',
            "JOB_START_TIME=$(date +%s)",
            f'JOB_NAME_RAW="{_SQLANCER_NODE}"',
            _extract_block(text, r"^json_escape\(\) \{$", _SQLANCER_JOB),
            _extract_block(text, r"^write_result\(\) \{$", _SQLANCER_JOB),
            "TEST_RESULTS=()",
            "ATTACHED_FILES_ARRAY=()",
            'OVERALL_STATUS="ERROR"',
            "trap write_result EXIT",
            'TEST_RESULTS+=("SQLancer,FAIL,java.lang.AssertionError: boom")',
            f"OVERALL_STATUS={overall_status}",
            # The real tail from the reproducer-log block to EOF: artifact
            # attachment and the server-teardown loop. That loop succeeding is
            # what made the script exit 0 despite a recorded failure.
            "wget() { return 1; }",
            "sleep() { return 0; }",
            f'SQLANCER_LOG_DIR="{tmp_path}/nonexistent-logs"',
            f'PID_FILE="{tmp_path}/nonexistent.pid"',
            _tail_from(text, "# On failure, attach the per-database reproducer"),
        ]
    )
    script = tmp_path / "harness.sh"
    script.write_text(harness, encoding="utf-8")
    proc = subprocess.run(
        ["bash", str(script)], capture_output=True, text=True, timeout=120
    )
    with open(tmp_path / "result_test.json", encoding="utf-8") as f:
        return proc.returncode, json.load(f)


def test_sqlancer_write_result_exits_nonzero_on_fail(tmp_path):
    exit_code, result = _run_sqlancer_write_result(tmp_path, "FAIL")
    assert exit_code != 0, "a recorded FAIL must not exit 0, or the CI step stays green"
    # The result file is still complete: the exit happens after it is written.
    assert result["status"] == Result.Status.FAIL
    assert result["name"] == _SQLANCER_NODE
    assert result["results"][0]["status"] == Result.Status.FAIL


def test_sqlancer_write_result_exits_zero_on_ok(tmp_path):
    exit_code, result = _run_sqlancer_write_result(tmp_path, "OK")
    assert exit_code == 0, "a passing job must still exit 0"
    assert result["status"] == Result.Status.OK


def test_sqlancer_write_result_emits_job_name_verbatim(tmp_path):
    """The emitted JSON round-trips and its `name` is exactly `JOB_NAME`.

    `JOB_NAME` contains spaces and parentheses, which is why the value goes
    through `json_escape` rather than a bare `printf`.
    """
    _, result = _run_sqlancer_write_result(tmp_path, "FAIL")
    assert result["name"] == _SQLANCER_NODE
    assert " " in result["name"] and "(" in result["name"]


def _run_sqlancer_pp_tail(tmp_path, overall_status):
    """Run `sqlancer_pp_job.sh`'s real tail: the result-writing block to EOF.

    `sqlancer_pp_job.sh` has no EXIT trap, so its result JSON and its exit
    status both come from the tail of the script. Everything from the JSON
    block's opening brace to the end of the file is run verbatim, with the
    server-facing commands stubbed out.
    """
    text = _read(_SQLANCER_PP_JOB)
    lines = text.splitlines()
    tail = "\n".join(lines[lines.index("{") :])

    harness = "\n".join(
        [
            "set -exu",
            f'TMP_PATH="{tmp_path}"',
            f'OUTPUT_PATH="{tmp_path}/out"',
            'mkdir -p "$OUTPUT_PATH"',
            f'RESULT_FILE="{tmp_path}/result_test.json"',
            "JOB_START_TIME=$(date +%s)",
            f'JOB_NAME_RAW="{_SQLANCER_PP_NODE}"',
            _extract_block(text, r"^json_escape\(\) \{$", _SQLANCER_PP_JOB),
            f"OVERALL_STATUS={overall_status}",
            'TEST_RESULTS=("NoREC,FAIL,exit=1; boom")',
            "ATTACHED_FILES_ARRAY=()",
            # Stub the commands that would talk to a server or reap processes.
            "wget() { return 1; }",
            "pkill() { return 0; }",
            "sleep() { return 0; }",
            tail,
        ]
    )
    script = tmp_path / "harness_pp.sh"
    script.write_text(harness, encoding="utf-8")
    proc = subprocess.run(
        ["bash", str(script)], capture_output=True, text=True, timeout=120
    )
    with open(tmp_path / "result_test.json", encoding="utf-8") as f:
        return proc.returncode, json.load(f)


def test_sqlancer_pp_exits_nonzero_on_fail(tmp_path):
    exit_code, result = _run_sqlancer_pp_tail(tmp_path, "FAIL")
    assert exit_code != 0, "a recorded FAIL must not exit 0, or the CI step stays green"
    assert result["status"] == Result.Status.FAIL
    assert result["name"] == _SQLANCER_PP_NODE


def test_sqlancer_pp_exits_zero_on_ok(tmp_path):
    exit_code, result = _run_sqlancer_pp_tail(tmp_path, "OK")
    assert exit_code == 0, "a passing job must still exit 0"
    assert result["status"] == Result.Status.OK


@pytest.mark.parametrize(
    "runner, node",
    [
        (_run_sqlancer_write_result, _SQLANCER_NODE),
        (_run_sqlancer_pp_tail, _SQLANCER_PP_NODE),
    ],
)
def test_emitted_result_reaches_the_report(tmp_path, runner, node):
    """End to end: what each script writes on failure does fail the workflow.

    This is the assertion that ties the three fixes together: the emitted `name`
    has to match the node AND the emitted `status` has to be a `Result.Status`
    token, or the merge leaves the report green.
    """
    _, emitted = runner(tmp_path, "FAIL")
    workflow_status, node_status, node_duration = _merge(
        node, emitted["name"], emitted["status"]
    )
    assert workflow_status == Result.Status.FAIL
    assert node_status == Result.Status.FAIL
    assert node_duration is not None


def test_pp_failure_path_emits_a_status_that_fails_the_report(tmp_path):
    """The pp failure token is taken from the script, not supplied by the test.

    `_run_sqlancer_pp_tail` sets `OVERALL_STATUS` itself, so it cannot catch a
    regression in the value the oracle loop assigns. This runs the real loop
    body's failure branch and feeds whatever it produces into the merge, which is
    what makes a lowercase token visible end to end.
    """
    text = _read(_SQLANCER_PP_JOB)
    failure_assignments = re.findall(
        r'TEST_RESULTS\+=\("\$\{ORACLE\},(?:FAIL|ERROR),[^\n]*\n\s*OVERALL_STATUS="([^"]+)"',
        text,
    )
    assert failure_assignments, "no oracle failure branch found in sqlancer_pp_job.sh"

    for status in set(failure_assignments):
        workflow_status, node_status, _ = _merge(
            _SQLANCER_PP_NODE, _SQLANCER_PP_NODE, status
        )
        assert workflow_status == Result.Status.FAIL, (
            f"the oracle failure branch assigns {status!r}, which does not fail "
            "the workflow rollup"
        )
        assert node_status == Result.Status.FAIL
