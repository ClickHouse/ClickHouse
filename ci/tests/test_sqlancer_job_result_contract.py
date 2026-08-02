"""
Regression coverage for the praktika result contract of the two SQLancer job
scripts, `ci/jobs/sqlancer_job.sh` and `ci/jobs/sqlancer_pp_job.sh`.

Both scripts hand-write their `result_<normalized_job_name>.json` in bash
instead of going through `Result.complete_job`, so they have to satisfy four
invariants by hand. The first three were violated, and the combined effect was
that a red SQLancer job reported green in the GitHub Actions conclusion and in
the workflow report, with only CIDB recording the failure:

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
  - every embedded string must be JSON-escaped, because `Result.from_fs`
    re-raises `JSONDecodeError` and its caller in `runner.py` does not catch it,
    so one unescaped backslash in a failure message discards the whole result.

The status-token and merge assertions exercise `ci.praktika.result` directly;
the exit-status assertions run the scripts' own shell text, extracted verbatim,
so that reverting any of the fixes in the scripts reddens this test. That
includes the regions that actually *produce* the values under test: the
`JOB_NAME_RAW` / `NORMALIZED_JOB_NAME` / `RESULT_FILE` block (so a hardcoded or
empty name reddens, and so does writing to a file praktika does not read) and
`sqlancer_pp_job.sh`'s oracle loop (so a regression in the status token or in the
`info` it builds reddens).
"""

import dataclasses
import json
import os
import re
import shlex
import subprocess
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.praktika._environment import _Environment
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


def _extract_compound(text, start_marker, end_line, path):
    """Lines from the line starting with start_marker to the next end_line.

    Keyed on the text, never on line numbers, so it survives edits elsewhere in
    the script. `end_line` is matched at column 0, i.e. the closing keyword of a
    top-level compound statement.
    """
    lines = text.splitlines()
    start = None
    for i, line in enumerate(lines):
        if line.startswith(start_marker):
            start = i
            break
    assert start is not None, f"{start_marker!r} not found in {path}"
    for j in range(start + 1, len(lines)):
        if lines[j] == end_line:
            return "\n".join(lines[start : j + 1])
    raise AssertionError(f"no closing {end_line!r} for {start_marker!r} in {path}")


def _extract_upto_prefix(text, start_marker, end_prefix, path):
    """Lines from start_marker through the first later line starting with end_prefix.

    Keyed on the text, never on line numbers. Unlike `_extract_compound` the last
    line is a simple assignment rather than a compound-statement terminator.
    """
    lines = text.splitlines()
    start = None
    for i, line in enumerate(lines):
        if line.startswith(start_marker):
            start = i
            break
    assert start is not None, f"{start_marker!r} not found in {path}"
    for j in range(start + 1, len(lines)):
        if lines[j].startswith(end_prefix):
            return "\n".join(lines[start : j + 1])
    raise AssertionError(f"no {end_prefix!r} line after {start_marker!r} in {path}")


def _extract_job_name_init(text, path):
    """The script's `JOB_NAME_RAW`, `NORMALIZED_JOB_NAME` and `RESULT_FILE` block.

    Both scripts read `JOB_NAME` out of the serialized praktika environment with
    an inline `python3 -c`, because `JOB_NAME` is not propagated into the docker
    container. Extracting it verbatim is what makes a regression there (a
    hardcoded literal, or a lookup that degrades to an empty string) visible: an
    empty `name` no-ops `update_sub_result` exactly like a wrong one.

    The range runs to `RESULT_FILE=` inclusive, so the harnesses also execute the
    normalization that decides *which file* praktika reads. Stopping at the first
    standalone `')` would leave that computation uncovered, and writing a plain
    `result.json` is the original defect this contract exists to pin: praktika
    then reports "Job killed or terminated, no Result provided".
    """
    return _extract_upto_prefix(text, "JOB_NAME_RAW=$(python3 -c '", "RESULT_FILE=", path)


def _write_environment_json(root, job_name, workflow_name="NightlySQLancer"):
    """Materialize the `ci/tmp/environment.json` that the init block reads.

    `_Environment.file_name_static` is `f"{Settings.TEMP_DIR}/{cls.name}.json"`
    with `TEMP_DIR = "./ci/tmp"`, i.e. relative to the *process* cwd, so the
    harness has to run from a scratch tree rather than the repo. The field list
    is generated from the dataclass instead of spelled out: a partial dict makes
    `_Environment.from_fs` raise `TypeError: missing N required positional
    arguments`, so hardcoding it would rot the moment a field is added.
    """
    required = [
        f.name
        for f in dataclasses.fields(_Environment)
        if f.default is dataclasses.MISSING and f.default_factory is dataclasses.MISSING
    ]
    env = {name: (0 if name == "PR_NUMBER" else "") for name in required}
    env["JOB_NAME"] = job_name
    env["WORKFLOW_NAME"] = workflow_name

    tmp_dir = os.path.join(root, "ci", "tmp")
    os.makedirs(tmp_dir, exist_ok=True)
    with open(os.path.join(tmp_dir, "environment.json"), "w", encoding="utf-8") as f:
        json.dump(env, f)


def _make_praktika_tree(root, job_name, workflow_name="NightlySQLancer"):
    """A scratch cwd from which the extracted init block resolves `ci.praktika`.

    Symlinks the real `ci/praktika` and `ci/settings` rather than copying, so the
    block runs against the in-tree framework, and keeps the generated
    `environment.json` out of the shared worktree.
    """
    ci_dir = os.path.join(root, "ci")
    os.makedirs(ci_dir, exist_ok=True)
    repo_ci = os.path.abspath(_CI_DIR)
    for name in ("praktika", "settings"):
        link = os.path.join(ci_dir, name)
        if not os.path.lexists(link):
            os.symlink(os.path.join(repo_ci, name), link)
    _write_environment_json(root, job_name, workflow_name)
    return root


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


def _read_emitted_result(tmp_path, proc, job_name):
    """Parse the result file at the path the *script* computed, not a fixed one.

    The harnesses run the script's own `RESULT_FILE=` assignment, so the path is
    evidence: praktika reads `Result.file_name_static(JOB_NAME)` and nothing else.
    Asserting the emitted path against it is what makes a regression in
    `NORMALIZED_JOB_NAME` (a hardcoded literal, or an empty value collapsing the
    name to `result_.json`) redden, rather than being re-derived in Python here.
    """
    emitted = re.findall(r"^RESULT_FILE_IS=(.*)$", proc.stdout, re.M)
    assert len(emitted) == 1, f"expected one RESULT_FILE_IS line, got {emitted}"
    path = emitted[0]
    assert os.path.basename(path) == os.path.basename(
        Result.file_name_static(job_name)
    ), f"the script writes {path!r}, which is not the file praktika reads"
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _run_sqlancer_write_result(tmp_path, overall_status, job_name=_SQLANCER_NODE):
    """Run `sqlancer_job.sh`'s real `write_result` (via its EXIT trap).

    The `JOB_NAME_RAW` lookup and the `RESULT_FILE` computation are the script's
    own, so both the emitted `name` and the file it lands in come from the real
    `_Environment` read rather than from this test. Returns
    (exit_code, parsed_result_json).
    """
    text = _read(_SQLANCER_JOB)
    cwd = _make_praktika_tree(str(tmp_path / "cwd"), job_name)
    harness = "\n".join(
        [
            "set -exu",
            "set -o pipefail",
            f'TMP_PATH="{tmp_path}"',
            f'OUTPUT_PATH="{tmp_path}/out"',
            'mkdir -p "$OUTPUT_PATH"',
            "JOB_START_TIME=$(date +%s)",
            _extract_job_name_init(text, _SQLANCER_JOB),
            'printf "RESULT_FILE_IS=%s\\n" "$RESULT_FILE"',
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
        ["bash", str(script)], capture_output=True, text=True, timeout=120, cwd=cwd
    )
    return proc.returncode, _read_emitted_result(tmp_path, proc, job_name)


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


def _run_sqlancer_pp_tail(
    tmp_path, overall_status, job_name=_SQLANCER_PP_NODE, rows=None
):
    """Run `sqlancer_pp_job.sh`'s real tail: the result-writing block to EOF.

    `sqlancer_pp_job.sh` has no EXIT trap, so its result JSON and its exit
    status both come from the tail of the script. Everything from the JSON
    block's opening brace to the end of the file is run verbatim, with the
    server-facing commands stubbed out. `OVERALL_STATUS` is supplied here;
    `_run_sqlancer_pp_oracle_loop` covers the code that assigns it. `rows`
    overrides the `TEST_RESULTS` rows, so a caller can drive a raw `info` payload
    through the script's own serialization.
    """
    text = _read(_SQLANCER_PP_JOB)
    lines = text.splitlines()
    tail = "\n".join(lines[lines.index("{") :])
    cwd = _make_praktika_tree(str(tmp_path / "cwd"), job_name)
    if rows is None:
        rows = ["NoREC,FAIL,exit=1; boom"]
    # shlex.quote, not an f-string: a row may itself contain a double quote.
    rows_literal = " ".join(shlex.quote(row) for row in rows)

    harness = "\n".join(
        [
            "set -exu",
            f'TMP_PATH="{tmp_path}"',
            f'OUTPUT_PATH="{tmp_path}/out"',
            'mkdir -p "$OUTPUT_PATH"',
            "JOB_START_TIME=$(date +%s)",
            _extract_job_name_init(text, _SQLANCER_PP_JOB),
            'printf "RESULT_FILE_IS=%s\\n" "$RESULT_FILE"',
            _extract_block(text, r"^json_escape\(\) \{$", _SQLANCER_PP_JOB),
            f"OVERALL_STATUS={overall_status}",
            f"TEST_RESULTS=({rows_literal})",
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
        ["bash", str(script)], capture_output=True, text=True, timeout=120, cwd=cwd
    )
    return proc.returncode, _read_emitted_result(tmp_path, proc, job_name)


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
def test_emitted_name_comes_from_the_environment_lookup(tmp_path, runner, node):
    """The emitted `name` tracks `JOB_NAME`, and is never empty.

    An empty `name` is the failure mode of a broken lookup: it produces a
    plausible-looking result file whose merge silently no-ops, exactly like a
    wrong name. Driving a second, distinct `JOB_NAME` through the real init block
    is what distinguishes a lookup from a hardcoded literal that happens to match
    the node.
    """
    other = node.replace("arm_asan_ubsan", "amd_asan_ubsan")
    assert other != node

    _, result = runner(tmp_path, "FAIL", job_name=other)
    assert result["name"], "an empty result name no-ops the report merge"
    assert result["name"] == other, "the name must come from JOB_NAME, not a literal"


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


# A JSON metacharacter in a failure message must not corrupt the result file.
# `Result.from_fs` re-raises `JSONDecodeError` (`Utils.MetaClasses.from_file`) and
# the `Result.from_fs(job.name)` call in `runner.py` is outside its try/except, so
# an unescaped `info` loses the whole result, not just that one message. All four
# payloads occur verbatim in Java assertion text.
_UNESCAPED_INFO_PAYLOADS = [
    pytest.param(r"exit=1; AssertionError: pattern \d+ did not match", id="regex-escape"),
    pytest.param(r"exit=1; AssertionError: SELECT \N FROM t", id="null-literal"),
    pytest.param('exit=1; AssertionError: expected "x"', id="double-quote"),
    pytest.param(r"exit=1; AssertionError: path C:\ ", id="trailing-backslash"),
]


@pytest.mark.parametrize("info", _UNESCAPED_INFO_PAYLOADS)
def test_pp_info_is_json_escaped(tmp_path, info):
    """The pp `info` goes through `json_escape`, so the result file stays parseable.

    Driven through the script's own serialization block, and read back by the real
    `Result.from_fs` rather than by `json.load`, so the assertion is that praktika
    can consume what the script wrote.
    """
    _, emitted = _run_sqlancer_pp_tail(
        tmp_path, "FAIL", rows=[f"NoREC,FAIL,{info}"]
    )
    assert emitted["results"][0]["info"] == info
    assert emitted["status"] == Result.Status.FAIL


def _run_sqlancer_pp_oracle_loop(tmp_path, java_exit=0, java_stdout="", server_up=True):
    """Run `sqlancer_pp_job.sh`'s real oracle loop and report what it assigned.

    This is the code that decides `OVERALL_STATUS`; `_run_sqlancer_pp_tail`
    supplies that value itself, so without this the loop's own tokens are
    uncovered. Only `wget` and `java` are stubbed: no server, no jar, no docker.

    Returns (overall_status, [test_result_rows]).
    """
    text = _read(_SQLANCER_PP_JOB)
    loop = _extract_compound(
        text, 'for ORACLE in "${ORACLES[@]}"; do', "done", _SQLANCER_PP_JOB
    )

    out_dir = tmp_path / "out"
    out_dir.mkdir(parents=True, exist_ok=True)
    wget_body = "printf 'Ok.'" if server_up else "printf 'nope'"
    java_body = f"printf '%s\\n' {java_stdout!r}; return {java_exit}"

    harness = "\n".join(
        [
            "set -u",
            f'OUTPUT_PATH="{out_dir}"',
            f'JAR="{tmp_path}/nonexistent.jar"',
            "NUM_THREADS=1",
            "NUM_QUERIES=1",
            "TIMEOUT=1",
            'SQLANCER_USER="sqlancer"',
            'SQLANCER_PASSWORD="sqlancer"',
            'ORACLES=( "WHERE" "NoREC" )',
            "TEST_RESULTS=()",
            "ATTACHED_FILES_ARRAY=()",
            # The loop's precondition, straight from the script.
            "OVERALL_STATUS=OK",
            f"wget() {{ {wget_body}; }}",
            f"java() {{ {java_body}; }}",
            loop,
            'printf "OVERALL=%s\\n" "$OVERALL_STATUS"',
            'printf "ROW=%s\\n" "${TEST_RESULTS[@]}"',
        ]
    )
    script = tmp_path / "harness_pp_loop.sh"
    script.write_text(harness, encoding="utf-8")
    proc = subprocess.run(
        ["bash", str(script)], capture_output=True, text=True, timeout=120
    )
    assert proc.returncode == 0, f"the loop harness itself failed:\n{proc.stderr}"

    overall = re.findall(r"^OVERALL=(.*)$", proc.stdout, re.M)
    rows = re.findall(r"^ROW=(.*)$", proc.stdout, re.M)
    assert len(overall) == 1, f"expected one OVERALL line, got {overall}"
    assert rows, "the loop produced no TEST_RESULTS rows"
    return overall[0], rows


@pytest.mark.parametrize(
    "kwargs, expect_ok, expect_leaf",
    [
        ({}, True, Result.Status.OK),
        ({"java_exit": 3}, False, Result.Status.FAIL),
        # `java` succeeds but leaves an assertion behind: still a failure.
        (
            {"java_stdout": "java.lang.AssertionError: boom"},
            False,
            Result.Status.FAIL,
        ),
        ({"server_up": False}, False, Result.Status.ERROR),
    ],
    ids=["clean", "java-fails", "assertion-only", "server-down"],
)
def test_pp_oracle_loop_assigns_statuses_that_reach_the_report(
    tmp_path, kwargs, expect_ok, expect_leaf
):
    """The loop's own `OVERALL_STATUS` and leaf tokens, fed into the real merge.

    Covering the clean branch as well is what stops this from passing by always
    expecting a failure.
    """
    overall, rows = _run_sqlancer_pp_oracle_loop(tmp_path, **kwargs)

    leaves = {row.split(",")[1] for row in rows}
    assert leaves == {expect_leaf}, f"unexpected leaf statuses {leaves}"

    workflow_status, node_status, _ = _merge(
        _SQLANCER_PP_NODE, _SQLANCER_PP_NODE, overall
    )
    if expect_ok:
        assert overall == Result.Status.OK
        assert workflow_status == Result.Status.OK
        assert node_status == Result.Status.OK
    else:
        assert workflow_status == Result.Status.FAIL, (
            f"the oracle loop assigned {overall!r}, which does not fail the "
            "workflow rollup"
        )
        assert node_status == Result.Status.FAIL


def test_pp_assertion_message_survives_loop_and_serialization(tmp_path):
    """Loop and tail together round-trip a failure message containing a quote.

    The loop builds the `info`, the tail serializes it. Testing them separately
    cannot see a double-escape: escaping the quote in *both* places yields valid
    JSON that decodes to a corrupted message, so the escaping has to live in
    exactly one of the two and only the pair pins that.
    """
    message = 'java.lang.AssertionError: expected "x" got y'
    _, rows = _run_sqlancer_pp_oracle_loop(tmp_path / "loop", java_stdout=message)
    assert len(rows) >= 1

    _, emitted = _run_sqlancer_pp_tail(tmp_path / "tail", "FAIL", rows=rows)

    infos = [r["info"] for r in emitted["results"]]
    assert infos, "no result rows were serialized"
    for info in infos:
        assert message in info, f"the assertion message was mangled: {info!r}"


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
