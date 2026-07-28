"""
Tests for Runner._read_job_result_or_running.

A `Stateless tests (amd_llvm_coverage, ...)` job whose 11882 tests all passed was
reported as a red job with a completely empty `info`. The host docker daemon died during
post-run processing and left the job result JSON empty; `Result.from_fs` then raised
`JSONDecodeError`, and because the second of the two job-result reads in `Runner.run`
was unguarded the exception escaped to the interpreter and killed the runner *before*
the reporting path that attaches the log tail, uploads `job.log`, and sets the commit
status. Nothing ever wrote a reason, which is why `info` was empty.

Two properties are pinned here:

* the read degrades instead of raising, and

* it degrades to **RUNNING**, not ERROR. `is_completed()` is
  "status not in (PENDING, RUNNING)", so an ERROR result is already completed and would
  skip `_run`'s `if not result.is_completed():` branch - the only place that records why
  the job died and attaches `process.get_latest_log()`. Synthesizing ERROR therefore
  removes the crash while preserving the blank-red-box symptom.

A genuine result is returned untouched: this must not be able to hide a real failure.
"""

import ast
import dataclasses
import os
import sys
import textwrap

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import pytest

from ci.praktika._environment import _Environment
from ci.praktika.job import Job
from ci.praktika.result import Result
from ci.praktika.runner import Runner
from ci.praktika.settings import Settings

JOB_NAME = "Stateless tests (amd_llvm_coverage, AsyncInsert, s3 storage, parallel)"


class _Job:
    def __init__(self, name=JOB_NAME):
        self.name = name


@pytest.fixture
def in_tmp_cwd(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    # Runner._run sets PRAKTIKA and rewrites PYTHONPATH process-globally, and pytest does
    # not restore process env between tests; monkeypatch does, so later tests in this
    # worker do not inherit them.
    for var in ("PRAKTIKA", "PYTHONPATH"):
        monkeypatch.setenv(var, os.environ.get(var, ""))
    os.makedirs(Settings.TEMP_DIR, exist_ok=True)
    return tmp_path


def _write_result_file(content):
    with open(Result.file_name_static(JOB_NAME), "w", encoding="utf8") as f:
        f.write(content)


# --- the degradation, over the whole unreadable class ---------------------------------


@pytest.mark.parametrize(
    "content, label",
    [
        ("", "empty file left by a killed writer"),
        ('{"na', "truncated mid-key"),
        ('{"name": "x", "status": "OK"', "truncated before the closing brace"),
        ("not json at all", "garbage"),
    ],
)
def test_unreadable_result_does_not_raise_and_is_running(in_tmp_cwd, content, label):
    _write_result_file(content)
    # Sanity: the file *is* unreadable, so this test is not vacuous.
    assert Result.exist(JOB_NAME) is True, label
    with pytest.raises(Exception):
        Result.from_fs(JOB_NAME)

    result = Runner._read_job_result_or_running(_Job())

    assert result is not None, "must return a usable Result, not None"
    assert result.name == JOB_NAME
    # ⭐ The load-bearing assertion. A synthesized ERROR would pass "does not raise" while
    # silently skipping the branch that attaches the log tail.
    assert result.is_completed() is False
    assert result.is_running() is True
    assert result.status == Result.Status.RUNNING
    assert result.is_ok() is False
    assert result.info, "the reason must be reported, not swallowed"


def test_missing_result_file_also_degrades(in_tmp_cwd):
    """An OSError (no file, or root-owned after a failed chown) must degrade too."""
    assert not os.path.exists(Result.file_name_static(JOB_NAME))
    result = Runner._read_job_result_or_running(_Job())
    assert result.status == Result.Status.RUNNING
    assert result.is_running() is True


def test_returned_object_is_a_usable_result(in_tmp_cwd):
    """`run()` calls `.is_ok()` on the returned value on the very next line; returning
    None would merely move the crash."""
    _write_result_file("")
    result = Runner._read_job_result_or_running(_Job())
    assert isinstance(result, Result)
    assert result.is_ok() is False
    assert result.is_error() is False
    assert callable(result.dump)


# --- the genuine-failure control: nothing may be masked -------------------------------


def test_a_real_test_failure_is_returned_unchanged(in_tmp_cwd):
    """The one way this fix could do harm is by relabeling a real failure."""
    original = Result(
        name=JOB_NAME,
        status=Result.Status.FAIL,
        start_time=1.0,
        duration=2.0,
        info="Failures: 3/11882",
        results=[
            Result(
                name="00001_select_1", status=Result.Status.FAIL, start_time=1.0,
                duration=0.5,
            )
        ],
    )
    original.dump()

    result = Runner._read_job_result_or_running(_Job())

    assert result.status == Result.Status.FAIL
    assert result.info == "Failures: 3/11882"
    assert [r.name for r in result.results] == ["00001_select_1"]
    assert result.is_completed() is True


def test_a_passing_result_is_returned_unchanged(in_tmp_cwd):
    Result(
        name=JOB_NAME, status=Result.Status.OK, start_time=1.0, duration=2.0
    ).dump()
    result = Runner._read_job_result_or_running(_Job())
    assert result.status == Result.Status.OK
    assert result.is_ok() is True


# --- structural pins on where the guarded read is used -------------------------------


def _runner_ast():
    path = os.path.join(os.path.dirname(__file__), "../praktika/runner.py")
    return ast.parse(open(path, encoding="utf8").read()), path


def _function(tree, name):
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    raise AssertionError(f"function {name} not found")


def test_no_unguarded_job_result_read_remains_in_run_or__run():
    """Both job-result reads in the crash path must go through the guarded helper.

    `_get_result_object` keeps its own explicit try/except, and the *workflow* result
    reads in `_post_run` are a different file written by a different process - out of
    scope here.
    """
    tree, _ = _runner_ast()
    for fn_name in ("run", "_run"):
        fn = _function(tree, fn_name)
        bare = [
            node.lineno
            for node in ast.walk(fn)
            if isinstance(node, ast.Call)
            and ast.unparse(node) == "Result.from_fs(job.name)"
        ]
        assert not bare, (
            f"{fn_name} still reads the job result directly at lines {bare}; "
            "use _read_job_result_or_running so an unreadable file cannot kill the runner"
        )


def test_status_reconciliation_stays_on_the_unconditional_run_path():
    """`if not res and result.is_ok(): set_status(ERROR)` is the only conversion of a
    completed OK result into ERROR when the run failed.

    Deleting it reports a green job while the runner exits 1. Moving it into
    `_get_result_object` loses it for local runs, since that method is called under
    `if run_hooks:`.
    """
    tree, _ = _runner_ast()
    run_fn = _function(tree, "run")

    reconciliations = [
        node
        for node in ast.walk(run_fn)
        if isinstance(node, ast.If)
        and "result.is_ok()" in ast.unparse(node.test)
        and "Status.ERROR" in ast.unparse(node)
    ]
    assert len(reconciliations) == 1, (
        "expected exactly one completed-OK -> ERROR reconciliation in Runner.run"
    )
    node = reconciliations[0]

    # It must not sit under an `if run_hooks:` (or any run_hooks-gated) block.
    for parent in ast.walk(run_fn):
        if isinstance(parent, ast.If) and "run_hooks" in ast.unparse(parent.test):
            body_lines = {
                n.lineno
                for stmt in parent.body
                for n in ast.walk(stmt)
                if hasattr(n, "lineno")
            }
            assert node.lineno not in body_lines, (
                "the reconciliation moved under `if run_hooks:` - local runs would lose it"
            )

    # And it must run before the on_error_hook is consulted in _get_result_object.
    calls = [
        n.lineno
        for n in ast.walk(run_fn)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Attribute)
        and n.func.attr == "_get_result_object"
    ]
    assert calls, "_get_result_object call not found in Runner.run"
    assert node.lineno < min(calls), (
        "the reconciliation must precede _get_result_object, which runs the on_error_hook"
    )


def test_helper_is_defined_and_documented():
    tree, _ = _runner_ast()
    fn = _function(tree, "_read_job_result_or_running")
    doc = ast.get_docstring(fn) or ""
    assert "RUNNING" in doc, "the RUNNING-vs-ERROR choice is load-bearing; document it"
    src = textwrap.dedent(ast.unparse(fn))
    assert "Status.RUNNING" in src
    assert "Status.ERROR" not in src, (
        "an ERROR fallback is is_completed()==True and skips the log-tail branch"
    )


# --- the behavioral pins: the whole recovery branch of _run, end to end ---------------
#
# The tests above pin the helper and the source shape; neither sees the *branch*. Moving
# the recovery out of the `with TeePopen(...)` scope loses process.get_latest_log() and
# reinstates the blank-red-box symptom while keeping all of them green.


def _run_job_with_unreadable_result(command):
    """Drive Runner._run over a job whose result file is the 0-byte incident state.

    No docker and no GH auth: run_in_docker="" and enable_gh_auth=False (the defaults)
    skip those branches, so a dumped _Environment plus Settings.TEMP_DIR is all _run
    needs. `workflow` is only dereferenced by the skipped branches.
    """
    required = [
        f.name
        for f in dataclasses.fields(_Environment)
        if f.default is dataclasses.MISSING
        and getattr(f, "default_factory", dataclasses.MISSING) is dataclasses.MISSING
    ]
    _Environment(**{n: (0 if n == "PR_NUMBER" else "") for n in required}).dump()

    job = Job.Config(name=JOB_NAME, runs_on=["x"], command=command, run_in_docker="")
    _write_result_file("")

    exit_code = Runner()._run(workflow=None, job=job)
    return exit_code, Result.from_fs(JOB_NAME)


def test_run_persists_error_with_reason_and_log_tail_when_result_unreadable(in_tmp_cwd):
    exit_code, persisted = _run_job_with_unreadable_result(
        "printf 'daemon-death-tail\\n'; exit 125"
    )

    assert exit_code == 125
    assert persisted.status == Result.Status.ERROR
    assert persisted.is_ok() is False
    assert "Failed to read Result json" in persisted.info
    # ⭐ The assertion this whole test exists for: the log tail is the information the
    # incident lacked, and it is available only inside the TeePopen scope.
    assert "daemon-death-tail" in persisted.info
    assert any(
        "Job killed, exit code [125]" in e["message"]
        for e in persisted.ext.get("errors", [])
    )


def test_run_leaves_result_running_when_job_succeeded_but_result_unreadable(in_tmp_cwd):
    """A process that exited 0 must not be given a fabricated completed status here;
    promoting it is _get_result_object's job (ResultInfo.KILLED)."""
    exit_code, persisted = _run_job_with_unreadable_result("true")

    assert exit_code == 0
    assert persisted.status == Result.Status.RUNNING
    assert persisted.is_completed() is False
    assert persisted.is_ok() is False
