"""
Tests for the interim result checkpoint in `ci/jobs/functional_tests.py`.

`main` holds every per-test result in memory from the moment the suite finishes until
`Result.complete_job` at the very end, and everything in between is teardown. A job killed
in that window published one CIDB row where its passing sibling shard published 6178.
`checkpoint_collected_results` writes the collected results into the job's existing result
file before that teardown starts, so the kill costs the job's status but not its results.

Five properties are load-bearing and none is visible from the call site:

* it must never RAISE. It is called unguarded and `main` has no enclosing try, so a raise
  here - and it is the first write after `_pre_run` - publishes the bare `RUNNING` the
  runner then turns into an EMPTY error, on a run that would have completed normally. The
  final `complete_job` writes these results anyway, so failing is always cheaper than
  propagating.

* it must actually RUN, and persist the results it was handed. Every structural arm here
  can only show that the call exists, which a guard that is never true satisfies while
  restoring the empty report.
* it must assign NO status. Every status decision in `main` (the `Check errors` fatal-log
  rows, the bugfix-validation inversion, `force_ok_exit`) runs after this point, so a
  status written here would be published as the verdict on a killed job - a
  bugfix-validation job would report its non-inverted one. Left `RUNNING`, the runner's own
  `KILLED` patch decides the status, and because `add_error` and `set_status` touch only
  `ext["errors"]` and `status`, the children survive that patch.
* it must UPDATE the existing result rather than build a fresh one. `Result.create_from`
  takes no `ext`, so a replacement would drop the `run_url` that `Runner._pre_run` wrote.
* it must publish by RENAME. `Result.dump` is `open(..., "w")` + `json.dump`, which
  truncates before serializing, so a kill mid-write would leave JSON that
  `Result.from_fs` refuses to parse - turning a lost result into an unreadable one.

Kill-based arms drive a real subprocess: the write is only atomic with respect to a
process death, which no in-process stub can produce.
"""

import ast
import json
import os
import signal
import subprocess
import sys
import time

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs.functional_tests import checkpoint_collected_results
from ci.praktika.cidb import CIDB
from ci.praktika.result import Result, ResultInfo
from ci.praktika.settings import Settings
from ci.praktika.utils import Utils

_JOB_SCRIPT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../jobs/functional_tests.py")
)
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))

_CHECKPOINT_HELPER = "checkpoint_collected_results"
_JOB_NAME = "Stateless tests (checkpoint probe)"
_RUN_URL = "https://example.invalid/run/1/job/2"


# --- structural: the checkpoint is placed and shaped correctly ------------------------


def _parse(path):
    with open(path, encoding="utf-8") as f:
        return ast.parse(f.read(), filename=path)


def _find_function(tree, name):
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    raise AssertionError(f"{name}() not found in {_JOB_SCRIPT}")


def _checkpoint_calls(scope):
    return [
        node
        for node in ast.walk(scope)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == _CHECKPOINT_HELPER
    ]


def _call_lines(scope, predicate):
    return sorted(
        node.lineno
        for node in ast.walk(scope)
        if isinstance(node, ast.Call) and predicate(node)
    )


def _is_named_call(node, attr):
    return isinstance(node.func, ast.Attribute) and node.func.attr == attr


def test_checkpoint_precedes_every_teardown_step_in_main():
    """The first checkpoint must come before the teardown that can outlive the job.

    `stop_log_exports` is the first teardown step and is where the reported run was
    killed; the final `complete_job` is the only dump today. A checkpoint after either
    measures nothing.

    Compared against the LAST `complete_job`: the earlier ones are pre-suite early exits
    ("No tests to run") that terminate the process before there is anything to
    checkpoint, so comparing against the first would assert an impossible ordering.
    """
    main = _find_function(_parse(_JOB_SCRIPT), "main")

    checkpoints = _call_lines(main, lambda n: isinstance(n.func, ast.Name) and n.func.id == _CHECKPOINT_HELPER)
    teardown = _call_lines(main, lambda n: _is_named_call(n, "stop_log_exports"))
    dumps = _call_lines(main, lambda n: _is_named_call(n, "complete_job"))

    assert checkpoints, f"main() never calls {_CHECKPOINT_HELPER}: the results are not checkpointed"
    assert teardown, "main() no longer calls stop_log_exports: this test measures nothing"
    assert dumps, "main() no longer calls complete_job: this test measures nothing"
    assert checkpoints[0] < teardown[0], (
        f"the first checkpoint is at line {checkpoints[0]} but teardown starts at "
        f"{teardown[0]}: a kill in that window would again discard the results"
    )
    assert checkpoints[-1] < dumps[-1], (
        f"the last checkpoint at line {checkpoints[-1]} is not before the final dump at "
        f"{dumps[-1]}"
    )


def test_the_bugfix_validation_loop_checkpoints_per_build_type():
    """A checkpoint must sit INSIDE the build-type loop, not only after it.

    The loop does `test_result.results = bt_result.results` - it REPLACES - and then stops
    the server and re-prepares the environment before producing any new rows, so a single
    post-loop checkpoint leaves each earlier build type's results exposed for the whole of
    the next iteration and unrecoverable from memory.
    """
    main = _find_function(_parse(_JOB_SCRIPT), "main")

    loops = [
        node
        for node in ast.walk(main)
        if isinstance(node, ast.For)
        and any(
            isinstance(sub, ast.Call) and _is_named_call(sub, "check_fatal_messages_in_logs")
            for sub in ast.walk(node)
        )
        and any(
            isinstance(sub, ast.Attribute) and sub.attr == "debug_files" for sub in ast.walk(node)
        )
    ]
    assert loops, "the bugfix-validation build-type loop was not found in main()"
    for loop in loops:
        assert _checkpoint_calls(loop), (
            f"the build-type loop at line {loop.lineno} contains no {_CHECKPOINT_HELPER} "
            "call: each earlier build type's results are replaced before being persisted"
        )


def test_every_checkpoint_call_passes_the_collected_results():
    """Each call must hand over the results, not a fresh or empty expression.

    `checkpoint_collected_results(name, [], flag)` satisfies every other arm here while
    writing the empty report the change exists to prevent.
    """
    main = _find_function(_parse(_JOB_SCRIPT), "main")
    calls = _checkpoint_calls(main)
    assert calls, f"main() never calls {_CHECKPOINT_HELPER}"

    for node in calls:
        passed = node.args + [kw.value for kw in node.keywords]
        names = set()
        for arg in passed:
            for sub in ast.walk(arg):
                if isinstance(sub, ast.Name):
                    names.add(sub.id)
                elif isinstance(sub, ast.Attribute):
                    names.add(sub.attr)
        assert "test_result" in names, (
            f"the checkpoint at line {node.lineno} does not pass `test_result` "
            f"(passes {sorted(names)}); anything else can persist an empty report"
        )
        assert "is_local_run" in names, (
            f"the checkpoint at line {node.lineno} hard-codes its local-run flag "
            f"(passes {sorted(names)}); the helper's guard would decide on a constant"
        )


def test_checkpoint_uses_the_existing_result_not_a_fresh_one():
    """The helper must build its result with `from_fs`, never `Result.create_from`.

    Asserted as an exact set rather than "no create_from": a rewrite to
    `create_from(...).dump()` has no `from_fs` at all, so a receiver-only check would
    pass over nothing.
    """
    checkpoint = _find_function(_parse(_JOB_SCRIPT), _CHECKPOINT_HELPER)
    constructors = sorted(
        node.func.attr
        for node in ast.walk(checkpoint)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr in ("from_fs", "create_from")
    )
    assert constructors == ["from_fs"], (
        f"{_CHECKPOINT_HELPER}() builds its result with {constructors} instead of exactly "
        "['from_fs']; anything but from_fs discards ext, which holds the run url"
    )


def test_checkpoint_publishes_by_rename():
    """The helper must write a temporary file and `os.replace` it into place.

    `Result.dump` truncates before serializing, so publishing directly turns a kill
    mid-write into unreadable JSON. The runtime arm below measures the outcome; this pins
    the mechanism, because a `dump()` that happens not to be interrupted passes that arm.
    """
    checkpoint = _find_function(_parse(_JOB_SCRIPT), _CHECKPOINT_HELPER)
    replaces = [
        node
        for node in ast.walk(checkpoint)
        if isinstance(node, ast.Call) and _is_named_call(node, "replace")
    ]
    assert replaces, (
        f"{_CHECKPOINT_HELPER}() never calls os.replace: it publishes its write directly, "
        "so a kill mid-write leaves JSON that Result.from_fs refuses to parse"
    )
    # `Result.dump`, not `json.dump`: the helper serializes with the latter into its own
    # temporary file, which is the whole point. Matched on the receiver so the arm keeps
    # working if the serialization is spelled differently.
    result_dumps = [
        node
        for node in ast.walk(checkpoint)
        if isinstance(node, ast.Call)
        and _is_named_call(node, "dump")
        and not (isinstance(node.func.value, ast.Name) and node.func.value.id == "json")
    ]
    assert result_dumps == [], (
        f"{_CHECKPOINT_HELPER}() calls Result.dump() at lines "
        f"{[n.lineno for n in result_dumps]}: dump truncates the published file in place, "
        "which is what the rename avoids"
    )


def test_checkpoint_assigns_no_status():
    """The helper must not assign a status anywhere.

    Structural as well as behavioural: the runtime arm sees only the status the helper
    leaves, not a `set_status` a later refactor puts on a branch it does not exercise.
    """
    checkpoint = _find_function(_parse(_JOB_SCRIPT), _CHECKPOINT_HELPER)
    assignments = sorted(
        node.func.attr
        for node in ast.walk(checkpoint)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr in ("set_status", "set_error", "set_failed", "set_success", "complete_job")
    )
    assert assignments == [], (
        f"{_CHECKPOINT_HELPER}() assigns a status ({assignments}); it would pre-empt the "
        "fatal-log rows, the bugfix-validation inversion and force_ok_exit, all of which "
        "run after this point"
    )
    for node in ast.walk(checkpoint):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                assert not (
                    isinstance(target, ast.Attribute) and target.attr == "status"
                ), f"{_CHECKPOINT_HELPER}() assigns .status directly at line {node.lineno}"


def test_checkpoint_guards_every_step_against_every_exception():
    """The helper must wrap all of its work in `except Exception`.

    Structural as well as behavioural: the behavioural arms below are satisfied by an
    `except OSError`, which leaves `Result.from_fs`'s `JSONDecodeError` propagating. This
    pins the breadth and the extent - a `try` that starts after the load guards nothing
    against the raiser the load itself is.
    """
    checkpoint = _find_function(_parse(_JOB_SCRIPT), _CHECKPOINT_HELPER)
    tries = [node for node in ast.walk(checkpoint) if isinstance(node, ast.Try)]
    assert tries, (
        f"{_CHECKPOINT_HELPER}() has no try/except: it is called unguarded from main(), "
        "which has no enclosing try, so any raise here loses the results, the logs and "
        "the status on a run that would have completed normally"
    )

    def _catches_everything(node):
        return any(
            isinstance(h.type, ast.Name) and h.type.id in ("Exception", "BaseException")
            for h in node.handlers
        ) or any(h.type is None for h in node.handlers)

    broad = [node for node in tries if _catches_everything(node)]
    assert broad, (
        f"{_CHECKPOINT_HELPER}() catches only "
        f"{[ast.dump(h.type) for n in tries for h in n.handlers]}: JSONDecodeError from "
        "Result.from_fs is not an OSError, so a corrupt result file still aborts main()"
    )

    # Every raiser must be INSIDE one of the broad handlers' bodies, not merely somewhere
    # in the function: a try that begins after `from_fs` leaves the load exposed.
    guarded = {
        node
        for tried in broad
        for stmt in tried.body
        for node in ast.walk(stmt)
        if isinstance(node, ast.Call)
    }
    for attr in ("from_fs", "replace", "dump"):
        calls = [
            node
            for node in ast.walk(checkpoint)
            if isinstance(node, ast.Call) and _is_named_call(node, attr)
        ]
        unguarded = [node.lineno for node in calls if node not in guarded]
        assert calls and not unguarded, (
            f"{_CHECKPOINT_HELPER}() calls .{attr}() outside the guarded block at lines "
            f"{unguarded}: that raiser still aborts main()"
        )


# --- behavioural: driving the real helper against a real result file -----------------


def _children(count, name="Tests"):
    return [
        Result.create_from(
            name=name,
            status=Result.Status.OK,
            results=[
                Result.create_from(name=f"0000{i}_test", status=Result.Status.OK)
                for i in range(count)
            ],
        )
    ]


def _seed_running_result(tmp_path, name=_JOB_NAME):
    """Write exactly what `Runner._pre_run` leaves on disk before the job script runs.

    A bare `Result` with a start_time and the run url in `ext`, not `create_from`:
    `create_from` reads start_time from a file that does not exist yet, so a fixture built
    with it would leave duration None and make the runner-patch arm assert nothing.
    """
    Settings.TEMP_DIR = str(tmp_path)
    result = Result(name=name, status=Result.Status.RUNNING, start_time=Utils.timestamp())
    result.add_ext_key_value("run_url", _RUN_URL)
    result.dump()
    return result


def _checkpointed(tmp_path, collected, is_local_run=False):
    original = Settings.TEMP_DIR
    try:
        _seed_running_result(tmp_path)
        checkpoint_collected_results(_JOB_NAME, collected, is_local_run)
        return Result.from_fs(_JOB_NAME)
    finally:
        Settings.TEMP_DIR = original


def test_checkpoint_persists_the_results(tmp_path):
    """The arm that catches a checkpoint which never executes.

    Every structural arm above is satisfied by a guard that is never true or a body
    disabled outright, both of which restore the empty report.
    """
    reread = _checkpointed(tmp_path, _children(3))

    assert len(reread.results) == 1, f"expected one 'Tests' child, got {len(reread.results)}"
    assert len(reread.results[0].results) == 3, (
        f"the checkpoint persisted {len(reread.results[0].results)} per-test rows instead "
        "of 3: the write did not reach disk"
    )


def test_checkpoint_is_skipped_on_a_local_run(tmp_path):
    """A local run has no runner to publish anything, so nothing is written.

    Pins the guard's polarity: inverted, this arm sees the children appear and the arm
    above sees them vanish, so the two together fix its direction.
    """
    reread = _checkpointed(tmp_path, _children(3), is_local_run=True)

    assert reread.results == [], (
        f"a local run wrote {len(reread.results)} children; the guard is inverted"
    )


def test_checkpoint_leaves_the_result_incomplete(tmp_path):
    """The checkpoint must stay non-terminal so the harness still decides the status."""
    reread = _checkpointed(tmp_path, _children(2))

    assert not reread.is_completed(), (
        f"the checkpoint published a completed status [{reread.status}]: on a killed job "
        "that half-decided status would be the verdict"
    )


def test_checkpoint_preserves_ext(tmp_path):
    """`ext` must survive: it carries the run url `Runner._pre_run` wrote."""
    reread = _checkpointed(tmp_path, _children(2))

    assert reread.ext.get("run_url") == _RUN_URL, (
        f"the checkpoint lost ext['run_url'] (got {reread.ext.get('run_url')!r}); "
        "Result.create_from takes no ext, so it must not have been used"
    )


def test_a_failing_write_degrades_to_a_no_op(tmp_path):
    """A write that raises must cost nothing: the helper returns, the file is untouched.

    The load-bearing arm for the guard. `main()` has no enclosing try and the checkpoint
    is the FIRST write after `_pre_run`, so a propagated exception publishes the bare
    `RUNNING` with zero children - and the runner turns that into an empty ERROR on a run
    that would have completed normally, losing the logs and the status too.

    ENOSPC on the temp write is the realistic raiser: the published file is 2.4 MB for
    6174 rows with empty info and 4.3 MB with 300-char info.
    """
    import builtins

    real_open = builtins.open

    def failing_open(*args, **kwargs):
        if args and str(args[0]).endswith(".tmp"):
            raise OSError(28, "No space left on device")
        return real_open(*args, **kwargs)

    original = Settings.TEMP_DIR
    try:
        _seed_running_result(tmp_path)
        builtins.open = failing_open
        try:
            checkpoint_collected_results(_JOB_NAME, _children(3), False)
        finally:
            builtins.open = real_open
        reread = Result.from_fs(_JOB_NAME)
    finally:
        builtins.open = real_open
        Settings.TEMP_DIR = original

    assert reread.status == Result.Status.RUNNING, (
        f"the published result is [{reread.status}] instead of the untouched RUNNING: a "
        "failed checkpoint must not alter what _pre_run left"
    )
    assert reread.results == [], (
        f"the failed write still left {len(reread.results)} children behind"
    )
    assert reread.ext.get("run_url") == _RUN_URL, "the failed write damaged ext"


def test_an_unparseable_result_file_degrades_to_a_no_op(tmp_path):
    """The second raiser: `Result.from_fs` -> `json.load` on a corrupt file.

    `JSONDecodeError` is a `ValueError`, not an `OSError`, so this arm is what makes
    `except Exception` necessary rather than merely sufficient.
    """
    original = Settings.TEMP_DIR
    try:
        _seed_running_result(tmp_path)
        path = Result.file_name_static(_JOB_NAME)
        with open(path, "w", encoding="utf-8") as f:
            f.write('{"name": "truncated", "resul')
        checkpoint_collected_results(_JOB_NAME, _children(3), False)
        with open(path, encoding="utf-8") as f:
            after = f.read()
    finally:
        Settings.TEMP_DIR = original

    assert after == '{"name": "truncated", "resul', (
        f"the helper rewrote the unparseable file as {after[:120]!r}: it must leave what it "
        "could not read alone"
    )


def test_runner_kill_patch_keeps_the_checkpointed_results(tmp_path):
    """The measured statement of the fix: the empty ERROR becomes a populated ERROR.

    Applies `Runner._get_result_object`'s patch sequence for an incomplete result. Pins
    the praktika behaviour the fix rests on, so a future change there cannot silently
    un-fix this - and asserts the honesty half too: the status is NOT kept green.
    """
    reread = _checkpointed(tmp_path, _children(5))
    assert not reread.is_completed(), "precondition: the checkpoint must be incomplete"

    Settings.TEMP_DIR = str(tmp_path)
    try:
        reread.add_error(ResultInfo.TIMEOUT).set_status(Result.Status.ERROR)
        reread.update_duration()
    finally:
        Settings.TEMP_DIR = "./ci/tmp"

    assert reread.status == Result.Status.ERROR, (
        f"the killed job reports [{reread.status}] instead of ERROR: the checkpoint must "
        "not let a killed job look green"
    )
    assert len(reread.results[0].results) == 5, (
        f"the runner's patch dropped rows ({len(reread.results[0].results)} of 5 left): "
        "the report would still be empty"
    )
    assert reread.duration is not None, "the runner did not fill in the duration"


def test_cidb_ingests_the_children_of_a_killed_checkpointed_result(tmp_path):
    """The quantitative claim: a killed job publishes per-test rows, not just one.

    `CIDB.json_data_generator` finds the per-test rows through the `result_name_for_cidb`
    sub-result ("Tests" for every functional-test job), so the checkpoint must keep that
    shape - a flat list of test rows would yield the same single row as no checkpoint.
    """
    reread = _checkpointed(tmp_path, _children(6))
    reread.add_error(ResultInfo.TIMEOUT)
    reread.status = Result.Status.ERROR

    rows = [json.loads(row) for row in CIDB.json_data_generator(reread, "Tests")]
    test_rows = [row for row in rows if row["test_name"]]

    assert len(test_rows) == 6, (
        f"a killed job with a checkpoint yields {len(test_rows)} per-test rows instead of "
        "6: the run stays invisible to every downstream flaky-detection query"
    )
    assert all(row["check_status"] == "error" for row in rows), (
        "the killed job's rows do not carry the error check_status"
    )


# --- end-to-end: main()'s real post-suite path, killed the way praktika kills ---------

# Drives the real `main()` with the suite, the server and the shells stubbed out, then
# SIGKILLs the process group inside a teardown step. The measurement is what
# `Result.from_fs(job_name)` yields afterwards - exactly what `Runner._get_result_object`
# reads. In-process arms cannot produce this: the loss and the atomicity both depend on a
# process death mid-flight.
_MAIN_PROBE = r"""
import json, os, sys, time
from pathlib import Path
sys.path.insert(0, {repo!r})
from ci.praktika.settings import Settings
Settings.TEMP_DIR = {tmp!r}
# Built from the DEFAULT TEMP_DIR at class-definition time, so the override above does
# not move it. Left as it is, `_Environment.get` reads the workflow context of the job
# running this test rather than the one seeded below.
Settings.WORKFLOW_STATUS_FILE = {tmp!r} + "/workflow_status.json"
import ci.praktika._environment as _env
import ci.jobs.functional_tests as ft
from ci.praktika.result import Result
from ci.praktika.utils import Utils

JOB_NAME = {job!r}
BUILD_TYPES = {build_types!r}
ROWS = {rows!r}
BLOCK_ON_STOP_SERVER_CALL = {block_call}
BLOCKED_MARKER = {blocked!r}
# "": all build types succeed. "first_fails": the first build type produces FAIL rows, so
# `test_result.is_ok()` is false and the `build_types[1:]` loop never runs.
# "startup_fails": `CH.start` returns False for the second build type, taking the
# `startup_error` break.
FAILURE_MODE = {failure_mode!r}
FATAL_ROW_NAME = {fatal_row!r}

# What `Runner._pre_run` leaves on disk before the job script starts.
Result(name=JOB_NAME, status=Result.Status.RUNNING,
       start_time=Utils.timestamp()).add_ext_key_value("run_url", "URL").dump()
_e = _env._Environment.get()
_e.JOB_NAME = JOB_NAME
_e.LOCAL_RUN = False
_e.PR_LABELS = ["pr-bugfix"]
_e.dump()

_calls = {{"n": 0}}


class _Proc:
    # Stands in for FTResultsProcessor: rows complete in memory, no server, no suite.
    def __init__(self, *a, **k):
        self.debug_files = []

    def run(self, runner_exit_code=None, is_bugfix_validation=False):
        _calls["n"] += 1
        tag = BUILD_TYPES[_calls["n"] - 1] if BUILD_TYPES else "run"
        n = ROWS[_calls["n"] - 1] if isinstance(ROWS, list) else ROWS
        st = Result.Status.OK
        if FAILURE_MODE == "first_fails" and _calls["n"] == 1:
            st = Result.Status.FAIL
        return Result(name="Tests", status=st, start_time=1700000000.0,
                      duration=1.0, results=[
            Result(name="%s_%05d" % (tag, i), status=st,
                   start_time=1700000000.0, duration=0.1) for i in range(n)])


class _CH:
    logs = []
    extra_tests_results = []
    client_core_path = ""
    stateful_setup_error = ""

    def __init__(self, *a, **k):
        pass

    def __getattr__(self, name):
        def _ok(*a, **k):
            # `stop_log_exports` is the first teardown step of a normal run and is where
            # the reported job was killed. `stop_server` is the per-build-type
            # stop/re-prepare inside the bugfix-validation loop.
            if name == "stop_log_exports" and BLOCK_ON_STOP_SERVER_CALL is None:
                open(BLOCKED_MARKER, "w").close()
                time.sleep(600)
            if (
                name == "stop_server"
                and BLOCK_ON_STOP_SERVER_CALL is not None
                and _calls["n"] >= BLOCK_ON_STOP_SERVER_CALL
            ):
                open(BLOCKED_MARKER, "w").close()
                time.sleep(600)
            if name == "start" and FAILURE_MODE == "startup_fails" and _calls["n"] >= 1:
                return False
            return True

        return _ok

    def check_fatal_messages_in_logs(self):
        # The real method always returns at least one row, and `extend_sub_results`
        # asserts a non-empty list.
        return [Result.create_from(name=FATAL_ROW_NAME, status=Result.Status.OK)]


class _Targeting:
    # `get_changed_tests` reads the PR diff, absent here; without it the bugfix job
    # early-exits with "No tests to run" before the loop under test.
    def __init__(self, *a, **k):
        pass

    def get_changed_tests(self):
        return ["00001_select_1"]

    def get_all_relevant_tests_with_info(self):
        return (["00001_select_1"], None)


ft.FTResultsProcessor = _Proc
ft.ClickHouseProc = _CH
ft.Targeting = _Targeting
ft.run_tests = lambda **kw: 0
ft.Shell.run = staticmethod(lambda *a, **k: 0)
ft.Shell.check = staticmethod(lambda *a, **k: True)
ft.Shell.get_output = staticmethod(lambda *a, **k: "")
ft.Result.from_commands_run = staticmethod(
    lambda name, command, **k: Result.create_from(name=name, status=Result.Status.OK))
_real_is_file = Path.is_file
Path.is_file = lambda self: True if "clickhouse" in str(self) else _real_is_file(self)

if BUILD_TYPES:
    ft.bugfix_build_types = lambda name: list(BUILD_TYPES)
    ft.find_master_builds = lambda build_types=None: {{bt: "url" for bt in build_types}}
    sys.argv = ["functional_tests.py", "--options", "BugfixValidation, amd_debug",
                "--test", "00001_select_1"]
else:
    sys.argv = ["functional_tests.py", "--options",
                "amd_msan, WasmEdge, parallel, 1/2"]
try:
    ft.main()
except SystemExit as e:
    print("EXIT=%s" % e.code, file=sys.stderr)
"""


_FATAL_ROW_NAME = "FATAL_MARKER_ROW"


def _tail(path, limit=4000):
    """The probe child's own output, for a failure that must name why it exited."""
    with open(path, "rb") as f:
        return f.read().decode(errors="replace")[-limit:] or "(no output)"


def _kill_main_in_teardown(
    tmp_path, build_types=None, rows=6174, block_call=None, failure_mode=""
):
    """Run `main()` and SIGKILL its process group inside teardown.

    Returns (status, per_test_rows). Each row is `(name, status, labels)`. `status` is
    `None` when no result file exists.
    """
    original = Settings.TEMP_DIR
    try:
        _seed_running_result(tmp_path)  # sets TEMP_DIR; the child re-seeds its own
        blocked = os.path.join(str(tmp_path), "blocked")
        script = os.path.join(str(tmp_path), "main_probe.py")
        with open(script, "w", encoding="utf-8") as f:
            f.write(
                _MAIN_PROBE.format(
                    repo=_REPO_ROOT,
                    tmp=str(tmp_path),
                    job=_JOB_NAME,
                    build_types=build_types,
                    rows=rows,
                    block_call=block_call,
                    blocked=blocked,
                    failure_mode=failure_mode,
                    fatal_row=_FATAL_ROW_NAME,
                )
            )
        log = os.path.join(str(tmp_path), "main_probe.log")
        log_handle = open(log, "wb")
        proc = subprocess.Popen(
            [sys.executable, script],
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        deadline = time.monotonic() + 300
        while time.monotonic() < deadline and not os.path.exists(blocked):
            if proc.poll() is not None:
                # The child's output is redirected to `log`, so `proc.stdout` is None:
                # read the file, or this branch raises instead of reporting the exit.
                raise AssertionError(
                    "main() exited before reaching the teardown step this arm blocks "
                    f"in:\n{_tail(log)}"
                )
            time.sleep(0.02)
        assert os.path.exists(blocked), (
            "main() never reached the blocking teardown step, output:\n"
            f"{_tail(log)}"
        )
        # praktika's watchdog kills the whole process group (`TeePopen`).
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        proc.wait(timeout=60)
        log_handle.close()

        Settings.TEMP_DIR = str(tmp_path)
        path = Result.file_name_static(_JOB_NAME)
        if not os.path.exists(path):
            return None, []
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        rows_out = []
        for child in data.get("results", []):
            if child.get("name") == "Tests":
                for row in child.get("results", []):
                    labels = [
                        label["name"] if isinstance(label, dict) else label
                        for label in (row.get("ext") or {}).get("labels", [])
                    ]
                    rows_out.append((row["name"], row["status"], labels))
        return data.get("status"), rows_out
    finally:
        Settings.TEMP_DIR = original


def test_a_job_killed_in_teardown_still_publishes_every_test_row(tmp_path):
    """THE measured statement of the fix, end to end.

    `main` is killed in `stop_log_exports` - the first teardown step, and where the
    reported run died - with all 6174 rows complete in memory. Every one must be on disk.
    Against pristine master this yields 0, which is the reported defect; the mutation arms
    (M1: remove the call) reproduce that here.
    """
    status, rows = _kill_main_in_teardown(tmp_path, rows=6174)

    assert status is not None, "the killed job left no result file at all"
    assert len(rows) == 6174, (
        f"a job killed in teardown published {len(rows)} per-test rows instead of 6174: "
        "the results it had already finished were discarded"
    )
    assert status == Result.Status.RUNNING, (
        f"the checkpoint published status [{status}] instead of leaving it non-terminal; "
        "the runner must be the one to decide a killed job's status"
    )


def test_a_bugfix_job_killed_between_build_types_keeps_the_previous_build(tmp_path):
    """The per-build-type placement, measured rather than inferred.

    Three build types, killed entering the THIRD: the loop has already REPLACED the
    first build's rows with the second's, so only a checkpoint INSIDE the loop can have
    saved them. Two build types cannot measure this - the post-suite checkpoint alone
    still covers the first - which is why this arm uses three.
    """
    status, rows = _kill_main_in_teardown(
        tmp_path,
        build_types=["bt_first", "bt_second", "bt_third"],
        rows=[11, 22, 33],
        block_call=2,
    )

    second = [name for name, _, _ in rows if name.startswith("bt_second_")]
    assert len(second) == 22, (
        f"the build type completed before the kill published {len(second)} rows instead "
        f"of 22 (got {rows[:3]}...): a single post-loop checkpoint would leave only "
        "the stale first build's rows"
    )
    assert status == Result.Status.RUNNING, (
        f"the checkpoint published status [{status}] on a bugfix-validation job: the "
        "inversion runs later, so a terminal status here would publish the "
        "un-inverted verdict"
    )


def test_a_failing_first_build_type_publishes_its_labels_and_fatal_rows(tmp_path):
    """The post-suite checkpoint runs BEFORE the bugfix-validation rows are final.

    Measured: the first build type FAILS, so `test_result.is_ok()` is false and the
    `build_types[1:]` loop never runs at all. Between the post-suite checkpoint and
    teardown, `main` still labels every row with the build type and folds in the fatal-log
    rows via `reconcile_bugfix_crash_repro`. Without a checkpoint after that work, a
    teardown kill publishes rows that do not say which build type produced them and omit
    the fatal that explains the failure - the same data-loss window, one path over.
    """
    status, rows = _kill_main_in_teardown(
        tmp_path,
        build_types=["bt_first", "bt_second"],
        rows=[5, 5],
        failure_mode="first_fails",
    )

    assert status == Result.Status.RUNNING, (
        f"the checkpoint published status [{status}] instead of leaving it non-terminal"
    )
    unlabelled = [name for name, _, labels in rows if "bt_first" not in labels]
    assert not unlabelled, (
        f"{len(unlabelled)} published rows carry no build-type label (e.g. {unlabelled[:3]}): "
        "the labelling at the top of the bugfix-validation block happened after the last "
        "checkpoint, so a teardown kill cannot say which build produced these rows"
    )
    assert any(name == _FATAL_ROW_NAME for name, _, _ in rows), (
        "the fatal-log row folded in by reconcile_bugfix_crash_repro is missing from the "
        f"published result (got {[name for name, _, _ in rows]}): the row that explains a "
        "crash repro was collected and then discarded by the kill"
    )


def test_a_build_type_that_fails_to_start_publishes_its_error_row(tmp_path):
    """The `startup_error` / `setup_error` breaks jump PAST the in-loop checkpoint.

    Measured: the second build type's server never comes up, so `main` appends a
    `Server startup (...)` ERROR row and breaks at once - before reaching the in-loop
    checkpoint, which sits after the tests for that build type. The row naming why the job
    errored must still be published when the kill lands in teardown.

    The `bt_result`-not-ok break is deliberately NOT covered by a separate arm: it is
    already after the in-loop checkpoint, so it needs no new call.
    """
    status, rows = _kill_main_in_teardown(
        tmp_path,
        build_types=["bt_first", "bt_second"],
        rows=[5, 5],
        failure_mode="startup_fails",
    )

    assert status == Result.Status.RUNNING, (
        f"the checkpoint published status [{status}] instead of leaving it non-terminal"
    )
    startup = [
        (name, row_status)
        for name, row_status, _ in rows
        if name.startswith("Server startup")
    ]
    assert startup == [("Server startup (bt_second)", Result.Status.ERROR)], (
        "the ERROR row for the build type whose server failed to start is missing from the "
        f"published result (got {[name for name, _, _ in rows]}): the break that appends it "
        "skips the in-loop checkpoint, so the kill leaves a report that does not explain "
        "the failure"
    )


def test_the_bugfix_validation_block_checkpoints_after_the_build_type_loop():
    """A checkpoint must sit after the build-type loop, inside the bugfix block.

    Structural as well as behavioural: the two arms above are also satisfied by adding a
    checkpoint before every `break` individually, which is more code for the same effect
    and drifts the moment a break is added. Pinning it after the loop keeps one call
    covering all three exits, and fails loudly if a refactor moves it out of the block into
    the shared teardown path, where it would no longer see the bugfix rows.
    """
    main = _find_function(_parse(_JOB_SCRIPT), "main")

    # `main` has TWO `if is_bugfix_validation:` blocks: the setup one that downloads the
    # master binaries, and the post-suite one under test. Select on the build-type loop, the
    # same way the per-build-type arm above does, so the setup block cannot be measured by
    # mistake - it has a `for` loop of its own and no checkpoint, which would fail here for
    # the wrong reason.
    blocks = [
        node
        for node in ast.walk(main)
        if isinstance(node, ast.If)
        and isinstance(node.test, ast.Name)
        and node.test.id == "is_bugfix_validation"
        and any(
            isinstance(sub, ast.Call)
            and _is_named_call(sub, "check_fatal_messages_in_logs")
            for sub in ast.walk(node)
        )
    ]
    assert blocks, "the post-suite `if is_bugfix_validation:` block was not found in main()"

    for block in blocks:
        loops = [
            node
            for node in ast.walk(block)
            if isinstance(node, ast.For)
            and any(
                isinstance(sub, ast.Call)
                and _is_named_call(sub, "check_fatal_messages_in_logs")
                for sub in ast.walk(node)
            )
        ]
        assert loops, f"the block at line {block.lineno} has no build-type loop"
        last_loop_end = max(node.end_lineno for node in loops)
        after_loop = [
            node.lineno
            for node in _checkpoint_calls(block)
            if node.lineno > last_loop_end
        ]
        assert after_loop, (
            f"the bugfix-validation block at line {block.lineno} has no "
            f"{_CHECKPOINT_HELPER} call after its build-type loop (ends at line "
            f"{last_loop_end}): the build-type labels, the reconciled fatal rows and the "
            "startup/setup ERROR rows are all produced after the last checkpoint inside it"
        )


def test_a_probe_that_exits_early_reports_why(tmp_path):
    """The diagnostic path itself, driven rather than inspected.

    When the child exits before blocking, this arm's whole value is naming the cause. The
    child's output goes to a file, so `proc.stdout` is None and reading it raises
    `AttributeError`, hiding the real reason behind a failure in the error handler - which
    is how a broken stub set reached CI unexplained. Force an early exit and require the
    child's own traceback in the message.
    """
    with open(_JOB_SCRIPT, encoding="utf-8") as f:
        marker = "SENTINEL_EXIT_" + str(len(f.read()))

    original = _MAIN_PROBE
    try:
        # Exit before the blocking teardown step, the way an unusable stub set does.
        globals()["_MAIN_PROBE"] = original.replace(
            "try:\n    ft.main()",
            f'raise RuntimeError("{marker}")\ntry:\n    ft.main()',
        )
        with pytest.raises(AssertionError) as excinfo:
            _kill_main_in_teardown(tmp_path, rows=1)
    finally:
        globals()["_MAIN_PROBE"] = original

    assert marker in str(excinfo.value), (
        "the early-exit failure does not carry the child's own output, so a probe that "
        f"cannot run reports nothing about why: {excinfo.value}"
    )


def test_the_probe_reads_no_ambient_workflow_context():
    """The probe must resolve its environment only from what it seeds itself.

    `Settings.WORKFLOW_STATUS_FILE` is built from the default `TEMP_DIR` when the class is
    defined, so the probe's `TEMP_DIR` override does not move it and `_Environment.get`
    reads the context of the job running this test. That context carries real
    `changed_files`, which sends `main` into the batch-skip branch these arms do not
    measure and the stub `Targeting` cannot serve: green on a developer box, `AttributeError`
    only in CI. `TEMP_DIR` alone is not enough.
    """
    assert 'Settings.WORKFLOW_STATUS_FILE = {tmp!r}' in _MAIN_PROBE, (
        "the probe no longer redirects Settings.WORKFLOW_STATUS_FILE into its own temp "
        "dir: it reads the workflow context of the job running this test"
    )


# --- kill-based: atomicity, against a real process death -----------------------------

# Both arms drive PRODUCTION code - the `rename` arm calls `checkpoint_collected_results`,
# the `direct` arm calls `Result.dump` - and differ only in which one they call.
#
# The kill is deterministic rather than timed: `builtins.open` is wrapped so the file
# object returned for the measured write SIGKILLs the process on its Nth `write` call.
# `json.dump` writes incrementally, so this lands strictly between the first byte and the
# last, which is the only window in which the two arms can differ. A `time.sleep` between
# the write's start and the signal cannot do this - measured, it left the plain dump
# complete and the negative control passing vacuously.
_KILL_PROBE = r"""
import builtins, os, signal, sys
sys.path.insert(0, {repo!r})
from ci.jobs.functional_tests import checkpoint_collected_results
from ci.praktika.result import Result
from ci.praktika.settings import Settings

Settings.TEMP_DIR = {tmp!r}
name = {name!r}
mode = sys.argv[1]
KILL_AFTER = {kill_after}

collected = [Result.create_from(name="Tests", status=Result.Status.OK, results=[
    Result.create_from(name="%05d_test" % i, status=Result.Status.OK, info="x" * 200)
    for i in range({rows})
])]

_real_open = builtins.open


class _KillingFile:
    # Wraps the write target and kills the process from inside the serialization.

    def __init__(self, f):
        self._f = f
        self._writes = 0

    def write(self, data):
        self._writes += 1
        if self._writes >= KILL_AFTER:
            self._f.flush()
            os.kill(os.getpid(), signal.SIGKILL)
        return self._f.write(data)

    def __getattr__(self, attr):
        return getattr(self._f, attr)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return self._f.__exit__(*a)


def _patched_open(*args, **kwargs):
    f = _real_open(*args, **kwargs)
    if len(args) > 1 and "w" in str(args[1]):
        return _KillingFile(f)
    return f


open({ready!r}, "w").close()
builtins.open = _patched_open
if mode == "rename":
    checkpoint_collected_results(name, collected, False)
else:
    existing = Result.from_fs(name)
    existing.results = collected
    existing.dump()
builtins.open = _real_open
print("NOT_KILLED", file=sys.stderr)
"""


def _kill_during_write(tmp_path, mode, rows=400, kill_after=5):
    """Kill a real process from inside its result write. Returns (readable, rows).

    `readable` is whether `Result.from_fs` still parses the published file; `rows` is how
    many per-test rows it holds (None when unreadable).
    """
    original = Settings.TEMP_DIR
    try:
        _seed_running_result(tmp_path)
        ready = os.path.join(str(tmp_path), f"ready.{mode}")
        script = os.path.join(str(tmp_path), f"probe_{mode}.py")
        with open(script, "w", encoding="utf-8") as f:
            f.write(
                _KILL_PROBE.format(
                    repo=_REPO_ROOT,
                    tmp=str(tmp_path),
                    name=_JOB_NAME,
                    rows=rows,
                    ready=ready,
                    kill_after=kill_after,
                )
            )
        proc = subprocess.run(
            [sys.executable, script, mode],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=120,
        )
        assert os.path.exists(ready), (
            f"the {mode} probe never reached its write: {proc.stderr.decode()[-2000:]}"
        )
        assert b"NOT_KILLED" not in proc.stderr, (
            f"the {mode} probe survived its write, so no interruption was measured: "
            f"{proc.stderr.decode()[-2000:]}"
        )
        assert proc.returncode == -signal.SIGKILL, (
            f"the {mode} probe exited with {proc.returncode} instead of SIGKILL: "
            f"{proc.stderr.decode()[-2000:]}"
        )

        Settings.TEMP_DIR = str(tmp_path)
        try:
            reread = Result.from_fs(_JOB_NAME)
        except Exception:
            return False, None
        return True, len(reread.results[0].results) if reread.results else 0
    finally:
        Settings.TEMP_DIR = original


def test_a_kill_during_a_plain_dump_destroys_the_result_file(tmp_path):
    """NEGATIVE CONTROL. Without it the arm below passes on any implementation.

    `Result.dump` is `open(..., "w")` + `json.dump`, so it truncates the published file
    before serializing: the same interruption must leave JSON `Result.from_fs` cannot
    parse. This is the failure mode the rename exists to prevent, and asserting it is
    what makes the arm below a measurement rather than a coincidence.
    """
    readable, rows = _kill_during_write(tmp_path, "direct")

    assert not readable, (
        f"a kill during a plain Result.dump left a parseable file with {rows} rows: the "
        "interruption did not land inside the write, so the arm below proves nothing"
    )


def test_a_kill_during_the_checkpoint_leaves_the_result_file_readable(tmp_path):
    """The same interruption against the checkpoint must leave a parseable file.

    Either the previous result or the complete new one - never a truncation. Differs from
    the control above only in calling `checkpoint_collected_results`.
    """
    readable, rows = _kill_during_write(tmp_path, "rename")

    assert readable, (
        "a kill during the checkpoint left a file Result.from_fs cannot parse: the runner "
        "then reports a harder failure than the lost result this replaced"
    )
    assert rows in (0, 400), (
        f"the published file holds {rows} rows - neither the previous result (0) nor the "
        "complete new one (400): the write was not atomic"
    )


def test_the_checkpoint_leaves_no_temporary_file_behind_on_a_kill(tmp_path):
    """A kill mid-write must not leave a stray temp file in the result directory.

    A leftover is unavoidable for a process SIGKILLed mid-write, so this pins the naming
    instead: anyone globbing `result_<job>.json*` must be able to tell the truncation from
    the published result.
    """
    _kill_during_write(tmp_path, "rename")

    published = os.path.basename(Result.file_name_static(_JOB_NAME))
    leftovers = [
        name
        for name in os.listdir(str(tmp_path))
        if name.startswith(published) and name != published
    ]
    for name in leftovers:
        assert name.endswith(".tmp"), (
            f"the checkpoint left [{name}] behind, which does not end in .tmp: a reader "
            f"looking for [{published}*] cannot tell it from the published result"
        )


if __name__ == "__main__":
    import tempfile
    from pathlib import Path

    for fn in (
        test_checkpoint_precedes_every_teardown_step_in_main,
        test_the_bugfix_validation_loop_checkpoints_per_build_type,
        test_every_checkpoint_call_passes_the_collected_results,
        test_checkpoint_uses_the_existing_result_not_a_fresh_one,
        test_checkpoint_publishes_by_rename,
        test_checkpoint_assigns_no_status,
        test_checkpoint_guards_every_step_against_every_exception,
    ):
        fn()
        print(f"ok {fn.__name__}")
    for fn in (
        test_a_job_killed_in_teardown_still_publishes_every_test_row,
        test_a_bugfix_job_killed_between_build_types_keeps_the_previous_build,
        test_checkpoint_persists_the_results,
        test_checkpoint_is_skipped_on_a_local_run,
        test_checkpoint_leaves_the_result_incomplete,
        test_checkpoint_preserves_ext,
        test_a_failing_write_degrades_to_a_no_op,
        test_an_unparseable_result_file_degrades_to_a_no_op,
        test_runner_kill_patch_keeps_the_checkpointed_results,
        test_cidb_ingests_the_children_of_a_killed_checkpointed_result,
        test_a_kill_during_a_plain_dump_destroys_the_result_file,
        test_a_kill_during_the_checkpoint_leaves_the_result_file_readable,
        test_the_checkpoint_leaves_no_temporary_file_behind_on_a_kill,
    ):
        with tempfile.TemporaryDirectory() as d:
            fn(Path(d))
        print(f"ok {fn.__name__}")
    print("ok")
