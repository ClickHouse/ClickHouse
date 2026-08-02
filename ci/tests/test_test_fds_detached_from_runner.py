"""
Regression test: a test process must not inherit the CI runner's output pipe.

Background
----------
`Stateless tests (amd_asan_ubsan, distributed plan, parallel)` jobs
intermittently finished with a job-level ERROR after producing no output at all
for almost two hours, until the 9000s job watchdog killed them. No test failed
and no server log was ever uploaded.

`ci/jobs/functional_tests.py` runs the suite as a shell PIPELINE,
`clickhouse-test ... | ts | tee -a <file>`, and `clickhouse-test` used to spawn
each test with `Popen(..., start_new_session=True)` and no `stdout`/`stderr`
arguments. The spawned `bash -c "<test> > out 2> err"` wrapper therefore
inherited the write end of that pipe (the redirect applies only to the test the
wrapper execs, not to the wrapper itself). A test that escaped into its own
session outlived the worker, so `ts` never saw EOF, `tee` never exited, the
top-level `bash` never exited, and `Shell.run`'s `proc.wait()` plus both reader
joins blocked until the job was SIGKILLed - before the stages that collect the
server log and check for an OOM kill.

`clickhouse-test` now passes explicit sinks for both descriptors, so an escaped
test cannot hold the runner's pipeline open. The wrapper's stderr gets an
artifact of its own rather than the test's `2>` target, because that one is
truncated by the test's redirect and is read back as `stderr`, where content is
itself a verdict and is matched against `MESSAGES_TO_RETRY`.

These tests drive the mechanism directly rather than running a real suite: the
production failure needs a dying server and a two-hour window, but the wedge
reproduces in seconds.

Fixture requirements (each of the negative arms below passes even against the
unpatched spawn, so getting these wrong certifies a broken fix):

* it must be the PIPELINE shape - with a single child, `proc.wait()` returns
  immediately and only the reader joins block;
* the escaping process must be the WRAPPER, whose own fd1/fd2 are the pipe
  while its redirect applies to the command it execs;
* both descriptors matter - detaching stdout alone still leaves the stderr
  reader blocked for the orphan's lifetime.
"""

import ast
import os
import runpy
import subprocess
import sys
import threading
import time
import types
from pathlib import Path

BASH = "/bin/bash"
_CLICKHOUSE_TEST = Path(__file__).resolve().parents[2] / "tests" / "clickhouse-test"

# The orphan outlives its parent by this much. It must comfortably exceed
# PROMPT_S so an unpatched spawn cannot pass by being merely fast, and stay
# short enough that a wedged arm does not stall the suite.
ORPHAN_LIFETIME_S = 8
# A detached pipeline ends as soon as the worker exits; measured at ~0.02s.
PROMPT_S = 3.0

# Emulates one `clickhouse-test` worker: spawn the test wrapper in its own
# session, then exit WITHOUT reaping it, which is what a SIGKILLed worker does.
# `mode` selects the spawn under test.
_WORKER = r"""
import os, subprocess, sys
mode, lifetime, out, err, sink = sys.argv[1:6]
# Mirrors `run_single_test`: the redirect binds the command the wrapper execs,
# so the wrapper itself keeps whatever fd1/fd2 it inherited from us.
command = f"sleep {lifetime} > {out} 2> {err}"
kwargs = {}
if mode in ("detach_stdout", "detach_both"):
    kwargs["stdout"] = subprocess.DEVNULL
if mode == "detach_both":
    kwargs["stderr"] = open(sink, "ab", buffering=0)
subprocess.Popen(command, shell=True, executable=r"%s",
                 start_new_session=True, **kwargs)
print("spawned", flush=True)
os._exit(0)
""" % BASH


def _run_pipeline(tmp_path, mode, pipeline=True):
    """Run the worker inside the runner's pipeline shape and time the waits.

    Returns `(proc_wait_s, both_joins_s)` measured from launch, the two waits
    `Shell.run` performs: `proc.wait()` plus the stdout and stderr reader joins.
    """
    worker = tmp_path / "worker.py"
    worker.write_text(_WORKER)
    args = " ".join(
        str(x)
        for x in (
            mode,
            ORPHAN_LIFETIME_S,
            tmp_path / "test.stdout",
            tmp_path / "test.stderr",
            tmp_path / "wrapper.stderr",
        )
    )
    command = f"{sys.executable} {worker} {args}"
    if pipeline:
        # The exact shape of `run_tests`: `... | ts | tee -a <file>`. `cat`
        # stands in for `ts`/`tee` so the test needs no moreutils.
        command = f"set -o pipefail; {command} | cat | cat"

    started = time.monotonic()
    # pylint:disable-next=consider-using-with; the waits below are the assertion
    proc = subprocess.Popen(
        command,
        shell=True,
        executable=BASH,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    readers = [
        threading.Thread(target=lambda s=stream: [None for _ in s], daemon=True)
        for stream in (proc.stdout, proc.stderr)
    ]
    for reader in readers:
        reader.start()

    proc.wait()
    proc_wait_s = time.monotonic() - started
    for reader in readers:
        reader.join(timeout=ORPHAN_LIFETIME_S * 3)
    return proc_wait_s, time.monotonic() - started


def test_detached_test_does_not_hold_the_runner_pipeline(tmp_path):
    """An escaped test must not keep the runner's pipeline open.

    No timeout and no watchdog are configured, so this passes only because the
    descriptors are detached - not because something killed the orphan.
    """
    proc_wait_s, joins_s = _run_pipeline(tmp_path, "detach_both")
    assert proc_wait_s < PROMPT_S, f"proc.wait() blocked for {proc_wait_s:.2f}s"
    assert joins_s < PROMPT_S, f"reader joins blocked for {joins_s:.2f}s"


def test_inherited_test_wedges_the_runner_pipeline(tmp_path):
    """The unpatched spawn: both waits block for the orphan's whole lifetime.

    This is the arm that fails if the fix is reverted, and it pins the
    mechanism: nothing but the inherited descriptors keeps the pipeline alive.
    """
    proc_wait_s, joins_s = _run_pipeline(tmp_path, "inherit")
    assert proc_wait_s >= ORPHAN_LIFETIME_S - 1, (
        f"expected the wedge, got proc.wait()={proc_wait_s:.2f}s"
    )
    assert joins_s >= ORPHAN_LIFETIME_S - 1


def test_detaching_only_stdout_is_not_enough(tmp_path):
    """Both descriptors must be detached.

    `Shell.run` joins the stdout AND stderr readers before `proc.wait()`, so an
    orphan holding fd2 alone still blocks the runner for its whole lifetime even
    though `proc.wait()` itself returns promptly.
    """
    proc_wait_s, joins_s = _run_pipeline(tmp_path, "detach_stdout")
    assert proc_wait_s < PROMPT_S
    assert joins_s >= ORPHAN_LIFETIME_S - 1, (
        f"expected the stderr reader to block, got {joins_s:.2f}s"
    )


def _spawn_kwargs():
    """The `Popen` keyword arguments of `run_single_test`, as AST nodes.

    Also returns the enclosing function so a sink bound to a local name can be
    resolved *within it*, which is what stops a decoy `open(...)` elsewhere in
    `clickhouse-test` from satisfying the assertions below.
    """
    tree = ast.parse(_CLICKHOUSE_TEST.read_text())
    found = [
        (func, {kw.arg: kw.value for kw in call.keywords})
        for func in ast.walk(tree)
        if isinstance(func, ast.FunctionDef) and func.name == "run_single_test"
        for call in ast.walk(func)
        if isinstance(call, ast.Call) and getattr(call.func, "id", "") == "Popen"
    ]
    assert found, "no Popen call found in run_single_test"
    return found


def _resolve(func, node):
    """`node`, or the value assigned to it if it is a local name of `func`."""
    if not isinstance(node, ast.Name):
        return node
    assigned = [
        assign.value
        for assign in ast.walk(func)
        if isinstance(assign, ast.Assign)
        for target in assign.targets
        if isinstance(target, ast.Name) and target.id == node.id
    ]
    assert len(assigned) == 1, (
        f"expected exactly one assignment to `{node.id}` in run_single_test, "
        f"got {len(assigned)}"
    )
    return assigned[0]


def test_run_single_test_detaches_stdout():
    """The real spawn site must send its stdout to `DEVNULL`.

    The timing tests above emulate the spawn, because driving the real one needs
    a running server, so they cannot catch a change here. Asserting the VALUE and
    not just the presence of the keyword is what makes this a live pin:
    `stdout=sys.stdout` or `stdout=None` restores the wedge while still passing a
    presence check.
    """
    for _func, kwargs in _spawn_kwargs():
        assert "stdout" in kwargs, (
            "run_single_test must pass an explicit stdout so a test cannot inherit "
            "the runner's output pipe"
        )
        assert ast.dump(kwargs["stdout"]) == ast.dump(
            ast.parse("subprocess.DEVNULL", mode="eval").body
        ), (
            "the wrapping shell's stdout must be subprocess.DEVNULL, got "
            f"`{ast.unparse(kwargs['stdout'])}`"
        )


def test_wrapper_stderr_goes_to_its_own_artifact():
    """The wrapper's stderr must be a file of its own, not `DEVNULL`.

    The test's own output is redirected by the command, but the wrapping shell
    reports job status (a killed or segfaulting test, a bad redirect target, a
    syntax error) on its own fd2. `DEVNULL` would drop those - the same class of
    loss this change exists to fix.

    It must not be `stderr_file` either, for two measured reasons: the test
    truncates that file when its own redirect opens it, which destroys anything
    written before that; and it is read back as `stderr`, where content is itself
    a verdict and is matched against `MESSAGES_TO_RETRY` - whose entries include
    `No such file or directory`, which a quoted command line can contain.
    """
    for func, kwargs in _spawn_kwargs():
        assert "stderr" in kwargs, "run_single_test must pass an explicit stderr"
        sink = _resolve(func, kwargs["stderr"])
        assert "DEVNULL" not in ast.unparse(sink), (
            "the wrapping shell's stderr must be reported, not sent to DEVNULL"
        )
        assert isinstance(sink, ast.Call) and getattr(sink.func, "id", "") == "open", (
            f"expected the stderr sink to be an `open(...)`, got `{ast.unparse(sink)}`"
        )
        path, mode = sink.args[0], sink.args[1]
        assert ast.unparse(path) == "self.wrapper_stderr_file", (
            "the wrapper's stderr must go to its own artifact, not the test's `2>` "
            f"target, got `{ast.unparse(path)}`"
        )
        assert ast.literal_eval(mode) == "ab", (
            f"the sink must append so nothing is truncated, got {ast.unparse(mode)}"
        )


def test_wrapper_stderr_is_kept_out_of_the_retry_matcher():
    """The relocated text must be reported without reaching the retry matcher.

    `process_result_impl` builds the description the matcher sees; the wrapper's
    lines are read there but appended by `process_result`, which runs after the
    retry check. Pin that ordering: appending them to `debug_log` instead puts a
    quoted command line in front of `MESSAGES_TO_RETRY`.
    """
    tree = ast.parse(_CLICKHOUSE_TEST.read_text())
    functions = {
        node.name: node
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef)
    }
    attribute = "wrapper_stderr_report"

    def stores_to(func):
        return [
            node
            for node in ast.walk(functions[func])
            if isinstance(node, ast.Attribute)
            and node.attr == attribute
            and isinstance(node.ctx, ast.Store)
        ]

    assert stores_to("process_result_impl"), (
        f"`process_result_impl` must read the wrapper's stderr into {attribute}"
    )
    reported = [
        node
        for node in ast.walk(functions["process_result"])
        if isinstance(node, ast.AugAssign)
        and attribute in ast.unparse(node.value)
    ]
    assert reported, (
        f"`process_result` must report {attribute}, so the text is visible in the "
        "job log without being matched against MESSAGES_TO_RETRY"
    )
    matcher_fed = [
        node
        for node in ast.walk(functions["process_result_impl"])
        if isinstance(node, ast.AugAssign)
        and isinstance(node.target, ast.Name)
        and node.target.id in ("debug_log", "stderr", "description")
        and attribute in ast.unparse(node.value)
    ]
    assert not matcher_fed, (
        f"{attribute} must not be folded into the description built by "
        "`process_result_impl`: that is what the retry matcher is handed"
    )


def _drive_process_result(tmp_path, wrapper_line, same_path):
    """Run the real `process_result_impl` + `process_result` over a wrapper message.

    `same_path` writes the line to the test's `2>` target instead of the wrapper's
    own artifact, which is what the sink used to be. Returns the
    `MESSAGES_TO_RETRY` entries the retry matcher would see and whether the line
    was reported to the job log.
    """
    ct = runpy.run_path(str(_CLICKHOUSE_TEST))
    test_case_cls, status_cls = ct["TestCase"], ct["TestStatus"]

    class _Args:
        debug_log_file = str(tmp_path / "dbg.log")
        bash_tracing_file = str(tmp_path / "trace.log")
        stop = False
        testcase_database = "test_db"
        test_runs = 1
        flaky_check = False
        cloud = False
        record = False
        unified = 3
        check_zookeeper_session = False
        dont_retry_failures = False

    case = test_case_cls.__new__(test_case_cls)
    case.name = "00001_probe"
    case.stdout_file = str(tmp_path / "t.stdout")
    case.stderr_file = str(tmp_path / "t.stderr")
    case.wrapper_stderr_file = case.stderr_file + "-wrapper"
    case.fatal_sanitizer_prefix = case.stderr_file + "-fatal"
    case.reference_file = str(tmp_path / "t.reference")
    case.testcase_args = case.args = _Args()
    case.show_whitespaces_in_diff = False
    case.tags = set()
    case.debug_log_retry_substitution = None
    case.wrapper_stderr_report = ""
    case.suite = types.SimpleNamespace(blacklist_check=set())

    for path in (case.stdout_file, case.stderr_file, case.reference_file):
        Path(path).write_text("")
    sink = case.stderr_file if same_path else case.wrapper_stderr_file
    Path(sink).write_text(wrapper_line)
    if same_path and os.path.exists(case.wrapper_stderr_file):
        os.remove(case.wrapper_stderr_file)

    # A non-zero exit code, the shape every arm of the routing table produces.
    proc = types.SimpleNamespace(returncode=1, pid=os.getpid())
    result = case.process_result_impl(proc, 1.0)
    retry_input = case.retry_matcher_input(result.description)
    matched = [msg for msg in ct["MESSAGES_TO_RETRY"] if msg in retry_input]
    reported = case.process_result(
        result, {status: f"[ {status.name} ]" for status in status_cls}
    )
    return matched, wrapper_line.strip() in reported.description


def test_wrapper_message_does_not_trigger_a_retry(tmp_path):
    """A quoted command line must not make a deterministic failure look flaky.

    This is the line the `stdout redirect target unopenable` arm really produces;
    it contains a verbatim `MESSAGES_TO_RETRY` entry. The `same_path` arm is the
    mutation: pointing the sink back at the test's `2>` target reddens this, which
    is what makes the assertion load-bearing rather than incidental.
    """
    line = "/bin/bash: line 1: /nonexistent/x.stdout: No such file or directory\n"

    matched, reported = _drive_process_result(tmp_path, line, same_path=False)
    assert matched == [], f"wrapper text reached the retry matcher: {matched}"
    assert reported, "the wrapper's message must still be reported"

    matched, reported = _drive_process_result(tmp_path, line, same_path=True)
    assert "No such file or directory" in matched, (
        "the mutation arm must show the retry channel this fix closes; if it does "
        "not, this test no longer pins anything"
    )
    assert reported


def test_empty_wrapper_file_is_not_left_behind():
    """An empty wrapper file must be removed while it is still known to be empty.

    `Popen` creates it whether or not the shell writes anything, and the OK-path
    cleanup does not run for a failing test, so without this every non-passing
    test leaks an empty file into the suite's tmp dir - which defaults to a
    directory inside the source tree. Measured before the guard existed: a
    220-test batch left 13283 of them.
    """
    tree = ast.parse(_CLICKHOUSE_TEST.read_text())
    run_single = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == "run_single_test"
    )
    guarded = [
        node
        for node in ast.walk(run_single)
        if isinstance(node, ast.If)
        and "getsize(self.wrapper_stderr_file) == 0" in ast.unparse(node.test)
        and "remove(self.wrapper_stderr_file)" in ast.unparse(node.body)
    ]
    assert guarded, (
        "run_single_test must remove the wrapper's file while it is still empty, so "
        "a non-passing test does not leak one into the suite's tmp dir"
    )


def test_child_side_messages_are_reported_exactly_as_before(tmp_path):
    """The classes that always went to the test's `2>` target must not move.

    `Permission denied` and `No such file or directory` from a failed exec are
    emitted after the fork, with the redirect already installed, so they are the
    test's own stderr and must keep reaching the verdict and the matcher.
    """
    line = "/bin/bash: line 1: /tmp/x.sh: Permission denied\n"
    matched, reported = _drive_process_result(tmp_path, line, same_path=True)
    assert "Permission denied" in matched, (
        "a child-side message is the test's own stderr and must still be matched"
    )
    assert reported


def test_single_child_fixture_cannot_detect_the_wedge(tmp_path):
    """Negative arm: without the pipeline this passes even unpatched.

    Kept deliberately - a "simplified" fixture that drops `| cat | cat` reports
    `proc.wait()` returning at once and silently stops testing anything.
    """
    proc_wait_s, _ = _run_pipeline(tmp_path, "inherit", pipeline=False)
    assert proc_wait_s < PROMPT_S
