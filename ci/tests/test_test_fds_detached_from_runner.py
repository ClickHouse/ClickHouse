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
test cannot hold the runner's pipeline open.

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
import subprocess
import sys
import threading
import time
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


def test_run_single_test_passes_explicit_sinks():
    """The real spawn site must keep passing both sinks.

    The timing tests above emulate the spawn, because driving the real one needs
    a running server. This asserts the property directly on the source, so
    dropping either argument from `run_single_test` fails here.
    """
    tree = ast.parse(_CLICKHOUSE_TEST.read_text())
    spawns = [
        sorted(kw.arg for kw in call.keywords)
        for func in ast.walk(tree)
        if isinstance(func, ast.FunctionDef) and func.name == "run_single_test"
        for call in ast.walk(func)
        if isinstance(call, ast.Call) and getattr(call.func, "id", "") == "Popen"
    ]
    assert spawns, "no Popen call found in run_single_test"
    for kwargs in spawns:
        assert "stdout" in kwargs and "stderr" in kwargs, (
            "run_single_test must pass explicit stdout and stderr so a test cannot "
            f"inherit the runner's output pipe, got {kwargs}"
        )


def test_wrapper_stderr_is_kept_not_discarded():
    """The wrapper's stderr must reach the stderr artifact, not `DEVNULL`.

    The test's own output is redirected by the command, but the wrapping shell
    still reports failures that happen before that redirect takes effect (a bad
    redirect target, a syntax error) on its own fd2. Sending those to `DEVNULL`
    would drop shell-level diagnostics - the same class of loss this change
    exists to fix - so pin the sink to `self.stderr_file`.
    """
    tree = ast.parse(_CLICKHOUSE_TEST.read_text())
    spawns = [
        {kw.arg: kw.value for kw in call.keywords}
        for func in ast.walk(tree)
        if isinstance(func, ast.FunctionDef) and func.name == "run_single_test"
        for call in ast.walk(func)
        if isinstance(call, ast.Call) and getattr(call.func, "id", "") == "Popen"
    ]
    assert spawns, "no Popen call found in run_single_test"
    for kwargs in spawns:
        stderr = ast.dump(kwargs["stderr"])
        assert "DEVNULL" not in stderr, (
            "the wrapping shell's stderr must go to the stderr artifact, not DEVNULL"
        )

    # The sink must be opened from `self.stderr_file`, in append mode so it adds
    # to what the test itself writes there rather than truncating it.
    source = _CLICKHOUSE_TEST.read_text()
    assert 'open(self.stderr_file, "ab"' in source, (
        "the wrapper's stderr sink must append to self.stderr_file"
    )


def test_single_child_fixture_cannot_detect_the_wedge(tmp_path):
    """Negative arm: without the pipeline this passes even unpatched.

    Kept deliberately - a "simplified" fixture that drops `| cat | cat` reports
    `proc.wait()` returning at once and silently stops testing anything.
    """
    proc_wait_s, _ = _run_pipeline(tmp_path, "inherit", pipeline=False)
    assert proc_wait_s < PROMPT_S
