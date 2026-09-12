"""
Regression tests for the per-test wrapper stdio invariant in tests/clickhouse-test.

A **per-test wrapper process** (and hence its descendants) must never hold a
descriptor belonging to the runner's own **stdout or stderr**.  The assertions
below are scoped to the test's own process group accordingly.

It matters because in CI the runner heads a shell pipeline
(``ci/jobs/functional_tests.py``: ``clickhouse-test | ts | tee -a <file>``), so a
wrapper that outlives the runner keeps that pipeline from ever seeing EOF, and the
runner's real exit code is replaced by the job-level wall-clock timeout.

See ``tests/clickhouse-test-process-management.md`` for the mechanism, the fd-0
carve-out and the worker/manager scope.
"""

import os
import re
import runpy
import shlex
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_CLICKHOUSE_TEST = str(_REPO_ROOT / "tests" / "clickhouse-test")
# A test that prints its line and then stays alive long enough to be inspected
# and to outlive the runner.
_TEST = "02_wrapper_stdio_wedge"
# A test that writes to its own stderr and then dies from a signal, so bash emits
# a job-control diagnostic into the same per-test stderr file.
_TEST_SIGNAL = "03_wrapper_stderr_signal"
# The suite directory is also where the runner puts its per-test files: `args.tmp`
# defaults to `args.queries`, so `suite_tmp_path` is the suite path itself.
_SUITE_TMP = _REPO_ROOT / "ci" / "tests" / "0_stateless"
# `TestCase.stdout_file` is `<suite_tmp>/<name><suffix>.stdout`, and the suffix is
# `.{pid}` of the *worker* process for a concurrent test - so glob for it rather
# than reconstructing the name.
_TEST_STDOUT_GLOB = f"{_TEST}*.stdout"

# Import helpers straight from clickhouse-test so path/name changes propagate.
# runpy.run_path handles the missing .py extension and the hyphen in the name.
_ct = runpy.run_path(_CLICKHOUSE_TEST)
pgrep = _ct["pgrep"]
_GROUP_PID_PATH = _ct["_GROUP_PID_PATH"]
_GROUP_PID_NAME = _ct["_GROUP_PID_NAME"]
_CGROUP_VERSION = _ct["CGROUP_VERSION"]
_FailureReason = _ct["FailureReason"]

# The wrapper must be observed within this many seconds of starting the runner.
_SPAWN_TIMEOUT = 90
# Once the runner is gone, a correctly-detached pipeline must finish well within
# this bound.  On the unfixed code it does not finish at all (it waits for the
# orphan, i.e. the fixture's full 120s), so any value comfortably below the
# fixture's lifetime discriminates.
_PIPELINE_TIMEOUT = 25


def _fd_target(pid: int, fd: int):
    try:
        return os.readlink(f"/proc/{pid}/fd/{fd}")
    except OSError:
        return None


def _clear_group_pid_files():
    _GROUP_PID_PATH.mkdir(parents=True, exist_ok=True)
    for f in _GROUP_PID_PATH.glob(f"{_GROUP_PID_NAME}.*"):
        f.unlink(missing_ok=True)


def _clear_run_artifacts(test=None):
    """
    Remove what the runner leaves behind when we kill it mid-test.

    `args.tmp` defaults to `args.queries`, so the per-test stdout/stderr files
    and the per-test tmp directory land in ci/tests/0_stateless/. Killed runs
    never reach the cleanup in `process_result_impl`, and the tmp directories are
    not gitignored - so leaving them behind would dirty the working tree.

    ``test`` selects which fixture's files to clear; the default covers every
    fixture in the suite, which is what the shared teardown wants.
    """
    for f in _SUITE_TMP.glob(f"{test or ''}*"):
        if f.suffix in (".stdout", ".stderr", ".debuglog") or ".stderr-fatal" in f.name:
            f.unlink(missing_ok=True)
    for d in _SUITE_TMP.glob("test_*"):
        if d.is_dir():
            shutil.rmtree(d, ignore_errors=True)


_CGROUP_ROOT = Path("/sys/fs/cgroup")
_CGROUP_GLOB = "clickhouse-test-*"


def _cgroup_snapshot():
    """
    The set of runner-created cgroup directories at the cgroup root.

    Empty when the root is unreadable (an unprivileged run cannot create them
    there at all, so there is nothing to track).
    """
    try:
        return set(_CGROUP_ROOT.glob(_CGROUP_GLOB))
    except OSError:
        return set()


def _own_cgroup_names(test):
    """
    The cgroup names the workers that ran ``test`` in this invocation would create.

    ``run_single_test`` names the cgroup ``clickhouse-test-{os.getpid()}``, and
    ``TestCase.__init__`` builds the per-test file names in that same process, so
    ``<test>.<pid>.stdout`` carries exactly the owning worker's pid.  That is the
    only source available: ``run_single_test`` unlinks its PGID file when its test
    finishes, so none of those survives to a teardown that runs after the runner
    has exited.

    Returns an empty set if no such artifact is there, which is what makes the
    caller's guard falsifiable - a run whose owner cannot be identified must fail
    loudly rather than clean up nothing and pass.  Nothing is added
    unconditionally for that reason, the runner's own pid included: this fixture
    carries no ``sequential`` tag, so its spawns happen in workers.
    """
    owner = re.compile(rf"^{re.escape(test)}\.(\d+)\.")
    names = set()
    for f in _SUITE_TMP.glob(f"{test}.*"):
        found = owner.match(f.name)
        if found:
            names.add(f"clickhouse-test-{found.group(1)}")
    return names


def _remove_own_cgroups(before, own):
    """
    Remove the cgroup directories this invocation created.

    The test drives the real `--memory-limit` path, whose setup does
    `os.makedirs("/sys/fs/cgroup/<name>")` *before* the `memory.max` write that
    fails, so the directory survives the failure.  `cleanup_cgroup` does not
    remove it: for cgroup v2 it builds the path from `/proc/self/cgroup`
    (`/sys/fs/cgroup/<current slice>/<name>`), which is not the root-level path
    setup created.  That mismatch is a pre-existing defect in the runner and is
    deliberately out of scope here, so the test cleans up after itself instead -
    otherwise the privileged CI Tests job would leak one empty root-level cgroup
    per run.

    A path is removed only if it is both new *and* in `own`.  Appearing after the
    snapshot does not establish ownership: the job runs `--cgroupns=host`, so a
    co-scheduled runner's directory is visible here, and it is removable while
    still empty between its `makedirs` and its `memory.max` write.  The name test
    keeps this teardown off it; the snapshot diff is kept as well, so a recycled
    pid from an earlier run is not adopted.

    That filter is a pid, and the container does not share the host's pid
    namespace, so it identifies the owner only within this container: a directory
    another container created inside the snapshot window under a colliding pid
    would still match.  Naming it out of reach of that would take a host-unique
    token in the name, which belongs to the runner rather than here.

    `rmdir` only: an empty cgroup directory is removable, and a non-empty one
    must be left alone rather than recursively deleted.

    Returns the paths it could not remove, so the caller can fail rather than
    leak silently: this runs as root in the CI Tests job, where a retained
    root-level cgroup accumulates one directory per run.  The per-path
    ``except`` is kept so one stubborn directory does not hide the others.
    """
    leaked = set()
    for path in _cgroup_snapshot() - before:
        if path.name not in own:
            continue
        try:
            os.rmdir(path)
        except OSError:
            leaked.add(path)
    return leaked


def _await_wrapper_pgid(deadline: float):
    """
    Wait for a live per-test wrapper and return its PGID.

    ``run_single_test`` writes the wrapper's PGID to a per-worker file
    immediately after ``Popen``, so that file is the reliable handle: the
    wrapper is a child of a *worker*, not of the runner itself.
    """
    while time.monotonic() < deadline:
        for f in _GROUP_PID_PATH.glob(f"{_GROUP_PID_NAME}.*"):
            try:
                pgid = int(f.read_text())
            except (OSError, ValueError):
                continue
            if pgid and pgrep(pgid=pgid):
                return pgid
        time.sleep(0.05)
    return None


def _kill_group(pgid):
    if not pgid:
        return
    for proc in pgrep(pgid=pgid):
        try:
            os.kill(proc[0], signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass


def _reap_runner(runner):
    """
    Kill the runner *and its worker processes*.

    The runner forks parallel workers as `multiprocessing.Process` children in
    its own process group, so killing the runner alone orphans them. The runner
    is started with `start_new_session=True` here precisely so its group can be
    signalled without touching pytest's own group.
    """
    try:
        os.killpg(runner.pid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        try:
            runner.kill()
        except ProcessLookupError:
            pass
    runner.wait()


def _runner_argv(test=_TEST):
    argv = [
        sys.executable,
        _CLICKHOUSE_TEST,
        "--queries",
        "ci/tests",
        test,
    ]
    # The fixture runs no queries, but the runner still needs a binary to reach
    # the server for its own bookkeeping.  In CI the binary is on PATH; locally
    # allow pointing at one explicitly.
    binary = os.environ.get("CLICKHOUSE_BINARY")
    if binary:
        argv[2:2] = ["--binary", binary]
    return argv


@pytest.mark.skipif(
    not Path("/proc").is_dir(),
    reason="the descriptor comparison reads /proc/<pid>/fd, which only exists on "
    "Linux; the CI Tests job runs on Linux and there is "
    "no macOS ci/tests job, so coverage is unaffected",
)
def test_test_process_group_does_not_hold_runner_stdio():
    """
    While a test is running, no process in the test's own process group may hold
    the runner's stdout or stderr.

    This is the direct, mechanism-level assertion of the invariant, and it
    covers descendants transitively: they inherit from the wrapper shell, so
    once the wrapper's own streams are redirected none of them can reach the
    runner's stdio.
    """
    _clear_group_pid_files()
    _clear_run_artifacts()

    # Give the runner a pipe for stdout/stderr, exactly like the CI pipeline.
    read_fd, write_fd = os.pipe()
    runner = subprocess.Popen(
        _runner_argv(), stdout=write_fd, stderr=write_fd, cwd=str(_REPO_ROOT),
        start_new_session=True,
    )
    os.close(write_fd)
    pgid = None
    try:
        runner_stdout = _fd_target(runner.pid, 1)
        runner_stderr = _fd_target(runner.pid, 2)
        assert runner_stdout, "could not read the runner's fd 1"

        pgid = _await_wrapper_pgid(time.monotonic() + _SPAWN_TIMEOUT)
        assert pgid, "no live per-test wrapper process group was observed"

        group = pgrep(pgid=pgid)
        assert group, f"process group {pgid} disappeared before it was inspected"

        holders = []
        for proc in group:
            pid = proc[0]
            targets = (_fd_target(pid, 1), _fd_target(pid, 2))
            if runner_stdout in targets or (
                runner_stderr and runner_stderr in targets
            ):
                holders.append((pid, proc[3], targets))

        assert not holders, (
            "processes in the test's process group hold the runner's stdio "
            f"(runner fd1={runner_stdout}, fd2={runner_stderr}):\n"
            + "\n".join(f"  pid={p} fds={t} cmd={c}" for p, c, t in holders)
        )
    finally:
        _kill_group(pgid)
        _reap_runner(runner)
        # Drain so the runner is never blocked on a full pipe.
        os.set_blocking(read_fd, False)
        drain_deadline = time.monotonic() + 10
        while time.monotonic() < drain_deadline:
            try:
                if not os.read(read_fd, 65536):
                    break
            except BlockingIOError:
                if runner.poll() is not None:
                    break
                time.sleep(0.05)
        os.close(read_fd)
        _clear_group_pid_files()
        _clear_run_artifacts()


def test_orphaned_test_process_does_not_wedge_the_ci_pipeline(tmp_path):
    """
    A per-test wrapper that outlives the runner must not keep the CI pipeline
    open, and the pipeline must report the runner's own exit status.

    The orphan is deliberately left **alive** while the pipeline's completion is
    observed: the fix works by removing the coupling, not by reaping the orphan,
    and asserting on a still-running orphan is what makes this test meaningful
    rather than passing because the orphan happened to exit.
    """
    _clear_group_pid_files()
    _clear_run_artifacts()

    # Put the pipeline in a script so the outer shell's own command line does not
    # mention clickhouse-test - otherwise the runner lookup below would match it.
    script = tmp_path / "pipeline.sh"
    script.write_text(
        "#!/usr/bin/env bash\n"
        "set -o pipefail\n"
        + shlex.join(_runner_argv())
        + " | cat > /dev/null\n"
    )
    script.chmod(0o755)

    pipeline = subprocess.Popen(
        [str(script)], cwd=str(_REPO_ROOT), start_new_session=True
    )
    pgid = None
    pipeline_rc = None
    try:
        deadline = time.monotonic() + _SPAWN_TIMEOUT
        pgid = _await_wrapper_pgid(deadline)
        assert pgid, "no live per-test wrapper process group was observed"

        runner_pid = None
        while time.monotonic() < deadline:
            # `pgrep` passes `ps -ww`, without which the command column is cut to
            # the terminal width (80 by default) and the runner's absolute path
            # never matches - the CI checkout is deep enough for that to happen.
            for pid, _, _, _ in pgrep(
                ppid=pipeline.pid, command=_CLICKHOUSE_TEST
            ):
                runner_pid = pid
                break
            if runner_pid:
                break
            time.sleep(0.05)
        assert runner_pid, "could not locate the clickhouse-test runner process"

        # Tear the runner tree down without reaping the wrapper group. This is
        # the observed CI shape: the runner exits (after joining or force-killing
        # its workers) while a wrapper in its own session survives.
        victims = [runner_pid] + [
            pid for pid, _, _, _ in pgrep(ppid=runner_pid) if pid != pgid
        ]
        for pid in victims:
            try:
                os.kill(pid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass

        try:
            pipeline_rc = pipeline.wait(timeout=_PIPELINE_TIMEOUT)
        except subprocess.TimeoutExpired:
            pipeline_rc = None

        # Vacuity guard: the orphan must still be alive, otherwise the pipeline
        # could have finished simply because the descriptor holder died.
        survivors = pgrep(pgid=pgid)
        assert survivors, (
            "the orphaned test process group exited on its own, so this run "
            "does not exercise the invariant"
        )

        assert pipeline_rc is not None, (
            f"the CI pipeline did not finish within {_PIPELINE_TIMEOUT}s after the "
            f"runner was gone - an orphaned test process is holding it open "
            f"({len(survivors)} process(es) alive in group {pgid})"
        )
        # The pipeline must surface the runner's own status (SIGKILL here), not a
        # value produced by an external timeout killing the wedged pipeline.
        assert pipeline_rc == -signal.SIGKILL or pipeline_rc == 128 + int(
            signal.SIGKILL
        ), f"unexpected pipeline exit status {pipeline_rc}"
    finally:
        _kill_group(pgid)
        if pipeline_rc is None:
            try:
                os.killpg(pipeline.pid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass
            pipeline.wait()
        _clear_group_pid_files()
        _clear_run_artifacts()


def test_test_output_is_still_collected_from_the_per_test_files():
    """
    No-regression guard: redirecting the wrapper's own streams must not perturb
    the test's output.

    The assertion is on the **runner-managed** per-test stdout file - the one
    ``run_single_test`` names in its ``{test} > {stdout} 2> {stderr}`` command and
    ``process_result_impl`` reads back - located by globbing ``suite_tmp_path``,
    because its basename carries the worker's pid for a concurrent test.

    Passes on both the unfixed and the fixed runner - it exists to prove the
    redirect loses nothing, not to discriminate.
    """
    _clear_group_pid_files()
    _clear_run_artifacts()

    runner = subprocess.Popen(
        _runner_argv(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        cwd=str(_REPO_ROOT), start_new_session=True,
    )
    pgid = None
    try:
        pgid = _await_wrapper_pgid(time.monotonic() + _SPAWN_TIMEOUT)
        assert pgid, "no live per-test wrapper process group was observed"

        # The fixture writes its line before sleeping, so the runner-managed
        # per-test stdout file must contain it while the test is still running.
        deadline = time.monotonic() + 30
        content = ""
        matches = []
        while time.monotonic() < deadline:
            matches = sorted(_SUITE_TMP.glob(_TEST_STDOUT_GLOB))
            if len(matches) == 1:
                content = matches[0].read_text()
                if content.strip():
                    break
            time.sleep(0.1)

        assert len(matches) == 1, (
            f"expected exactly one runner-managed per-test stdout file matching "
            f"{_SUITE_TMP / _TEST_STDOUT_GLOB}, found {[str(m) for m in matches]}"
        )
        assert content.strip() == "1", (
            "the test's own stdout was not collected into the runner-managed "
            f"per-test file ({matches[0]}), got {content!r}"
        )
    finally:
        # Kill the wrapper group first: the runner is blocked in proc.wait() on
        # it, and (with the fix) it is in its own session, so killing only the
        # runner would leave the wrapper running for the fixture's full sleep.
        _kill_group(pgid)
        _reap_runner(runner)
        _clear_group_pid_files()
        _clear_run_artifacts()


@pytest.mark.skipif(
    _CGROUP_VERSION != 2,
    reason="the negative-limit trigger is fatal only on cgroup v2 (on v1, -1 means "
    "unlimited, so the fatal branch is never taken); the CI Tests job runs on "
    "Linux with --cgroupns=host and cgroup v2, so it is covered there",
)
def test_fatal_cgroup_failure_is_reported_as_the_test_result():
    """
    A ``preexec_fn`` that dies before ``exec`` must still have its diagnostic
    reported as this test's failure, exactly once per reported attempt.

    The truncation is visible only *across* attempts sharing one stderr file, and
    the flags below - not the errno - are what force three of them, because the
    errno depends on privilege and only one of the two is in ``MESSAGES_TO_RETRY``:
    unprivileged, ``makedirs`` under cgroupfs fails with ``Permission denied`` and
    is retried; as root, which is how CI runs, the ``memory.max`` write fails with
    ``Invalid argument`` and is not.
    """
    _clear_group_pid_files()
    _clear_run_artifacts()

    argv = _runner_argv()
    argv.insert(-1, "--memory-limit=-1")
    # Fix the number of reported attempts by the flags rather than by the errno
    # (see the docstring).  `--long-test-runs-ratio 1.0` is required
    # because the fixture carries the `long` tag and `get_runs` scales a long
    # test's repetitions by that ratio, whose 0.1 default would collapse back to
    # a single run.  `--dont_retry_failures` removes the privilege-dependent
    # extra attempts, so the expected count is exact rather than a lower bound.
    argv[-1:-1] = [
        "--test-runs",
        "3",
        "--long-test-runs-ratio",
        "1.0",
        "--dont_retry_failures",
    ]
    cgroups_before = _cgroup_snapshot()
    proc = None
    # Everything after the snapshot is inside the `try`, so the cleanup in
    # `finally` also covers a `subprocess.TimeoutExpired` from the run itself.
    # `Popen` rather than `subprocess.run` because the latter's timeout path kills
    # the runner's pid alone, orphaning the forked workers that own the wrappers.
    try:
        proc = subprocess.Popen(
            argv,
            cwd=str(_REPO_ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
        out_s, err_s = proc.communicate(timeout=300)
        out = out_s + err_s

        # Vacuity guard: the wrapper really did take the fatal branch and wrote its
        # diagnostic to the per-test stderr file.  This holds whether or not the
        # harness goes on to report it, so it cannot mask the assertions below.
        written = "".join(
            f.read_text(errors="replace") for f in sorted(_SUITE_TMP.glob(f"{_TEST}*.stderr"))
        )
        assert "Failed to configure cgroup" in written, (
            "the fatal cgroup branch was not reached, so this run does not "
            f"exercise the guard:\n{out}"
        )

        # The teardown attributes the cgroups it removes to this invocation's pids,
        # so it is only non-vacuous while that attribution is available.  Asserted
        # here rather than in `finally`, where it would mask a real failure.
        assert _own_cgroup_names(_TEST), (
            "no owning pid could be derived from this run, so the cgroup teardown "
            f"would silently remove nothing:\n{out}"
        )

        # Without the pre-created stdout file the normalization raises and `run`'s
        # generic handler reports `UNKNOWN` / "Test internal error" with a
        # `FileNotFoundError` traceback, never reading that stderr file at all.
        for marker in ("Test internal error", "FileNotFoundError", "[ UNKNOWN ]"):
            assert marker not in out, (
                f"the fatal cgroup diagnostic was discarded: found {marker!r}, so "
                "the failure was reported as an unrelated internal error instead "
                f"of as this test's failure:\n{out}"
            )
        assert "Failed to configure cgroup" in out, (
            "the fatal cgroup diagnostic reached the per-test stderr file but was "
            f"not reported as part of the test's failure:\n{out}"
        )

        # It must be reported *as this test's failure*: a non-zero runner status,
        # the FAIL marker (`colored()` is plain text because stdout is a pipe here)
        # and the exit-code reason built from `FailureReason.EXIT_CODE`.
        assert proc.returncode == 1, (
            f"expected the runner to exit 1, got {proc.returncode}:\n{out}"
        )
        assert "[ FAIL ]" in out, f"the test was not reported as FAIL:\n{out}"
        # `proc.returncode` above is the *runner's* status; the wrapper's own exit
        # status is a separate observable, appended to the description by
        # `description += str(proc.returncode)`.  Assert the numeric value, and
        # build the expected text from `FailureReason` rather than typing it, so a
        # change to the enum's spelling or to the space `process_result` adds after
        # `reason.value` cannot silently make this vacuous - the value already ends
        # in a space, so the emitted text carries two.
        expected_reason = f"Reason: {_FailureReason.EXIT_CODE.value} 1"
        assert expected_reason in out, (
            "the reported failure reason did not carry the wrapper's numeric exit "
            f"status: expected {expected_reason!r}\n{out}"
        )

        # Each reported attempt must carry the diagnostic exactly once. With an
        # append-mode open the later attempts accumulate: the counts observed on
        # the unfixed code were [1, 2, 3] instead of [1, 1, 1].
        attempts = re.split(r"(?=Reason: return code)", out)[1:]
        counts = [a.count("Failed to configure cgroup") for a in attempts]
        # Assert the shape before the contents: a single-attempt run would make
        # the count assertion below hold vacuously, so a flag or semantics change
        # that stops producing three attempts must fail loudly here instead.
        assert len(counts) == 3, (
            "expected exactly 3 reported attempts from `--test-runs 3` (the "
            "multi-attempt shape is the only one the truncation is visible in), "
            f"got {len(counts)}: {counts}\n{out}"
        )
        assert set(counts) == {1}, (
            "the per-test stderr file was not truncated before each attempt, so a "
            f"later attempt reported earlier attempts' diagnostics: per-attempt "
            f"counts {counts}, expected 1 in each\n{out}"
        )
    finally:
        # Group-aware and idempotent, so a timeout cannot leave the workers behind.
        if proc is not None:
            _reap_runner(proc)
        # Before `_clear_run_artifacts`, which is where the ownership comes from.
        leaked = _remove_own_cgroups(cgroups_before, _own_cgroup_names(_TEST))
        _clear_group_pid_files()
        _clear_run_artifacts()
        # Last, so a cleanup failure cannot skip the artifact cleanup above.
        assert not leaked, (
            "cgroup directories created by this run could not be removed and "
            f"would accumulate one per CI run: {sorted(str(p) for p in leaked)}"
        )


def test_wrapper_diagnostic_is_appended_after_the_test_own_stderr():
    """
    The wrapper's own diagnostics must be *appended* to the per-test stderr file,
    never written over the test's own stderr.

    The fixture writes a ``" <Fatal> "``-bearing line to its own stderr and then
    kills itself with ``SIGSEGV`` - the only shape in this suite that makes bash
    emit a job-control diagnostic *alongside* the test's own stderr, since the
    other tests kill wrapper groups from the outside.

    Runtime note: driving ``SERVER_DIED`` makes the runner attach ``lldb`` to the
    live CI server, paying an on-demand apt install first (up to 210 s once per
    runner subprocess), so a slow run here is expected rather than a hang.
    """
    _clear_group_pid_files()
    _clear_run_artifacts()

    own_line = "FIXTURE-OWN-STDERR-LINE"
    proc = None
    # The spawn is inside the `try` so the cleanup in `finally` also covers a
    # `subprocess.TimeoutExpired` from the run itself, which the runtime note above
    # makes this test's likeliest failure. `Popen` rather than `subprocess.run`
    # because the latter's timeout path kills the runner's pid alone, orphaning the
    # forked workers that own the wrappers.
    try:
        # `SERVER_DIED` makes the runner call `stop_tests()`, whose `killpg` targets
        # its own process group - so give it a session of its own, or it would take
        # pytest down with it.
        proc = subprocess.Popen(
            _runner_argv(_TEST_SIGNAL),
            cwd=str(_REPO_ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
        out_s, err_s = proc.communicate(timeout=300)
        out = out_s + err_s

        collected = sorted(_SUITE_TMP.glob(f"{_TEST_SIGNAL}*.stderr"))
        assert len(collected) == 1, (
            f"expected exactly one runner-managed per-test stderr file matching "
            f"{_SUITE_TMP / (_TEST_SIGNAL + '*.stderr')}, found "
            f"{[str(c) for c in collected]}"
        )
        stderr = collected[0].read_text(errors="replace")

        # Vacuity guard: the wrapper really did emit a job-control diagnostic, so
        # a run where the child was not signalled cannot pass by default.
        bash_marker = "Segmentation fault"
        assert bash_marker in stderr, (
            "the wrapper shell emitted no job-control diagnostic, so this run "
            f"does not exercise the invariant:\n{stderr!r}\n{out}"
        )

        assert own_line in stderr, (
            "the test's own stderr was overwritten by the wrapper's diagnostic - "
            "the parent-side descriptor on this file is not in append mode:\n"
            f"{stderr!r}"
        )
        assert stderr.index(own_line) < stderr.index(bash_marker), (
            "the wrapper's diagnostic precedes the test's own stderr, so it was "
            "not appended at the end of the file:\n"
            f"{stderr!r}"
        )

        # Pin the property at the *reported* level too: the description is
        # assembled from this file, and the ` <Fatal> ` line it must carry is what
        # `process_result_impl` promotes to `SERVER_DIED`.
        assert own_line in out, (
            f"the test's own stderr was not reported as part of its failure:\n{out}"
        )
        assert "[ FAIL ]" in out, f"the test was not reported as FAIL:\n{out}"
        assert "server died" in out, (
            "the ` <Fatal> ` line in the test's own stderr did not reach the "
            f"reader, so it was not promoted to SERVER_DIED:\n{out}"
        )
    finally:
        # Group-aware and idempotent, so a timeout cannot leave the workers behind.
        if proc is not None:
            _reap_runner(proc)
        _clear_group_pid_files()
        _clear_run_artifacts()
