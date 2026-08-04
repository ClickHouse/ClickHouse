"""
Guards for the abort path in ``tests/clickhouse-test``: it must not leave a live test
process group behind.

Background
----------
When a run is aborted (hung check, server death, global time limit, ``--max-failures``), the
parent terminates its workers and force-kills the stragglers.  A worker killed that way dies
inside ``kill_process_group``'s own ``sleep``, and SIGKILL runs no Python ``finally``, so the
test process group it was killing survives.  That group inherited the parent's stdout, which
in CI is the write end of ``clickhouse-test | ts | tee``, so the pipeline never sees EOF and
the job burns its whole budget without ever collecting its logs (measured live: the report of
such a run carries ``files: []`` - no server log, no ``fatal.log``, no test rows).

Two couplings keep that from coming back, and both are invisible to any functional test:

1. the PGID record must still exist when the group needs reaping.  ``run_single_test`` used to
   unlink it in the ``finally`` around ``proc.wait``, which runs BEFORE ``process_result_impl``
   kills the group - so at the one moment a reaper is needed, ``cleanup_test_groups`` (and
   therefore ``clickhouse-test --cleanup``) could find nothing;
2. the abort path must actually reap those records, from the parent - the only process still
   guaranteed alive there - and it must do so without the two evidence-gathering delays, which
   are ``60 s`` (sanitizer report window) plus ``10 s`` (client stacktrace) *per group*,
   walked serially, inside the very window the job is being timed against.

The record must still be removed on the normal paths, or a later ``--cleanup`` could signal an
unrelated, PID-recycled process group.

The assertions below drive the real functions (``runpy`` loads the runner, which has no ``.py``
extension and a hyphen in its name) rather than a copy of them.  Each coupling is asserted by
EFFECT - is the group dead, is the record present, was the delay taken - because locating a
statement proves nothing about whether it runs on the path that matters.
"""

import multiprocessing
import os
import runpy
import signal
import subprocess
import time
from argparse import Namespace
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_CLICKHOUSE_TEST = str(_REPO_ROOT / "tests" / "clickhouse-test")

_ct = runpy.run_path(_CLICKHOUSE_TEST)
_GROUP_PID_PATH = _ct["_GROUP_PID_PATH"]
_GROUP_PID_NAME = _ct["_GROUP_PID_NAME"]


def _records():
    return [
        f
        for f in _GROUP_PID_PATH.glob(f"{_GROUP_PID_NAME}.*")
        if not f.name.endswith(".tmp")
    ]


def _clear_records():
    _GROUP_PID_PATH.mkdir(parents=True, exist_ok=True)
    for f in _GROUP_PID_PATH.glob(f"{_GROUP_PID_NAME}.*"):
        f.unlink(missing_ok=True)



def _patch(ct, **names):
    """Rebind names in the namespace the loaded runner's functions actually read.

    ``runpy.run_path`` returns a COPY of the module namespace, so writing into that dict
    changes nothing the code sees - a stub installed that way is silently ignored and every
    assertion relying on it passes vacuously.  ``__globals__`` is the live namespace, and
    every top-level function shares it.
    """
    g = ct["kill_process_group"].__globals__
    assert g is not ct, "expected run_path to return a copy; the patch target changed"
    g.update(names)
    return g


def _kill_group(pgid):
    try:
        os.killpg(pgid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        pass


def _alive(pgid):
    """RUNNABLE PIDs still in `pgid`, read straight from /proc.

    Neither the runner's `pgrep()` nor `ps -g` is usable here: both enumerate every process
    on the machine (measured at ~40 s each on a loaded machine), and this test polls.
    Reading `/proc/<pid>/stat` costs no subprocess per sample.

    Zombies are excluded.  A killed group leader stays listed until its parent - this test -
    reaps it, and counting that as a survivor would fail the fixed code for a state that is
    an artifact of the test's own bookkeeping rather than a live process holding an fd.
    """
    alive = []
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        try:
            with open(f"/proc/{entry}/stat", "rb") as f:
                # The comm field may contain spaces or ')', so split after the LAST ')':
                # then field 0 is state and field 2 is the pgid.
                fields = f.read().rpartition(b")")[2].split()
            if int(fields[2]) == pgid and fields[0] != b"Z":
                alive.append(int(entry))
        except (FileNotFoundError, ProcessLookupError, PermissionError, IndexError):
            continue
    return alive


def _launch_real_test(ct, tmp_path, monkeypatch, lifetime=600):
    """Drive the REAL ``run_single_test`` so the PGID record is written by the real code.

    Only ``Popen`` is replaced, and it still starts a genuine process group in its own
    session (``start_new_session=True``, as the real launch does) that outlives the abort -
    so what is being observed is the record's real lifetime around a real live group, not a
    hand-written stand-in.  A test that writes the record itself cannot detect the defect:
    the whole bug is *when the real code removes it*.

    Returns the group's pgid.
    """
    group = subprocess.Popen(
        f"sleep {lifetime} & sleep {lifetime} & wait",
        shell=True,
        start_new_session=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline and len(_alive(group.pid)) < 3:
        time.sleep(0.05)
    assert len(_alive(group.pid)) >= 3, "fixture group did not come up"

    monkeypatch.setenv("CLICKHOUSE_DATABASE", "test_abort_path_reaps_test_groups")
    monkeypatch.setenv("CLICKHOUSE_TMP", str(tmp_path))
    for mutated in (
        "CLICKHOUSE_CLIENT_OPT",
        "CLICKHOUSE_LOG_COMMENT",
        "TSAN_OPTIONS",
        "ASAN_OPTIONS",
        "MSAN_OPTIONS",
        "UBSAN_OPTIONS",
    ):
        monkeypatch.setenv(mutated, "")

    testcase_args = Namespace(
        testcase_client="clickhouse-client",
        testcase_start_time=datetime.now(),
        testcase_database="test_abort_path_reaps_test_groups",
        testcase_basename="04999_group_record",
        debug_log_file=str(tmp_path / "absent.debuglog"),
        bash_tracing_file=str(tmp_path / "absent.bashlog"),
        trace=False,
        secure=False,
        memory_limit=0,
        timeout=1,
        hide_db_name=False,
        replicated_database=False,
        shared_catalog=False,
        cloud=False,
        client="clickhouse-client",
        database="test_abort_path_reaps_test_groups",
        stop=False,
        record=False,
        flaky_check=False,
        test_runs=1,
        unified=3,
        server_logs_level="warning",
    )
    stdout_file = tmp_path / "04999_group_record.stdout"
    stdout_file.write_text("")
    stderr_file = tmp_path / "04999_group_record.stderr"
    # Non-empty and no reference file: steers process_result_impl onto a FAIL return, so it
    # runs past the kill site instead of returning early.
    stderr_file.write_text("a non-empty stderr, so a FAIL path is taken\n")
    case = SimpleNamespace(
        testcase_args=testcase_args,
        args=testcase_args,
        tags=set(),
        ext=".sh",
        case_file=str(tmp_path / "04999_group_record.sh"),
        stdout_file=str(stdout_file),
        stderr_file=str(stderr_file),
        name="04999_group_record.sh",
        reference_file=None,
        show_whitespaces_in_diff=False,
        suite=None,
    )

    def popen_stub(command, **kwargs):
        # Hand the real code the real group: `run_single_test` records `proc.pid` as the
        # PGID, and `start_new_session=True` above makes that exactly true.
        return SimpleNamespace(
            pid=group.pid, returncode=None, wait=lambda _timeout: None
        )

    monkeypatch.setitem(ct["kill_process_group"].__globals__, "Popen", popen_stub)
    proc, _ = ct["TestCase"].run_single_test(case, "warning", "")
    assert proc is not None, "the launch was never reached"
    return group.pid, case, proc


def test_abort_path_leaves_no_live_test_group(tmp_path, monkeypatch):
    """The real oracle: the abort-path reap kills the group a dead worker left behind.

    On master this FAILS on behaviour, not on a missing API: `run_single_test` has already
    unlinked the record by the time the reap runs, so `cleanup_test_groups` finds nothing and
    the group is still alive.  Called with no keyword arguments precisely so that it exercises
    the same entry point on both master and the fix.
    """
    _clear_records()
    ct = runpy.run_path(_CLICKHOUSE_TEST)
    # `pgrep` feeds only `kill_process_group`'s diagnostic prints; the killing itself is
    # `os.killpg`, left real. Stubbed because the runner's `pgrep` lists every process on
    # the box, which costs minutes on a loaded machine and measures nothing here.
    _patch(ct, pgrep=lambda **kw: [])
    pgid = None
    try:
        pgid, _case, _proc = _launch_real_test(ct, tmp_path, monkeypatch)
        # The worker is now dead (SIGKILLed inside kill_process_group, which runs no
        # `finally`), so nobody called `process_result_impl`. The group is still running.
        assert len(_alive(pgid)) >= 3, (
            "precondition: the group must still be alive here, otherwise this test cannot "
            "distinguish a working reap from a group that exited on its own"
        )

        # What the parent does at the end of the abort block.
        ct["cleanup_test_groups"]()

        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and _alive(pgid):
            time.sleep(0.1)
        survivors = _alive(pgid)
        assert not survivors, (
            "the abort path must leave no live test process group; still alive: "
            f"{survivors}"
        )
        assert not _records(), "the reap must also drop the records it consumed"
    finally:
        if pgid:
            _kill_group(pgid)
        _clear_records()


def test_record_survives_the_launch_and_is_dropped_after_the_kill(tmp_path, monkeypatch):
    """Pins coupling 1, in both directions, over the REAL ``run_single_test``.

    The record must still be there when ``run_single_test`` returns (master fails here: its
    ``finally`` has already unlinked it, while the group is still running and nothing has
    killed it yet), and it must be gone once ``process_result_impl`` has killed the group -
    the leak that a naive "just stop unlinking it" fix would introduce.
    """
    _clear_records()
    ct = runpy.run_path(_CLICKHOUSE_TEST)
    killed = []

    def spy(pgid, fatal_log, *a, **kw):
        # Snapshot at the exact moment the group is killed: this is when a reaper would
        # need the record, so this is where its absence is fatal.
        killed.append(sorted(p.name for p in _records()))

    _patch(ct, pgrep=lambda **kw: [], kill_process_group=spy)
    pgid = None
    try:
        pgid, case, proc = _launch_real_test(ct, tmp_path, monkeypatch)

        # DIRECTION 1: the group is still alive and unkilled, so the record must exist.
        assert len(_alive(pgid)) >= 3, "precondition: the group must still be running"
        assert _records(), (
            "the PGID record must outlive run_single_test while the group is still "
            "running, otherwise cleanup_test_groups can never find the group"
        )

        ct["TestCase"].process_result_impl(case, proc, 1.0)

        # DIRECTION 2: the kill site saw the record...
        assert killed, "kill_process_group was not reached"
        assert killed[0], (
            f"the record must still exist when the group is killed; saw {killed[0]}"
        )
        # ...and nothing is left behind afterwards. A stale record would let a later
        # `--cleanup` signal an unrelated, PID-recycled group.
        assert not _records(), (
            "the record must be dropped once the group has been dealt with"
        )
    finally:
        if pgid:
            _kill_group(pgid)
        _clear_records()


def test_teardown_mode_skips_both_delays():
    """Pins coupling 2: ``diagnostics=False`` skips BOTH delays, and the default keeps them.

    The two are independent (``SANITIZED`` -> 60 s, ``CAPTURE_CLIENT_STACKTRACE`` -> 10 s) and
    ``cleanup_test_groups`` walks records serially, so skipping only one would still add a
    minute per group on every sanitizer job - inside the window the job is timed against.
    The second half matters just as much: it stops the fix from silently deleting the
    sanitizer report window that the delay exists to provide.
    """
    ct = runpy.run_path(_CLICKHOUSE_TEST)
    slept = []
    # `pgrep` is stubbed because the real one shells out to a full `ps` listing; `killpg`
    # because this pgid is synthetic. Both are incidental to what is being measured.
    _patch(
        ct,
        SANITIZED=True,  # the asan/msan/tsan shape
        CAPTURE_CLIENT_STACKTRACE=True,
        RELEASE_NON_SANITIZED=False,
        sleep=slept.append,
        pgrep=lambda **kw: [],
    )
    real_killpg = os.killpg
    os.killpg = lambda *a: None
    try:
        ct["kill_process_group"](12345, None, diagnostics=False)
        # The only remaining wait is the 0.1 s SIGTERM->SIGKILL grace, which is part of
        # killing rather than of gathering evidence. Assert the total, so a re-introduced
        # delay of any size fails here.
        assert sum(slept) < 1, (
            f"teardown mode must skip both the 60 s sanitizer wait and the 10 s "
            f"stacktrace wait; slept {slept}"
        )

        slept.clear()
        ct["kill_process_group"](12345, None)
        assert 60 in slept, (
            f"the default must keep the sanitizer report window, or the fix silently "
            f"deletes the evidence path it exists for; slept {slept}"
        )
        assert 10 in slept, (
            f"the default must keep the client-stacktrace wait; slept {slept}"
        )
    finally:
        os.killpg = real_killpg
