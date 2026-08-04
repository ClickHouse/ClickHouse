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

import errno
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

import pytest

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
    """Clear every record, not just ours.

    The directory is shared with `test_cleanup_test_groups.py`, so this rests on these
    files running serially: `ci/jobs/ci_tests_job.py` builds a plain `pytest ci/tests/`
    with no `-n`, and no conftest enables xdist.
    """
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

    Neither the runner's `pgrep` nor `ps -g` is usable here: both enumerate every process
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


def _spawn_group(lifetime=600, members=3):
    """A live process group in its own session, shaped like a test's own group."""
    group = subprocess.Popen(
        f"sleep {lifetime} & sleep {lifetime} & wait",
        shell=True,
        start_new_session=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    # Poll on the child count, not on `_alive`: the latter walks all of /proc, and the
    # fixture is up as soon as the leader has forked its members.
    children = f"/proc/{group.pid}/task/{group.pid}/children"
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        try:
            with open(children, "rb") as f:
                kids = f.read().split()
        except OSError:
            kids = []
        if len(kids) + 1 >= members:
            break
        time.sleep(0.05)
    assert len(_alive(group.pid)) >= members, "fixture group did not come up"
    return group


def _group_gone(pgid):
    """Cheap poll: does the kernel still know of any process in `pgid`?

    One syscall, versus `_alive`'s walk over every process on the machine.  It counts an
    unreaped zombie leader, so it can only ever be *pessimistic* here - which makes it safe
    to poll on and useless as a verdict.  `_alive` stays the authority.

    Do NOT replace `_alive` with a walk down the leader's `/proc/<pid>/task/<pid>/children`
    instead: when the leader dies first its children are reparented out of that tree, so the
    walk reports an empty group while live members still hold the inherited fd - vacuous in
    exactly the case these tests exist to catch (measured).
    """
    try:
        os.killpg(pgid, 0)
        return False
    except ProcessLookupError:
        return True
    except PermissionError:
        return False


def _wait_dead(pgid, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline and not _group_gone(pgid):
        time.sleep(0.05)
    return _alive(pgid)


def _spawn_leaderless_group(lifetime=600):
    """A group whose LEADER has exited while a member lives on.

    The shape of any ``.sh`` test that backgrounds something and returns: the leader's
    ``returncode`` is then set, which says nothing about the group.  ``_spawn_group`` cannot
    stand in - its leader ``wait``s, so it never reaches this state.
    """
    leader = subprocess.Popen(
        f"sleep {lifetime} & exit 0",
        shell=True,
        start_new_session=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    leader.wait()
    # `wait` has reaped the leader, so `_group_gone` being False here means a real member
    # is up.  Polled with the cheap predicate; `_alive` stays the authority in the caller.
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline and _group_gone(leader.pid):
        time.sleep(0.05)
    return leader


def _make_case(tmp_path, monkeypatch):
    """The ``TestCase`` stand-in ``process_result_impl`` reads its inputs from.

    Non-empty stderr and no reference file, so the FAIL return is taken and the code runs
    past the kill site instead of returning early.
    """
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
        fatal_sanitizer_prefix=f"{stderr_file}-fatal",
        debug_log_retry_substitution=None,
    )
    return case, stderr_file


def _launch_real_test(ct, tmp_path, monkeypatch, lifetime=600, returncode=None, group=None):
    """Drive the REAL ``run_single_test`` so the PGID record is written by the real code.

    Only ``Popen`` is replaced, and it still starts a genuine process group in its own
    session (``start_new_session=True``, as the real launch does) that outlives the abort -
    so what is being observed is the record's real lifetime around a real live group, not a
    hand-written stand-in.  A test that writes the record itself cannot detect the defect:
    the whole bug is *when the real code removes it*.

    ``returncode`` is what the stub reports for the leader; the default ``None`` keeps every
    existing caller on the "still running" branch of ``process_result_impl``.

    Returns the group's pgid.
    """
    group = group if group is not None else _spawn_group(lifetime=lifetime)
    case, _stderr_file = _make_case(tmp_path, monkeypatch)

    def popen_stub(command, **kwargs):
        # Hand the real code the real group: `run_single_test` records `proc.pid` as the
        # PGID, and `start_new_session=True` above makes that exactly true.
        #
        # `wait` is the fixture's own, so the reap after the kill really does reap: a stub
        # that faked it would hide the zombie leader that makes a killed group look alive,
        # and the liveness check would then never drop a record.  The FIRST call, standing
        # in for the timed-out one in `run_single_test`, must not block.
        first_wait = [True]

        def wait_stub(timeout=None):
            if first_wait[0]:
                first_wait[0] = False
                return None
            return group.wait(timeout=timeout)

        return SimpleNamespace(
            pid=group.pid, returncode=returncode, wait=wait_stub
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

        survivors = _wait_dead(pgid)
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
    real_kill = ct["kill_process_group"]

    def spy(pgid, fatal_log, *a, **kw):
        # Snapshot at the exact moment the group is killed: this is when a reaper would
        # need the record, so this is where its absence is fatal.  Then kill for real -
        # removal is conditional on the group being gone, so a spy that only watched
        # would assert the record is dropped while the group is still running.
        killed.append(sorted(p.name for p in _records()))
        return real_kill(pgid, fatal_log, *a, **kw)

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
        assert not _alive(pgid), "precondition: the kill must have emptied the group"
        assert not _records(), (
            "the record must be dropped once the group is gone"
        )
    finally:
        if pgid:
            _kill_group(pgid)
        _clear_records()


def _reason_for_a_timed_out_test(ct, tmp_path, monkeypatch, stderr_text):
    """Run the REAL ``process_result_impl`` over a REAL timed-out ``Popen``.

    No ``wait`` stub and no ``SimpleNamespace`` around the process: the classification
    reads ``proc.returncode``, and only a real ``Popen`` can have it mutated by a ``wait``
    inside the code under test.  A stubbed ``wait`` cannot, so an arm built on one is blind
    to exactly this class of defect.

    Returns ``(result, pgid)``; the caller owns the cleanup.
    """
    case, stderr_file = _make_case(tmp_path, monkeypatch)
    stderr_file.write_text(stderr_text)
    _patch(ct, pgrep=lambda **kw: [])

    proc = subprocess.Popen(
        "sleep 600 & sleep 600 & wait",
        shell=True,
        start_new_session=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    # The state `run_single_test` leaves behind when its `proc.wait(args.timeout)` raised.
    try:
        proc.wait(0.5)
    except subprocess.TimeoutExpired:
        pass
    assert proc.returncode is None, (
        "precondition: a timed-out test's leader must still be unreaped here, otherwise "
        "this arm cannot tell a timeout from an exit"
    )
    ct["write_text_atomic"](
        ct["test_process_group_record"](proc.pid), f"{proc.pid}\n"
    )
    return ct["TestCase"].process_result_impl(case, proc, 1.0), proc.pid


def test_a_timed_out_test_is_still_reported_as_a_timeout(tmp_path, monkeypatch):
    """The kill/reap block must not change what the timeout is reported AS.

    ``process_result_impl`` decides the failure reason from ``proc.returncode`` 140 lines
    below the kill site, and ``Popen.wait`` ASSIGNS that field rather than merely reading it.
    So any ``wait`` between the two silently re-reports every timed-out test as
    ``EXIT_CODE`` - a distinct bucket in ``TEST_FAILURE_PATTERNS``, so every future timeout
    is mis-triaged - and the group liveness this file guards says nothing about it.
    """
    _clear_records()
    ct = runpy.run_path(_CLICKHOUSE_TEST)
    pgid = None
    try:
        result, pgid = _reason_for_a_timed_out_test(
            ct, tmp_path, monkeypatch, "a non-empty stderr, so a FAIL path is taken\n"
        )
        assert result.reason is ct["FailureReason"].TIMEOUT, (
            f"a test that timed out must be reported as a timeout; got {result.reason!r}"
        )
        # And the group is really gone, so the assertion above cannot be satisfied by
        # simply never killing anything.
        assert not _wait_dead(pgid), "the timed-out test's group must still be killed"
    finally:
        if pgid:
            _kill_group(pgid)
        _clear_records()


def test_a_timed_out_test_with_a_fatal_line_does_not_abort_the_run(tmp_path, monkeypatch):
    """The escalation arm: a ` <Fatal> ` line must not turn a timeout into ``SERVER_DIED``.

    ``SERVER_DIED`` calls ``stop_tests`` and raises ``StopTesting``, tearing down the whole
    run - and it is reachable only from the ``returncode != 0`` branch, which a timeout must
    never enter.  On a sanitizer build the client's own fatal log is appended to the very
    stderr that branch searches, so such a line there is ordinary.  One misclassified
    timeout would then abort a 12000-test run: the damage class this file exists to prevent.
    """
    _clear_records()
    ct = runpy.run_path(_CLICKHOUSE_TEST)
    pgid = None
    try:
        result, pgid = _reason_for_a_timed_out_test(
            ct,
            tmp_path,
            monkeypatch,
            "2026.08.04 00:00:00.000000 [ 1 ] {} <Fatal> BaseDaemon: (version 26.8.1)\n",
        )
        assert result.reason is not ct["FailureReason"].SERVER_DIED, (
            "a fatal line in a timed-out test's stderr must not escalate to SERVER_DIED: "
            "that aborts the entire run"
        )
        assert result.reason is ct["FailureReason"].TIMEOUT, (
            f"the reason must still be the timeout itself; got {result.reason!r}"
        )
        assert not _wait_dead(pgid), "the timed-out test's group must still be killed"
    finally:
        if pgid:
            _kill_group(pgid)
        _clear_records()


def test_teardown_mode_skips_all_evidence_gathering(monkeypatch):
    """Pins coupling 2: ``diagnostics=False`` skips both delays AND the process listing.

    The three are independent (``SANITIZED`` -> 60 s, ``CAPTURE_CLIENT_STACKTRACE`` -> 10 s,
    and an unconditional ``pgrep`` between them) and ``cleanup_test_groups`` walks records
    serially, so skipping only some would still cost per group on the one path with no outer
    timeout.  ``pgrep`` shells out to a full ``ps`` with no timeout, so it is the one item
    here that is unbounded rather than merely slow.  The second half matters just as much:
    it stops the fix from silently deleting the evidence the default path exists to gather.
    """
    ct = runpy.run_path(_CLICKHOUSE_TEST)
    slept = []
    pgreps = []

    def pgrep_spy(**kw):
        pgreps.append(kw)
        return []

    # `killpg` is stubbed because this pgid is synthetic; `sleep`/`pgrep` are what is
    # being measured.
    _patch(
        ct,
        SANITIZED=True,  # the asan/msan/tsan shape
        CAPTURE_CLIENT_STACKTRACE=True,
        RELEASE_NON_SANITIZED=False,
        sleep=slept.append,
        pgrep=pgrep_spy,
    )
    monkeypatch.setattr(os, "killpg", lambda *a: None)

    ct["kill_process_group"](12345, None, diagnostics=False)
    # The only remaining wait is the 0.1 s SIGTERM->SIGKILL grace, which is part of
    # killing rather than of gathering evidence. Assert the total, so a re-introduced
    # delay of any size fails here.
    assert sum(slept) < 1, (
        f"teardown mode must skip both the 60 s sanitizer wait and the 10 s "
        f"stacktrace wait; slept {slept}"
    )
    assert not pgreps, (
        f"teardown mode must not shell out to `ps` at all: it is unbounded and this runs "
        f"in the parent, per record, on the path with no outer timeout; saw {pgreps}"
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
    assert pgreps, "the default must still list the processes in the group"


def test_failed_kill_neither_propagates_nor_strands_the_group(tmp_path, monkeypatch):
    """A kill that raises must not leave a live group with no record pointing at it.

    ``process_result_impl`` is the sole normal-path remover, and the worker's NEXT test
    overwrites the same record path (it is named for the worker's pid).  So an exception
    escaping into ``TestCase.run``'s generic handler - which returns UNKNOWN without setting
    ``stop_testing``, so the worker keeps going - re-creates the very defect this file
    guards: a live group nothing can find.
    """
    _clear_records()
    ct = runpy.run_path(_CLICKHOUSE_TEST)

    def boom(pgid, fatal_log, *a, **kw):
        raise OSError(errno.EACCES, "boom")

    _patch(ct, pgrep=lambda **kw: [], kill_process_group=boom)
    pgid = None
    try:
        pgid, case, proc = _launch_real_test(ct, tmp_path, monkeypatch)
        assert len(_alive(pgid)) >= 3, "precondition: the group must still be running"

        # Must not propagate: the caller would otherwise return UNKNOWN and carry on.
        ct["TestCase"].process_result_impl(case, proc, 1.0)

        survivors = _wait_dead(pgid)
        assert not survivors, (
            f"a group whose kill failed must still be killed, not stranded; alive: "
            f"{survivors}"
        )
        assert not _records(), (
            "the record must not outlive the worker's next test, which overwrites it"
        )
    finally:
        if pgid:
            _kill_group(pgid)
        _clear_records()


def test_record_is_kept_while_a_background_member_outlives_the_leader(
    tmp_path, monkeypatch
):
    """``proc.returncode`` is not evidence that the group is dead.

    A ``.sh`` test that backgrounds anything and returns leaves ``returncode`` set while its
    group still runs - and still holds the stdout it inherited, which is the wedge this file
    exists to prevent.  Dropping the record there discards the only pointer to it.
    """
    _clear_records()
    ct = runpy.run_path(_CLICKHOUSE_TEST)
    _patch(ct, pgrep=lambda **kw: [])
    leader = None
    try:
        leader = _spawn_leaderless_group()
        assert _alive(leader.pid), (
            "precondition: the fixture's background member must outlive its leader, "
            "otherwise this test proves nothing"
        )
        pgid, case, proc = _launch_real_test(
            ct, tmp_path, monkeypatch, returncode=0, group=leader
        )

        ct["TestCase"].process_result_impl(case, proc, 1.0)

        assert _alive(pgid), "precondition: the member must still be alive at this point"
        assert _records(), (
            "the record must survive a leader that merely exited, or nothing can ever "
            "find the live group it left behind"
        )

        # And the record is still usable: the parent's reap finds and kills the group.
        ct["cleanup_test_groups"]()
        assert not _wait_dead(pgid), "the kept record must still lead a reaper to the group"
    finally:
        if leader:
            _kill_group(leader.pid)
        _clear_records()


def test_record_is_kept_when_the_kill_is_interrupted(tmp_path, monkeypatch):
    """A ``Terminated`` unwinding out of the kill must not discard a live group's record.

    ``Terminated`` is a ``KeyboardInterrupt``, so it is not an ``Exception`` and passes
    straight through the handler above - but it does run the ``finally``.  The parent's
    ``terminate`` on the abort path delivers exactly that SIGTERM, and it can land inside
    ``kill_process_group``'s SIGTSTP wait.
    """
    _clear_records()
    ct = runpy.run_path(_CLICKHOUSE_TEST)

    real_kill = ct["kill_process_group"]

    def interrupted(pgid, fatal_log, *a, **kw):
        raise ct["Terminated"](signal.SIGTERM)

    _patch(ct, pgrep=lambda **kw: [], kill_process_group=interrupted)
    pgid = None
    try:
        pgid, case, proc = _launch_real_test(ct, tmp_path, monkeypatch)
        assert len(_alive(pgid)) >= 3, "precondition: the group must still be running"

        # It must still propagate: `run_tests_array`'s `except KeyboardInterrupt` is what
        # turns this into an orderly `stop_tests`.
        raised = None
        try:
            ct["TestCase"].process_result_impl(case, proc, 1.0)
        except BaseException as e:
            raised = e
        assert isinstance(raised, ct["Terminated"]), (
            f"Terminated must keep propagating to the worker's handler; got {raised!r}"
        )

        assert _alive(pgid), "precondition: the group must still be alive here"
        assert _records(), (
            "an interrupted kill must leave the record in place; the group is alive and "
            "nothing else can find it"
        )
        # Restore the real kill for the reap: the stub above stands in for a signal that
        # arrives once, not for a broken `kill_process_group`.
        _patch(ct, kill_process_group=real_kill)
        ct["cleanup_test_groups"]()
        assert not _wait_dead(pgid), "the kept record must still lead a reaper to the group"
    finally:
        if pgid:
            _kill_group(pgid)
        _clear_records()


def test_reap_leaves_another_invocations_groups_alone():
    """The reap must be scoped to this run's own workers.

    Records live in the repo-wide ``ci/tmp`` and carry a worker pid and a pgid, no run
    identity.  That was safe while the only caller was ``--cleanup`` at job teardown;
    called from a LIVE parent it can otherwise SIGKILL a concurrent invocation's in-flight
    tests, and concurrent invocations over one tree are a documented condition here (see
    ``test_shared_engine_replacer_discovery.py``).

    The owned record is named by the real helper, so the scoping is asserted against the
    name shape the code under test actually writes.
    """
    _clear_records()
    ct = runpy.run_path(_CLICKHOUSE_TEST)
    _patch(ct, pgrep=lambda **kw: [])
    mine = theirs = None
    try:
        mine = _spawn_group()
        theirs = _spawn_group()
        # A foreign worker's record: same directory, a worker pid this run never started.
        foreign = (
            _GROUP_PID_PATH
            / f"{_GROUP_PID_NAME}.{theirs.pid}-feed1234.{theirs.pid}.{theirs.pid}"
        )
        ct["write_text_atomic"](
            ct["test_process_group_record"](mine.pid), f"{mine.pid}\n"
        )
        ct["write_text_atomic"](foreign, f"{theirs.pid}\n")

        ct["reap_recorded_test_groups"]({os.getpid()})

        assert not _wait_dead(mine.pid), "an owned group must be reaped"
        assert _alive(theirs.pid), (
            "a foreign invocation's live test group must survive this run's abort"
        )
        assert foreign.exists(), "a foreign record must not be consumed either"
    finally:
        for group in (mine, theirs):
            if group:
                _kill_group(group.pid)
        _clear_records()


def test_a_recycled_worker_pid_does_not_widen_the_reap(tmp_path, monkeypatch):
    """The scope key must identify the INVOCATION, not just a pid.

    ``worker_pids`` holds bare numbers and is deliberately never pruned, so a retired
    worker's pid stays in scope for the rest of the run.  ``pid_max`` is finite and the
    records live in the repo-wide ``ci/tmp``, which concurrent invocations over one tree
    share, so the OS recycling that number into a concurrent run's worker would put that
    run's LIVE tests in this run's sweep - the same SIGKILL-a-stranger the scoping was
    adopted to prevent, merely made less likely.
    """
    _clear_records()
    ct = runpy.run_path(_CLICKHOUSE_TEST)
    _patch(ct, pgrep=lambda **kw: [])
    theirs = None
    try:
        theirs = _spawn_group()
        # The recycled-pid shape: a DIFFERENT invocation's token, but a worker pid that IS
        # in this run's `worker_pids`. Matching on the pid alone cannot tell them apart.
        foreign = (
            _GROUP_PID_PATH
            / f"{_GROUP_PID_NAME}.{os.getpid()}-feed1234.{os.getpid()}.{theirs.pid}"
        )
        ct["write_text_atomic"](foreign, f"{theirs.pid}\n")

        ct["reap_recorded_test_groups"]({os.getpid()})

        assert _alive(theirs.pid), (
            "a concurrent invocation's live test group must survive a pid collision with "
            "one of this run's own workers"
        )
        assert foreign.exists(), "and its record must not be consumed either"
    finally:
        if theirs:
            _kill_group(theirs.pid)
        _clear_records()


def test_cleanup_still_consumes_records_written_by_an_older_runner():
    """``--cleanup`` is an orphan sweep and must stay shape-agnostic.

    It runs at job teardown when nothing else is live, and the orphan it exists for can
    have been written by a runner from before the name changed.  Scoping that path on the
    token would silently strand exactly those groups, leaking the wedge this file guards.
    """
    _clear_records()
    ct = runpy.run_path(_CLICKHOUSE_TEST)
    _patch(ct, pgrep=lambda **kw: [])
    legacy = []
    try:
        # Both historical shapes: `<worker>` and `<worker>.<pgid>`.
        for name in (
            f"{_GROUP_PID_NAME}.{os.getpid()}",
            f"{_GROUP_PID_NAME}.{os.getpid()}.0",
        ):
            group = _spawn_group()
            legacy.append(group)
            ct["write_text_atomic"](_GROUP_PID_PATH / name, f"{group.pid}\n")

        # `worker_pids is None`: the orphan sweep, which must match every record.
        ct["cleanup_test_groups"]()

        for group in legacy:
            assert not _wait_dead(group.pid), (
                "an orphan recorded by an older runner must still be reaped by --cleanup"
            )
        assert not _records(), "and its record must be consumed"
    finally:
        for group in legacy:
            _kill_group(group.pid)
        _clear_records()


def _abort_run(ct, sequential, jobs=2, signal_parent=False):
    """Drive the REAL ``do_run_tests`` through an abort, server-free.

    The worker stub creates a live group and records it through the real
    ``write_text_atomic``/``test_process_group_record`` pair, then blocks forever, so the
    parent must ``terminate`` and then ``kill`` it - the exact CI shape, in which no
    Python ``finally`` in the worker can clean up.  Returns the group's pgid.

    ``signal_parent`` drives the other abort shape instead: the worker never sets
    ``stop_testing`` and signals the PARENT, so the run leaves the parallel loop through
    ``Terminated`` rather than through the ``stop_testing`` branch.  The caller then
    expects ``Terminated``, not ``StopTesting``.
    """
    started = multiprocessing.Queue()
    stop_testing = multiprocessing.Event()
    # Set by the parent's own poll loop, so the signalling worker can wait until the
    # parent is really inside it. Without that the SIGTERM can land before
    # `worker_pids.add(process.pid)` runs, and the reap would then have nothing in scope
    # - a spurious failure that has nothing to do with what is being asserted.
    parent_in_loop = multiprocessing.Event()

    def spawn_and_record():
        group = subprocess.Popen(
            "sleep 600 & sleep 600 & wait",
            shell=True,
            start_new_session=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        # Through the real pair, so the record's NAME is produced by the code under test.
        ct["write_text_atomic"](
            ct["test_process_group_record"](group.pid), f"{group.pid}\n"
        )
        started.put(group.pid)
        return group

    def parallel_worker(_payload):
        spawn_and_record()
        if signal_parent:
            # No `stop_testing`: the parent must leave the parallel loop through the
            # signal, which is the path with no reap site on it.
            parent_in_loop.wait(30)
            os.kill(os.getppid(), signal.SIGTERM)
        else:
            stop_testing.set()
        # Ignore SIGTERM so the parent must reach its `p.kill`: SIGKILL runs no Python
        # `finally`, which is exactly why the group is orphaned in CI.  In the signalling
        # shape it also keeps this worker from being what ends the loop, so `Terminated`
        # is the only way out.
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        while True:
            time.sleep(3600)

    def sequential_worker(_payload):
        # The sequential runner IS the parent, so there is no worker to kill; the abort
        # arrives as the exception the real runner raises.
        spawn_and_record()
        raise ct["StopTesting"]("simulated abort out of the sequential runner")

    # The sanitizer shape, so the reap's `diagnostics=False` is OBSERVABLE here: at import
    # both flags are False, and then neither the default nor teardown mode pays a delay, so
    # a probe taken in the default shape cannot tell them apart.
    slept = []

    def sleep_spy(seconds):
        slept.append(seconds)
        # The parent's poll loop is the only 0.1 s sleeper, so this is also where the
        # signalling worker learns the parent has reached the loop (and therefore has
        # already added its pid to `worker_pids`).
        if seconds == 0.1:
            parent_in_loop.set()
        # Still yield, so the parent's own 0.1 s poll loop does not busy-spin, but never
        # actually pay a diagnostic delay: a real 60 s here would defeat the point.
        time.sleep(min(seconds, 0.05))

    _patch(
        ct,
        SANITIZED=True,
        CAPTURE_CLIENT_STACKTRACE=True,
        RELEASE_NON_SANITIZED=False,
        sleep=sleep_spy,
        pgrep=lambda **kw: [],
        get_server_memory_fraction=lambda _args: None,
        run_tests_process=sequential_worker if sequential else parallel_worker,
        run_tests_array=sequential_worker if sequential else parallel_worker,
    )

    names = ["04999_a.sh", "04999_b.sh"]
    suite = SimpleNamespace(
        parallel_tests=[] if sequential else list(names),
        sequential_tests=list(names) if sequential else [],
    )
    args = Namespace(
        jobs=jobs,
        no_self_parallel=True,  # avoids the multiprocessing.Manager
        stop_time=None,
        hung_check=False,
    )
    raised = None
    # `__main__` installs the runner's SIGTERM handler, and `runpy.run_path` does not run
    # that block, so the signalling shape has to install the REAL handler here - otherwise
    # the default disposition kills this process and nothing is asserted.
    old_handler = None
    if signal_parent:
        old_handler = signal.signal(signal.SIGTERM, ct["signal_handler"])
    try:
        ct["do_run_tests"](
            jobs,
            suite,
            args,
            multiprocessing.Value("i", 0),  # exit_code
            [],  # restarted_tests
            stop_testing,
            multiprocessing.Event(),  # runner_process_killed
        )
    except BaseException as e:
        raised = e
    finally:
        if old_handler is not None:
            signal.signal(signal.SIGTERM, old_handler)
        # SIGKILL any worker still up.  In the signalling shape the parent never reaches
        # its own `terminate`/`kill`, and the workers ignore SIGTERM, so multiprocessing's
        # atexit would block forever joining them (observed).  This runs after the reap
        # under test, so it cannot stand in for it.
        for child in multiprocessing.active_children():
            child.kill()
            child.join(timeout=10)
    expected = ct["Terminated"] if signal_parent else ct["StopTesting"]
    assert isinstance(raised, expected), (
        f"the abort must surface as {expected.__name__}; got {raised!r}"
    )
    if signal_parent:
        assert raised.signal == signal.SIGTERM, (
            f"the parent must carry the delivered signal (128+15 semantics at the top "
            f"level); got {raised.signal!r}"
        )

    pgids = []
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        try:
            pgids.append(started.get(timeout=0.5))
        except Exception:
            if pgids:
                break
    assert pgids, "the stub worker never reported its group"
    # Retire the queue's feeder thread before returning.  The sequential shape runs the
    # worker stub IN THIS PROCESS, so its `put` starts a `QueueFeederThread` here that
    # otherwise outlives the call - and a later test in the same session then forks with a
    # second thread alive, which Python warns about as a deadlock risk.
    started.close()
    started.join_thread()
    return pgids, slept


def test_parallel_abort_reaps_the_orphaned_group():
    """The load-bearing wiring: ``do_run_tests``'s parallel abort must reap, in teardown mode.

    Asserted through ``do_run_tests`` itself rather than by calling the reap directly, so that
    deleting the call fails here; and the sanitizer flags are set for the run, so that
    dropping the reap's ``diagnostics=False`` fails here too.  Without those flags both
    mutations look identical, because at import neither delay is enabled at all.
    """
    _clear_records()
    ct = runpy.run_path(_CLICKHOUSE_TEST)
    pgids = []
    try:
        pgids, slept = _abort_run(ct, sequential=False)
        survivors = {pgid: _wait_dead(pgid, timeout=20) for pgid in pgids}
        assert not any(survivors.values()), (
            f"the parallel abort path must reap the groups its killed workers left; "
            f"alive: {survivors}"
        )
        # Only the two diagnostic delays are forbidden; the abort block's own 5 s
        # results-flush wait is unrelated and stays.
        assert not {60, 10} & set(slept), (
            f"the abort-path reap must not pay the evidence-gathering delays: on a "
            f"sanitizer job that is 60 s + 10 s per group, serially, inside the window "
            f"the job is timed against; slept {slept}"
        )
    finally:
        for pgid in pgids:
            _kill_group(pgid)
        _clear_records()


def test_sequential_abort_reaps_the_orphaned_group():
    """Same wiring for the sequential runner, which is invoked outside the parallel block.

    Its orphan cannot come from a SIGKILLed worker (it IS the parent), but the exception
    path above can strand a group there, and without this the sequential runner has no
    reaper at all.
    """
    _clear_records()
    ct = runpy.run_path(_CLICKHOUSE_TEST)
    pgids = []
    try:
        pgids, slept = _abort_run(ct, sequential=True)
        survivors = {pgid: _wait_dead(pgid, timeout=20) for pgid in pgids}
        assert not any(survivors.values()), (
            f"the sequential abort path must reap the group it left; alive: {survivors}"
        )
        assert not {60, 10} & set(slept), (
            f"the sequential reap must not pay the diagnostic delays either; slept {slept}"
        )
    finally:
        for pgid in pgids:
            _kill_group(pgid)
        _clear_records()


def test_a_retained_record_survives_the_workers_next_launch(tmp_path, monkeypatch):
    """A record that is correctly KEPT must still be there after the worker's next test.

    The record used to be named for the WORKER, one path for every test it runs, and it is
    written with ``os.replace``.  So retaining it for a live leaderless group bought only
    seconds: the next ``run_single_test`` in the same worker overwrote it, the pgid was gone
    from disk, and the group was unreachable again while still holding the runner's
    inherited stdout - the wedge carrier this file exists to close.  Keying the record on
    the pgid as well as the worker is what makes the retention mean anything.
    """
    _clear_records()
    ct = runpy.run_path(_CLICKHOUSE_TEST)
    _patch(ct, pgrep=lambda **kw: [])
    first = second = None
    try:
        # Group A: leader exited, member alive, so `process_result_impl` takes the RETAIN
        # branch rather than dropping the record.
        first = _spawn_leaderless_group()
        assert _alive(first.pid), (
            "precondition: the fixture's background member must outlive its leader"
        )
        pgid_a, case_a, proc_a = _launch_real_test(
            ct, tmp_path, monkeypatch, returncode=0, group=first
        )
        ct["TestCase"].process_result_impl(case_a, proc_a, 1.0)
        assert _alive(pgid_a), "precondition: group A must still be alive after its result"
        assert _records(), "precondition: the retain branch must have kept A's record"

        # The same worker's next test, through the real launch, so the overwrite is the
        # real code's.
        second = _spawn_group()
        pgid_b, _case_b, _proc_b = _launch_real_test(
            ct, tmp_path, monkeypatch, group=second
        )
        assert pgid_a != pgid_b, "precondition: the two launches must be distinct groups"

        assert _alive(pgid_a), (
            "precondition: group A must still be alive here, otherwise this proves nothing "
            "about whether its record survived"
        )
        recorded = {int(f.read_text()) for f in _records()}
        assert pgid_a in recorded, (
            f"the live group's record must survive the worker's next launch, or nothing "
            f"can find it; on disk: {sorted(recorded)}, wanted {pgid_a}"
        )

        # And it is still usable, which is the only property that matters.
        ct["cleanup_test_groups"]()
        assert not _wait_dead(pgid_a), (
            "the surviving record must still lead a reaper to the live group"
        )
    finally:
        for group in (first, second):
            if group:
                _kill_group(group.pid)
        _clear_records()


@pytest.mark.parametrize("sequential", [False, True], ids=["parallel", "sequential"])
def test_a_signal_inside_the_reap_does_not_replace_the_abort(monkeypatch, sequential):
    """A SIGTERM landing inside an outer reap must not mask the in-flight abort.

    The reap catches ``except Exception``, and the runner's own ``Terminated`` is a
    ``KeyboardInterrupt``, so it is not caught (measured).  Raised from a ``finally`` it
    REPLACES the ``StopTesting`` propagating through it, and the job side then reads
    ``143`` / "terminated unexpectedly" instead of the exit code it parses - the exact
    misclassification the abort block's own SIGTERM mask exists to prevent.

    Both new reap sites get an arm: they are separate ``finally`` blocks with separate
    masks, so one arm can only ever pin one of them.

    The signal is delivered from inside the reap rather than raced against it: the window is
    a few syscalls wide, so a real SIGTERM cannot be aimed at it otherwise.  It IS a real
    signal though - injecting a ready-made ``Terminated`` instead would assert nothing, since
    a mask can only stop a signal from becoming an exception, not an exception already raised.
    """
    _clear_records()
    ct = runpy.run_path(_CLICKHOUSE_TEST)
    real_cleanup = ct["kill_process_group"].__globals__["cleanup_test_groups"]

    def signal_from_inside_the_reap(*args, **kwargs):
        # Unmasked, `signal_handler` turns this into `Terminated` right here, and that
        # replaces the `StopTesting` unwinding through the enclosing `finally`.
        os.kill(os.getpid(), signal.SIGTERM)
        return real_cleanup(*args, **kwargs)

    # `__main__` installs the runner's handler and `runpy.run_path` does not run that block,
    # so without this the default disposition would kill the test session outright.
    before = signal.signal(signal.SIGTERM, ct["signal_handler"])
    pgids = []
    try:
        monkeypatch.setitem(
            ct["kill_process_group"].__globals__,
            "cleanup_test_groups",
            signal_from_inside_the_reap,
        )
        # `_abort_run` asserts the surfaced exception is `StopTesting`, so an unmasked reap
        # fails inside it with `Terminated`.
        pgids, _slept = _abort_run(ct, sequential=sequential)
        assert signal.getsignal(signal.SIGTERM) is ct["signal_handler"], (
            "the SIGTERM disposition must be restored after the reap, or every later "
            "signal in the run is silently ignored"
        )
    finally:
        signal.signal(signal.SIGTERM, before)
        for pgid in pgids:
            _kill_group(pgid)
        _clear_records()


def test_a_signal_to_the_parent_still_reaps_the_group():
    """A signal delivered to the PARENT must not bypass the reap.

    ``reap_recorded_test_groups`` used to run only inside the ``stop_testing`` branch, and
    the parallel block had no enclosing ``try``.  ``signal_handler`` raises ``Terminated``
    from wherever the parent happens to be, so a SIGTERM anywhere else in the parallel loop
    unwound straight past the reap to the top-level handler - leaving the group alive with
    the job's stdout.  Same path as a worker that simply finishes while holding a retained
    record: no reap site is on it either.
    """
    _clear_records()
    ct = runpy.run_path(_CLICKHOUSE_TEST)
    pgids = []
    try:
        # One worker: a signalled parent cannot join its workers first (that is the point of
        # the path), so with several of them a second worker can record its group AFTER the
        # reap has walked the directory - a race in the fixture, not in the runner.  One
        # group is enough: it is recorded before the signal is sent, so the reap either runs
        # on this path or it does not.
        pgids, _slept = _abort_run(ct, sequential=False, jobs=1, signal_parent=True)
        survivors = {pgid: _wait_dead(pgid, timeout=20) for pgid in pgids}
        assert not any(survivors.values()), (
            f"a parent signalled outside the stop_testing branch must still reap the "
            f"groups its workers recorded; alive: {survivors}"
        )
    finally:
        for pgid in pgids:
            _kill_group(pgid)
        _clear_records()
