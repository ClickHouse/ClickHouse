"""
Regression tests for bounded post-run archiving (`Utils.compress_files_gz`) and the
`Shell` watchdog (`Shell._check_timeout`) that enforces its bound.

Archiving sits between collecting the results and the only result dump, so the bound
must cost the tarball and nothing else. Four properties carry that, and a bounding-only
change gets each of them wrong:

* it must not raise. `Shell.check(..., strict=True, timeout=N)` raises on the timeout,
  which still skips the result dump. Nor may a missing input or a failed publish raise.
* it must not publish a partial archive. `tar` killed by SIGTERM leaves a large truncated
  archive that the upload's existence check accepts, so the archive is built under a
  temporary name and renamed in only once it is known good.
* that temporary name must be unique per writer. Both callers writing the job's
  `logs.tar.gz` stage beside it, and a shared name lets the loser keep writing into the
  archive the winner already published.
* it must judge the archive, not `tar`'s exit code. `tar` reports failure for an input it
  cannot stat while archiving everything else and finishing the stream, so the exit code
  alone cannot tell a complete archive from a truncated one; `tar -tzf` can, because it
  walks the member stream. `tar -cf - | gzip` could not even report the failure - without
  `pipefail` only gzip's status was visible - which is why the direct `-czf` form is used.

The watchdog is a prerequisite, not separate scope: bounding is what starts passing
`timeout=` to short-lived commands inside long-lived processes, and `_check_timeout` slept
the timeout out unconditionally before killpg'ing the group, so a command that finished
immediately left a live watchdog aimed at a possibly PID-recycled group. It now waits on
an `Event` set when the attempt's child is reaped, backstopped by process-GROUP liveness
tests: a background descendant holding the output pipes keeps `finished` unset while the
leader is already reaped, so a leader-only test would return without enforcing anything.
All three liveness tests in `_check_timeout` - before the SIGTERM, in the grace loop and
at the SIGKILL gate - ask about the group for that reason. The `Event` is per attempt,
since one hoisted out of the retry loop is already set when attempt 2 begins waiting.

`TeePopen._check_timeout` had the same blind sleep and was fixed by cfb2ac8ff0c244.
"""

import builtins
import contextlib
import errno
import io
import os
import re
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.praktika.utils import Shell, Utils

# A tree that cannot be archived within ARCHIVE_TIMEOUT: incompressible random data, so
# gzip cannot shortcut it.
SLOW_TREE_FILES = 400
SLOW_TREE_FILE_BYTES = 200 * 1024
ARCHIVE_TIMEOUT = 1
# Generous ceiling for a bounded call: far above the ~1s bound plus the watchdog's
# graceful-termination wait, far below the natural duration of the archive.
MAX_ELAPSED = 60


def _slow_tree(tmp_path):
    """A directory whose archive takes far longer than ARCHIVE_TIMEOUT."""
    src = tmp_path / "src"
    src.mkdir()
    blob = os.urandom(SLOW_TREE_FILE_BYTES)
    for i in range(SLOW_TREE_FILES):
        (src / f"f{i}.bin").write_bytes(blob + i.to_bytes(8, "little"))
    return src


# --- (A) bounded, non-raising, no partial archive ------------------------------------


def test_archive_overrun_is_bounded_and_not_fatal(tmp_path):
    """An overrunning archive returns a failure signal instead of raising or hanging."""
    src = _slow_tree(tmp_path)
    archive = tmp_path / "logs.tar.gz"

    start = time.monotonic()
    result = Utils.compress_files_gz([str(src)], str(archive), timeout=ARCHIVE_TIMEOUT)
    elapsed = time.monotonic() - start

    assert result is None, (
        f"an archive that overran its timeout reported success ({result!r}); the caller "
        "would attach a file it cannot trust"
    )
    assert elapsed < MAX_ELAPSED, (
        f"archiving took {elapsed:.1f}s against a {ARCHIVE_TIMEOUT}s bound "
        f"(>= {MAX_ELAPSED}s) -- it was not bounded"
    )


def test_archive_overrun_leaves_no_partial_archive(tmp_path):
    """A timed-out archive must not leave anything at the destination.

    Absence is asserted rather than validity: nothing should be there to test, so
    `gzip -t` is not a usable oracle here.
    """
    src = _slow_tree(tmp_path)
    archive = tmp_path / "logs.tar.gz"

    Utils.compress_files_gz([str(src)], str(archive), timeout=ARCHIVE_TIMEOUT)

    assert not archive.exists(), (
        f"a timed-out archive left {archive.stat().st_size} bytes at the destination; "
        "the upload would publish the truncated file as the logs"
    )
    leftovers = [p.name for p in tmp_path.iterdir() if p.name.startswith("logs.tar.gz")]
    assert leftovers == [], f"temporary archive files were left behind: {leftovers}"


def test_the_archive_command_does_not_pipe_tar_into_gzip():
    """The archiving command must not hide tar's exit status behind a pipe.

    Asserted on the command as well as behaviourally: the behavioural arms can only show
    that the current form reports a failure, not that a refactor back to a pipe would
    stop reporting it.
    """
    calls = []

    def record(command, **kwargs):
        calls.append(command)
        return 0

    original_run = Shell.run
    Shell.run = staticmethod(record)
    try:
        Utils.compress_files_gz([str(Path(__file__))], "/dev/null/unused")
    except Exception:
        pass
    finally:
        Shell.run = original_run

    assert calls, "compress_files_gz did not invoke Shell.run"
    for command in calls:
        assert "|" not in command, (
            f"the archiving command pipes tar's output: {command!r}. Without pipefail "
            "only gzip's status is visible, so a failing tar reports success"
        )
        # Asserted as a token, not a substring, so an added tar flag does not break it.
        tokens = command.split()
        assert tokens and tokens[0] == "tar" and "-czf" in tokens, (
            f"unexpected archiving command {command!r}: expected a direct `tar ... -czf`, "
            "which reports tar's own exit status"
        )


def test_archive_reports_tar_failure_and_publishes_nothing(tmp_path):
    """A tar that could not finish writing must be reported and publish nothing.

    The counterpart of the arm above: same non-zero exit, unusable archive, so the two
    together show the outcome follows the archive's state, not the exit code. The write
    limit is the trigger because an unreadable input would be read by root regardless.
    """
    src = _slow_tree(tmp_path)
    archive = tmp_path / "logs.tar.gz"

    original_run = Shell.run

    def run_with_a_write_limit(command, **kwargs):
        # `ulimit -f` is in 512-byte blocks: 100 blocks = 50KB, far below the archive.
        return original_run(f"ulimit -f 100; {command}", **kwargs)

    Shell.run = staticmethod(run_with_a_write_limit)
    try:
        result = Utils.compress_files_gz([str(src)], str(archive), timeout=600)
    finally:
        Shell.run = original_run

    assert result is None, (
        f"a tar that could not finish writing reported success ({result!r}); the "
        "truncated archive would be published as the logs"
    )
    assert (
        not archive.exists()
    ), "a failing tar left an archive at the destination instead of being discarded"
    leftovers = [p.name for p in tmp_path.iterdir() if p.name.startswith("logs.tar.gz")]
    assert leftovers == [], f"temporary archive files were left behind: {leftovers}"


def test_archive_failure_leaves_an_existing_archive_intact(tmp_path):
    """A failed attempt must not destroy an archive already at the destination.

    Both archiving paths write the same `ci/tmp/logs.tar.gz`, and the hook can run after
    the normal path already published one, so a second writer that overran must not be
    able to replace a complete archive with nothing.
    """
    existing = tmp_path / "logs.tar.gz"
    existing.write_bytes(b"previous complete archive")
    src = _slow_tree(tmp_path)

    result = Utils.compress_files_gz([str(src)], str(existing), timeout=ARCHIVE_TIMEOUT)

    assert result is None, "precondition: this archiving attempt must fail"
    assert existing.read_bytes() == b"previous complete archive", (
        "a failed archiving attempt damaged the archive already at the destination; "
        "the second writer would destroy the first writer's complete archive"
    )


def test_archive_publishes_a_member_appended_to_while_read(tmp_path):
    """`tar` exit 1 is benign and its archive must still be published.

    Every archive this helper builds contains a file the job is still writing, so
    treating 1 as failure would discard a complete archive on nearly every failing shard.
    The member is pre-seeded large so tar's read reliably overlaps the writer.
    """
    src = tmp_path / "src"
    src.mkdir()
    (src / "static.txt").write_text("static\n")
    live = src / "job.log"
    live.write_bytes(os.urandom(2 * 1024 * 1024))
    archive = tmp_path / "logs.tar.gz"

    stop = threading.Event()

    def append():
        with open(live, "a") as f:
            while not stop.is_set():
                f.write("appended\n")

    writer = threading.Thread(target=append, daemon=True)
    writer.start()
    try:
        result = Utils.compress_files_gz([str(src)], str(archive), timeout=600)
    finally:
        stop.set()
        writer.join(timeout=10)

    assert result == str(archive), (
        f"an archive whose member was appended to reported failure ({result!r}); tar's "
        "exit 1 is a warning, and rejecting it discards a complete archive"
    )
    assert archive.is_file(), "the archive was not published at the destination"
    listing = subprocess.run(
        ["tar", "-tzf", str(archive)], capture_output=True, text=True, check=True
    ).stdout
    assert (
        "static.txt" in listing and "job.log" in listing
    ), f"the published archive is missing members: {listing!r}"


def test_archive_publishes_a_complete_archive_despite_a_failing_tar_exit(tmp_path):
    """A complete archive must be published even when `tar` exits non-zero.

    An input vanishing between the existence filter and tar is reachable (the hook's
    `_instances*` glob stays unexpanded when no cluster started), and needs no
    privileges, so this arm also covers the failure direction on the root CI runner.
    """
    src = tmp_path / "src"
    src.mkdir()
    (src / "kept.txt").write_text("payload\n")
    doomed = tmp_path / "doomed"
    doomed.mkdir()
    (doomed / "x.txt").write_text("about to vanish\n")
    archive = tmp_path / "logs.tar.gz"

    original_run = Shell.run
    removed = []

    def remove_then_run(command, **kwargs):
        if not removed:
            removed.append(True)
            subprocess.run(["rm", "-rf", str(doomed)], check=True)
        return original_run(command, **kwargs)

    Shell.run = staticmethod(remove_then_run)
    try:
        result = Utils.compress_files_gz(
            [str(src), str(doomed)], str(archive), timeout=600
        )
    finally:
        Shell.run = original_run

    assert removed, "the input was never removed: this arm proves nothing"
    assert result == str(archive), (
        f"a complete archive was discarded over tar's exit code ({result!r}); the "
        "surviving inputs were archived and the stream finished"
    )
    listing = subprocess.run(
        ["tar", "-tzf", str(archive)], capture_output=True, text=True, check=True
    ).stdout
    assert (
        "kept.txt" in listing
    ), f"the surviving input is not in the archive: {listing!r}"
    assert "x.txt" not in listing, (
        f"the vanished input appears in the archive: {listing!r}; the trigger did not "
        "work as intended"
    )


def test_archive_rc_one_from_an_internal_failure_is_not_published_unread(tmp_path):
    """An rc of 1 must be judged by reading the archive back, not accepted on sight.

    `Shell.run` reports an internal failure of its own as 1, the same value `tar` uses
    for "some files differ", and such a failure can leave the archive cut short. A
    truncated staging archive must not be renamed into place: the upload only checks
    that the file exists.
    """
    src = tmp_path / "src"
    src.mkdir()
    (src / "payload.bin").write_bytes(b"z" * (4 * 1024 * 1024))
    archive = tmp_path / "logs.tar.gz"

    original_run = Shell.run
    staged = []

    def truncate_then_report_one(command, **kwargs):
        if command.startswith("tar --warning="):
            rc = original_run(command, **kwargs)
            assert rc == 0, f"the staging tar itself failed ({rc}): arm proves nothing"
            match = re.search(r"-czf (\S+)", command)
            assert match, f"could not find the staging path in {command!r}"
            path = Path(match.group(1))
            assert path.exists(), f"the staging archive is missing at {path}"
            # What a leader killed mid-write leaves behind: a real prefix of a real
            # archive, which the upload's existence check would accept as the logs.
            path.write_bytes(path.read_bytes()[: max(1, path.stat().st_size // 2)])
            staged.append(path)
            return 1
        return original_run(command, **kwargs)

    Shell.run = staticmethod(truncate_then_report_one)
    try:
        result = Utils.compress_files_gz([str(src)], str(archive), timeout=600)
    finally:
        Shell.run = original_run

    assert staged, "the staging tar was never intercepted: this arm proves nothing"
    assert result is None, (
        f"a truncated archive was published on rc 1 ({result!r}); rc 1 can be an "
        "internal Shell.run failure with tar still writing"
    )
    assert not archive.exists(), f"the truncated archive was renamed into {archive}"
    assert not staged[0].exists(), f"the staging archive was left behind at {staged[0]}"


def test_archive_skips_missing_inputs_without_raising(tmp_path):
    """A declared-but-absent input must be dropped, not raise.

    Reachable: `Result.from_pytest_run` registers the pytest log and report before
    running pytest, and a conftest import error leaves both absent.
    """
    present = tmp_path / "present.txt"
    present.write_text("payload\n")
    absent = tmp_path / "never_created.log"
    archive = tmp_path / "logs.tar.gz"

    result = Utils.compress_files_gz(
        [str(present), str(absent)], str(archive), timeout=600
    )

    assert result == str(archive), (
        f"a missing declared input was not tolerated ({result!r}); the caller's result "
        "upload would be skipped by the exception"
    )
    listing = subprocess.run(
        ["tar", "-tzf", str(archive)], capture_output=True, text=True, check=True
    ).stdout
    assert "present.txt" in listing, f"the existing input was not archived: {listing!r}"


def test_an_internal_failure_kills_the_whole_group_it_started(tmp_path):
    """An exception inside `Shell.run` must not leave the command's group running.

    The exception handler is the one exit that does not go through the watchdog: the
    `finally` cancels it, so whatever the handler leaves alive is never signalled by
    anyone. Killing only the leader therefore lets a background descendant outlive the
    attempt and keep writing, which is what the bound exists to stop.

    Driven by a 16 MiB stdin write into a command that does not read stdin, which raises
    after `Popen`. Observed through a marker the descendant writes only if it survived:
    the leader `exec`s away immediately, so its own liveness says nothing.
    """
    marker = tmp_path / "descendant-survived"
    sleep_s = 6
    fragment = f"(sleep {sleep_s}; touch {marker}) & exec true"

    # This fragment really does outlive an ordinary Shell.run, so a missing marker below
    # is the kill and not the fragment failing to run.
    control = tmp_path / "control-survived"
    Shell.run(f"(sleep {sleep_s}; touch {control}) & exec true", verbose=False)
    time.sleep(sleep_s + 3)
    assert control.exists(), "the control descendant never ran: this arm proves nothing"

    started = time.time()
    rc = Shell.run(
        fragment, stdin_str="x" * (16 * 1024 * 1024), timeout=2, verbose=False
    )
    elapsed = time.time() - started

    assert rc != 0, "the stdin write did not fail, so no exception path was taken"
    assert elapsed < sleep_s, (
        f"Shell.run took {elapsed:.1f}s, as long as the descendant lives: it returned "
        "normally rather than through the exception path this arm is about"
    )
    time.sleep(sleep_s + 4)
    assert not marker.exists(), (
        "the descendant outlived the failed attempt: the exception path killed only the "
        "leader, and the watchdog it cancels can no longer reach the group"
    )


def test_a_retry_that_never_started_signals_nothing(monkeypatch):
    """An attempt that raises before its own `Popen` must signal no group at all.

    Killing the group rather than the leader makes a stale handle dangerous: the pid of
    a reaped attempt can have been recycled as an unrelated process group, and the
    signal is a SIGKILL. Driven by letting attempt one exit non-zero and breaking the
    log-file open on attempt two, so the second attempt raises with no child of its own.
    """
    signalled = []
    real_killpg = os.killpg
    real_open = open
    opens = {"n": 0}

    def spy_killpg(pgid, sig):
        signalled.append((pgid, sig))
        return real_killpg(pgid, sig)

    def failing_second_open(path, *args, **kwargs):
        mode = kwargs.get("mode", args[0] if args else "r")
        if path == "/dev/null" and mode == "w":
            opens["n"] += 1
            if opens["n"] >= 2:
                raise OSError(errno.EACCES, "simulated log open failure")
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr(os, "killpg", spy_killpg)
    monkeypatch.setattr(builtins, "open", failing_second_open)
    rc = Shell.run("exit 7", retries=2, verbose=False, timeout=30)
    monkeypatch.undo()

    assert opens["n"] >= 2, (
        f"the second attempt never opened the log ({opens['n']} opens), so it never "
        "raised and this arm proves nothing"
    )
    assert rc != 0, "the command unexpectedly succeeded"
    assert signalled == [], (
        f"a group was signalled by an attempt that created no child: {signalled}; that "
        "pid belongs to an already-reaped attempt and may have been recycled"
    )


def test_archive_staging_failure_is_reported_not_raised(tmp_path, monkeypatch):
    """A failure while writing the tar manifest must return None, not raise.

    The manifest is written before tar runs, so a full or read-only temporary filesystem
    throws here - and an exception loses the caller's result upload just as an unbounded
    hang did, which is the outcome the whole bound exists to prevent.
    """
    src = tmp_path / "a.log"
    src.write_text("payload\n")
    archive = tmp_path / "logs.tar.gz"

    def no_space(*args, **kwargs):
        raise OSError(errno.ENOSPC, "No space left on device")

    monkeypatch.setattr(tempfile, "NamedTemporaryFile", no_space)

    result = Utils.compress_files_gz([str(src)], str(archive), timeout=600)

    assert (
        result is None
    ), f"expected None when the manifest cannot be written: {result!r}"
    assert not archive.exists(), "a failed staging step still published an archive"
    leftovers = sorted(p.name for p in tmp_path.iterdir() if p.name.endswith(".tmp"))
    assert leftovers == [], f"staging files were left behind: {leftovers}"


def test_archive_of_nothing_returns_none(tmp_path):
    """When no input exists at all there is nothing to publish, and still no raise."""
    archive = tmp_path / "logs.tar.gz"

    result = Utils.compress_files_gz(
        [str(tmp_path / "a.log"), str(tmp_path / "b.log")], str(archive), timeout=600
    )

    assert result is None, f"expected None for an empty input set, got {result!r}"
    leftovers = [p.name for p in tmp_path.iterdir()]
    assert leftovers == [], f"files were left behind: {leftovers}"


def test_archive_publish_failure_is_reported_not_raised(tmp_path):
    """A failed rename must return None, not raise.

    Publication is the last step, so it can still throw after tar succeeded - and an
    exception here loses the caller's result upload just as an unbounded hang did. A
    non-empty directory at the destination is the reachable trigger used to prove it.
    """
    src = tmp_path / "src"
    src.mkdir()
    (src / "a.txt").write_text("payload\n")
    blocked = tmp_path / "logs.tar.gz"
    blocked.mkdir()
    (blocked / "occupant").write_text("in the way\n")

    result = Utils.compress_files_gz([str(src)], str(blocked), timeout=600)

    assert result is None, f"a failed publish reported success ({result!r})"
    leftovers = [p.name for p in tmp_path.iterdir() if p.name.endswith(".tmp")]
    assert leftovers == [], f"the temporary archive was left behind: {leftovers}"


def test_archive_cleanup_failure_is_reported_not_raised(tmp_path):
    """A cleanup that cannot remove the temporary archive must not raise.

    `unlink(missing_ok=True)` suppresses only `FileNotFoundError`, and an exception on
    the failure path loses the caller's result upload just as an unbounded hang did.
    """
    src = tmp_path / "src"
    src.mkdir()
    (src / "a.txt").write_text("payload\n")
    archive = tmp_path / "logs.tar.gz"

    # The staged path occupied by a directory, so tar cannot write it and the cleanup
    # that follows cannot unlink it either. Created from the name the call itself
    # chooses: a pre-seeded name would not be the one a per-invocation name picks, and
    # the arm would pass over a run that never reached the cleanup at all.
    original_run = Shell.run

    def occupy_then_run(command, **kwargs):
        staged = re.search(r"-czf (\S+)", command)
        if staged:
            occupied = Path(staged.group(1))
            occupied.mkdir()
            (occupied / "occupant").write_text("in the way\n")
        return original_run(command, **kwargs)

    Shell.run = staticmethod(occupy_then_run)
    try:
        result = Utils.compress_files_gz([str(src)], str(archive), timeout=600)
    finally:
        Shell.run = original_run

    assert result is None, f"a failed archive reported success ({result!r})"
    assert not archive.exists(), "a failed archive was published at the destination"


def test_the_temporary_archive_name_is_unique_per_writer(tmp_path):
    """Two writers must not stage into the same temporary file.

    A shared name lets one writer's rename publish the file while the other's descriptor
    follows the inode into the PUBLISHED archive. Asserted by observing that two calls
    for the same destination choose different names, which a pid-derived name does not
    satisfy: the job script archives this path from inside Docker's own pid namespace
    while sharing the directory with the host, so both writers can be pid 1.
    """
    staged = []

    def record(command, **kwargs):
        staged.append(command)
        return 0

    destination = str(tmp_path / "logs.tar.gz")
    original_run = Shell.run
    Shell.run = staticmethod(record)
    try:
        for _ in range(2):
            Utils.compress_files_gz([str(Path(__file__))], destination)
    finally:
        Shell.run = original_run

    assert len(staged) == 2, f"compress_files_gz did not stage twice: {staged!r}"
    names = [re.search(r"-czf (\S+)", command).group(1) for command in staged]
    for name in names:
        assert name != destination, (
            f"the archive is written straight to its destination ({name!r}); a tar cut "
            "short by the timeout would be published as the logs"
        )
        assert name.startswith(destination), (
            f"the staging file {name!r} is not beside its destination, so the publishing "
            "rename is no longer atomic"
        )
    assert names[0] != names[1], (
        f"both writers stage into the same file ({names[0]!r}). The loser would keep "
        "writing into the archive the winner published"
    )


def test_archive_success_publishes_a_complete_archive(tmp_path):
    """The normal path must still produce a usable archive at the destination."""
    src = tmp_path / "src"
    src.mkdir()
    (src / "a.txt").write_text("first\n")
    (src / "b.txt").write_text("second\n")
    archive = tmp_path / "logs.tar.gz"

    result = Utils.compress_files_gz([str(src)], str(archive), timeout=600)

    assert result == str(archive), f"unexpected return value {result!r}"
    assert archive.is_file(), "no archive was produced on the success path"
    listing = subprocess.run(
        ["tar", "-tzf", str(archive)], capture_output=True, text=True, check=True
    ).stdout
    assert (
        "a.txt" in listing and "b.txt" in listing
    ), f"the published archive is missing its contents: {listing!r}"
    # Generalized rather than pinning one literal name: the temporary is unique per
    # writer, so the property is that nothing but the archive survives.
    leftovers = [
        p.name
        for p in tmp_path.iterdir()
        if p.name.startswith("logs.tar.gz") and p.name != "logs.tar.gz"
    ]
    assert (
        leftovers == []
    ), f"temporary archive files survived a successful rename: {leftovers}"


def test_archive_without_timeout_still_works(tmp_path):
    """`timeout` is optional: existing callers must keep working unchanged."""
    src = tmp_path / "src"
    src.mkdir()
    (src / "a.txt").write_text("payload\n")
    archive = tmp_path / "logs.tar.gz"

    assert Utils.compress_files_gz([str(src)], str(archive)) == str(archive)
    assert archive.is_file()


# --- (B) the watchdog that enforces the bound ----------------------------------------

WATCHDOG_TIMEOUT = 2
# Child sleeps far longer than the watchdog timeout, so a watchdog that never signals is
# unambiguous.
WATCHDOG_CHILD_SLEEP = 60


def _live_watchdog_threads():
    return [t.name for t in threading.enumerate() if "_check_timeout" in t.name]


def _wait_for_watchdogs_to_exit(before, deadline_sec=10):
    """Live watchdog threads beyond `before`, after giving cancelled ones time to exit.

    A cancelled watchdog returns as soon as it is scheduled, which is not necessarily
    before the caller looks. Polling removes that race without weakening the assertion:
    an uncancelled watchdog is sleeping for its whole timeout, which is far longer than
    the deadline here.
    """
    end = time.monotonic() + deadline_sec
    while True:
        leaked = set(_live_watchdog_threads()) - before
        if not leaked or time.monotonic() >= end:
            return leaked
        time.sleep(0.1)


def test_timeout_terminates_a_long_running_command():
    """Must-not-regress: the bound is actually enforced."""
    start = time.monotonic()
    rc = Shell.run(
        f"sleep {WATCHDOG_CHILD_SLEEP}", timeout=WATCHDOG_TIMEOUT, verbose=False
    )
    elapsed = time.monotonic() - start

    assert rc != 0, "a command killed by the watchdog reported success"
    assert elapsed < MAX_ELAPSED, (
        f"the watchdog took {elapsed:.1f}s to stop a {WATCHDOG_CHILD_SLEEP}s command "
        f"(>= {MAX_ELAPSED}s)"
    )


def test_timeout_is_enforced_when_a_background_descendant_holds_the_pipes():
    """The bound must hold when the shell exits but a descendant keeps the pipes open.

    A background descendant inheriting the pipes keeps `finished` unset while the leader
    is already reaped, so a leader-only `poll()` backstop returns without signalling and
    leaves the command entirely unbounded. The GROUP test is what enforces the bound.
    """
    start = time.monotonic()
    Shell.run(
        f"sleep {WATCHDOG_CHILD_SLEEP} &", timeout=WATCHDOG_TIMEOUT, verbose=False
    )
    elapsed = time.monotonic() - start

    assert elapsed < MAX_ELAPSED, (
        f"a background descendant kept the call running {elapsed:.1f}s against a "
        f"{WATCHDOG_TIMEOUT}s bound (>= {MAX_ELAPSED}s): the watchdog only checked the "
        "process leader, which was already reaped, so nothing was enforced"
    )


def test_the_group_is_force_killed_when_a_descendant_ignores_sigterm():
    """The SIGKILL escalation must fire for a descendant that ignores SIGTERM.

    Pins the liveness test at the SIGKILL gate; the SIGKILL log line is the evidence it
    ran. The child's sleep must stay well above the grace loop's own 100s budget or the
    arm passes without the SIGKILL and proves nothing: do not reduce it.
    """
    child_sleep = 200
    ceiling = 130  # the loop's 100s budget plus one 5s interval, with margin
    assert (
        child_sleep > ceiling
    ), "the child must outlive the ceiling or nothing is pinned"

    output = io.StringIO()
    start = time.monotonic()
    with contextlib.redirect_stdout(output):
        Shell.run(
            f"trap '' TERM; sleep {child_sleep} &",
            timeout=WATCHDOG_TIMEOUT,
            verbose=False,
        )
    elapsed = time.monotonic() - start
    captured = output.getvalue()

    assert elapsed < ceiling, (
        f"a SIGTERM-ignoring descendant kept the call running {elapsed:.1f}s against a "
        f"{WATCHDOG_TIMEOUT}s bound (>= {ceiling}s): the SIGKILL gate asked about the "
        "already-reaped process leader, so the escalation never ran"
    )
    assert "sending SIGKILL" in captured, (
        "the call returned in time but the SIGKILL escalation never ran, so the "
        f"descendant was not what ended it: {captured!r}"
    )


def test_a_descendant_gets_its_full_graceful_shutdown_window():
    """The grace loop must not end early, or SIGTERM handlers are cut short.

    Pins the liveness test inside the grace loop. The failure makes the call FASTER, so a
    duration assertion cannot see it: the observable is the cleanup's own marker file,
    with the absent SIGKILL corroborating that the graceful path was taken.
    """
    cleanup_seconds = 6
    marker = Path(tempfile.mkdtemp()) / "cleanup-finished"
    # A background descendant that inherits the pipes, handles SIGTERM, and needs
    # `cleanup_seconds` to finish. `wait` keeps the subshell alive to receive the signal.
    command = (
        f"( trap 'sleep {cleanup_seconds}; touch {marker}; exit 0' TERM; "
        f"  sleep {WATCHDOG_CHILD_SLEEP} & wait ) &"
    )

    output = io.StringIO()
    with contextlib.redirect_stdout(output):
        Shell.run(command, timeout=WATCHDOG_TIMEOUT, verbose=False)
    # The call returns once the readers unblock; give a cut-short handler the same wall
    # clock it would have needed, so the assertion cannot pass on timing alone.
    time.sleep(cleanup_seconds + 4)
    captured = output.getvalue()

    assert marker.exists(), (
        "the descendant's SIGTERM handler never finished: the grace loop asked about the "
        "already-reaped process leader, ended on its first iteration and escalated to "
        f"SIGKILL mid-cleanup. Captured: {captured!r}"
    )
    assert "sending SIGKILL" not in captured, (
        "the group was force-killed even though the descendant shut down gracefully "
        f"within the grace budget: {captured!r}"
    )


def test_fast_command_leaves_no_live_watchdog():
    """A command that finishes early must cancel its watchdog.

    A leaked watchdog would later killpg a reaped, possibly PID-recycled group. Two calls
    are made because the leak is per-call; the timeout is far longer than the settle
    deadline so a pre-fix watchdog is still sleeping when the assertion runs.
    """
    before = set(_live_watchdog_threads())

    assert Shell.run("echo fast", timeout=600, verbose=False) == 0
    assert Shell.run("echo fast", timeout=600, verbose=False) == 0

    leaked = _wait_for_watchdogs_to_exit(before)
    assert leaked == set(), (
        f"fast commands left live watchdog threads {sorted(leaked)}; each will later "
        "signal a process group that no longer belongs to it"
    )


def test_every_retry_attempt_is_bounded():
    """With `retries=2` BOTH attempts must be bounded, not just the first.

    Pins the per-attempt `Event`: one hoisted out of the retry loop is already set when
    attempt 2 begins waiting, restoring the unbounded hang for every caller passing both
    `timeout` and `retries > 1`. Every other arm here still passes in that case.
    """
    retries = 2
    # Inter-attempt delay in Shell.run: 2s before the second attempt.
    inter_attempt_delay = 2
    start = time.monotonic()
    rc = Shell.run(
        f"sleep {WATCHDOG_CHILD_SLEEP}",
        timeout=WATCHDOG_TIMEOUT,
        retries=retries,
        verbose=False,
    )
    elapsed = time.monotonic() - start

    # The primary pin is the return code, not the duration: an unenforced attempt 2 runs
    # the command to completion, so it SUCCEEDS and the call returns 0, where a bounded
    # one is killed and returns -15.
    assert rc != 0, (
        f"the call reported success (rc {rc}); the last attempt ran to completion, so its "
        "timeout was never enforced -- which is what a shared timeout Event causes"
    )
    # Corroborating bound, tight enough to also discriminate: bounded costs about one
    # timeout per attempt plus the inter-attempt delay, unbounded costs the child's full
    # sleep.
    ceiling = retries * WATCHDOG_TIMEOUT + inter_attempt_delay + 15
    assert elapsed < ceiling, (
        f"{retries} attempts took {elapsed:.1f}s (>= {ceiling}s); attempt 2 ran "
        "unbounded"
    )
    assert elapsed >= WATCHDOG_TIMEOUT, (
        f"the call returned in {elapsed:.1f}s, faster than a single {WATCHDOG_TIMEOUT}s "
        "bound -- the command did not run as expected, so this arm proves nothing"
    )


if __name__ == "__main__":
    import tempfile

    t0 = time.monotonic()
    test_the_archive_command_does_not_pipe_tar_into_gzip()
    print("ok test_the_archive_command_does_not_pipe_tar_into_gzip")
    for fn in (
        test_archive_overrun_is_bounded_and_not_fatal,
        test_archive_overrun_leaves_no_partial_archive,
        test_archive_reports_tar_failure_and_publishes_nothing,
        test_archive_failure_leaves_an_existing_archive_intact,
        test_archive_publishes_a_member_appended_to_while_read,
        test_archive_publishes_a_complete_archive_despite_a_failing_tar_exit,
        test_archive_skips_missing_inputs_without_raising,
        test_archive_of_nothing_returns_none,
        test_archive_publish_failure_is_reported_not_raised,
        test_the_temporary_archive_name_is_unique_per_writer,
        test_archive_success_publishes_a_complete_archive,
        test_archive_without_timeout_still_works,
    ):
        with tempfile.TemporaryDirectory() as d:
            fn(Path(d))
        print(f"ok {fn.__name__}")
    for fn in (
        test_timeout_terminates_a_long_running_command,
        test_timeout_is_enforced_when_a_background_descendant_holds_the_pipes,
        test_the_group_is_force_killed_when_a_descendant_ignores_sigterm,
        test_a_descendant_gets_its_full_graceful_shutdown_window,
        test_fast_command_leaves_no_live_watchdog,
        test_every_retry_attempt_is_bounded,
    ):
        fn()
        print(f"ok {fn.__name__}")
    print(f"ok in {time.monotonic() - t0:.1f}s")
