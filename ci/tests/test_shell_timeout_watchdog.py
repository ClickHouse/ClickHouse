"""
Regression tests for bounded post-run archiving (`Utils.compress_files_gz`) and the
`Shell` watchdog (`Shell._check_timeout`) that enforces its bound.

Background
----------
An integration shard hung ~80 minutes in the unbounded post-run `tar` that runs between
collecting the results and the only result dump, and published a job-level ERROR with no
children while 502 per-test outcomes sat in its own log.

`Utils.compress_files_gz` now takes a `timeout`, and so does the `on_error_hook` in
`Runner._get_result_object` (the second unbounded archiving path ahead of the upload).
An overrun must cost the tarball only, so three properties are required beyond the bound
itself, each of which a bounding-only change gets wrong:

* it must not raise. `Shell.check(..., strict=True, timeout=N)` raises `RuntimeError` on
  the timeout, so merely adding `timeout=` converts the hang into an exception that still
  skips the result dump. A missing input and a failed publish must not raise either.
* it must not publish a partial archive. `tar` killed by SIGTERM leaves a large truncated
  archive that the upload's existence check accepts, so it would be published as the
  logs. The archive is built under a temporary name and renamed in on success only, and
  that name is unique per writer: the two callers that write the job's `logs.tar.gz`
  otherwise share one temporary, and the loser keeps writing into the published archive.
* it must tell a real failure from a benign one, in BOTH directions:
  - `tar -cf - -T <list> | gzip > <archive>` reported only gzip's status (`Shell.run` uses
    bash without `pipefail`), so a tar that failed part-way exited 0 and the previous
    `strict=True` could not catch it at all. The direct `tar -czf` form reports it.
  - `tar` exit 1 is "some files differ", which a file appended to while being read
    produces. Every archive here contains such files, so rejecting 1 would discard a
    complete archive on essentially every failing shard - a worse regression than the
    hang, because it needs no timeout to fire.

The watchdog fix is a prerequisite, not separate scope: bounding is what starts passing
`timeout=` to short-lived commands inside long-lived processes, and `_check_timeout` slept
the timeout out unconditionally and then killpg'd the group, so a command that finished
immediately left a live watchdog that would later signal a possibly PID-recycled group.
It now waits on an `Event` set when the attempt's child is reaped, with a process-GROUP
liveness backstop: a leader-only `poll()` returns without enforcing anything whenever a
background descendant holds the output pipes that keep `finished` unset. The `Event` is
created inside the retry loop, because a single Event hoisted out of it would already be
set when attempt 2's watchdog began waiting, leaving every retry after the first
unenforced.

`TeePopen._check_timeout` had the same blind sleep and was fixed by cfb2ac8ff0c244;
`Shell._check_timeout` was the remaining twin.
"""

import os
import subprocess
import sys
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

    `tar` killed by SIGTERM leaves a large truncated archive, and the upload
    path only tests `Path(file).is_file()`, so a destination-writing implementation
    publishes that truncation as the logs. Absence is asserted rather than validity:
    `gzip -t` on such an archive is not a usable oracle here (it reports the truncation
    on stderr, but the whole point is that nothing should be there to test).
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

    `Shell.run` uses bash without `pipefail`, so `tar -cf - | gzip > out` reports only
    gzip's status: a tar that fails part-way exits 0, which is why the previous
    `strict=True` never caught a tar failure, and which would make the success-gated
    rename publish a silently incomplete archive as the logs.

    This is asserted on the command rather than behaviourally because every way of making
    tar fail deterministically is either unavailable to a test (a path vanishing
    mid-archive) or ineffective for root, which is who the CI Tests job runs as. The
    behavioural counterpart below covers a non-root developer run.
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
    """Behavioural counterpart: a failing tar is reported and publishes nothing.

    Uses an unreadable input, which root can read regardless, so this is skipped there
    (the CI Tests job runs as root in its container) and the command-form test above is
    what holds in CI. Skipping loudly rather than returning silently, so it cannot look
    like a passing assertion.
    """
    src = tmp_path / "src"
    src.mkdir()
    good = src / "good.txt"
    good.write_text("payload\n")
    unreadable = src / "unreadable.txt"
    unreadable.write_text("payload\n")
    unreadable.chmod(0o000)
    if os.access(unreadable, os.R_OK):
        unreadable.chmod(0o644)
        import pytest

        pytest.skip("running as root: an unreadable file is still readable")

    archive = tmp_path / "logs.tar.gz"
    try:
        result = Utils.compress_files_gz(
            [str(good), str(unreadable)], str(archive), timeout=600
        )
    finally:
        unreadable.chmod(0o644)

    assert result is None, (
        f"a failing tar reported success ({result!r}); the pipe form hides tar's exit "
        "status, so a silently incomplete archive would be published"
    )
    assert (
        not archive.exists()
    ), "a failing tar left an archive at the destination instead of being discarded"


def test_archive_failure_leaves_an_existing_archive_intact(tmp_path):
    """A failed attempt must not destroy an archive already at the destination.

    Both archiving paths write the same `ci/tmp/logs.tar.gz`: the normal post-run path
    and the on_error_hook, which runs afterwards on the reachable "archived fine, then
    reported ERROR" ordering. Writing to the destination and truncating it on open, or
    removing it before archiving, would replace a complete archive with nothing whenever
    the second writer overran. Building under a temporary name keeps the failure
    confined to that temporary file.
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

    A file appended to while tar reads it makes tar exit 1 ("some files differ") with a
    complete archive. Every archive this helper builds for a CI job contains such files -
    the job's own log, the docker log, live test instance directories - so treating 1 as
    failure would discard a complete archive on essentially every failing shard, without
    the timeout ever firing. The old pipe form hid this by reporting only gzip's status.

    Reliability: the appended-to member is pre-seeded so tar spends measurable time
    reading it. Measured 10/10 at 1MB and 2MB with a tight-loop writer (tar itself ~0.1s),
    and 10/10 at 2-20MB with a 5ms-interval writer.
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


def test_archive_skips_missing_inputs_without_raising(tmp_path):
    """A declared-but-absent input must be dropped, not raise.

    `Result.from_pytest_run` registers the pytest log and report files before running
    pytest, and a conftest import error or a usage error leaves both absent (measured with
    pytest 7.4.4). Raising here would skip the caller's result upload, which is exactly
    the outcome the bound exists to prevent.
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


def test_the_temporary_archive_name_is_unique_per_writer(tmp_path):
    """Two writers must not stage into the same temporary file.

    Both callers that write the job's `logs.tar.gz` stage next to it. Sharing one
    temporary name introduces a corruption mode the destination-writing form did not
    have: one writer's rename publishes the file while the other's open descriptor
    follows the inode and keeps writing into the PUBLISHED archive (measured out of
    tree: 21495808 -> 81990028 bytes after publication, and `tar -tzf` then rejects it).

    Asserted by observing the staged name, because reproducing the overlap needs two
    concurrent archiving processes racing on one path.
    """
    staged = []

    def record(command, **kwargs):
        staged.append(command)
        return 0

    original_run = Shell.run
    Shell.run = staticmethod(record)
    try:
        Utils.compress_files_gz([str(Path(__file__))], str(tmp_path / "logs.tar.gz"))
    except Exception:
        pass
    finally:
        Shell.run = original_run

    assert staged, "compress_files_gz did not invoke Shell.run"
    for command in staged:
        assert f"logs.tar.gz.{os.getpid()}.tmp" in command, (
            f"the archive is staged under a name shared by every writer: {command!r}. A "
            "second writer would keep writing into the archive the first one published"
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

    `Shell.run` joins the stdout/stderr readers before signalling that the attempt is
    over, so a background descendant inheriting those pipes keeps the readers blocked and
    `finished` unset while the shell leader is already reaped. A leader-only `poll()`
    backstop sees the reaped leader and returns without signalling the group, leaving the
    command entirely unbounded (measured: 60.0s against a 3s bound). Testing the process
    GROUP instead restores enforcement.
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


def test_fast_command_leaves_no_live_watchdog():
    """A command that finishes early must cancel its watchdog.

    Pre-fix the watchdog slept the full timeout regardless, so each such call left a
    live thread that would later killpg a reaped (possibly PID-recycled) group. Two
    calls are made because the leak is per-call and the count is the clearest signal.
    The timeout is far longer than the settle deadline, so a pre-fix watchdog is still
    sleeping when the assertion runs.
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

    This pins the per-attempt `Event`. A single Event created once per `Shell.run` call
    would already be set when attempt 2's watchdog began waiting, so attempt 2 would run
    to its natural duration with no enforcement -- silently restoring the unbounded hang
    for every caller passing both `timeout` and `retries > 1`. Every other arm in this
    file still passes in that case, so without this one the regression is invisible.
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
    # the command to completion, so it SUCCEEDS and the call returns 0 (measured: rc 0
    # after 64s with a shared Event, against rc -15 after 6s when bounded).
    assert rc != 0, (
        f"the call reported success (rc {rc}); the last attempt ran to completion, so its "
        "timeout was never enforced -- which is what a shared timeout Event causes"
    )
    # Corroborating bound, tight enough to also discriminate: bounded costs about one
    # timeout per attempt plus the delay (6s measured), unbounded costs the child's full
    # sleep (64s measured).
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
        test_fast_command_leaves_no_live_watchdog,
        test_every_retry_attempt_is_bounded,
    ):
        fn()
        print(f"ok {fn.__name__}")
    print(f"ok in {time.monotonic() - t0:.1f}s")
