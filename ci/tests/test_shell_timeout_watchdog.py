"""
Regression tests for bounded post-run archiving (`Utils.compress_files_gz`) and the
`Shell` watchdog (`Shell._check_timeout`) that enforces its bound.

Background
----------
`Integration tests (amd_asan_ubsan, db disk, old analyzer, 5/6)` published a job-level
ERROR with `results: []` while its five sibling shards at the same commit each uploaded
600-1300 test rows. 502 per-test outcome lines were present in the job log, so the
results existed: the job hung in the unbounded `tar` that runs between collecting them
and the only result dump, for ~80 minutes, until the whole runner was cancelled.

`Utils.compress_files_gz` now takes a `timeout`, and so does the `on_error_hook` in
`Runner._get_result_object` (the second unbounded archiving path ahead of the upload).
An overrun must cost the tarball only, so three properties are required beyond the bound
itself, each of which a bounding-only change gets wrong:

* it must not raise. `compress_files_gz` passed `strict=True`, and
  `Shell.check(..., strict=True, timeout=N)` raises `RuntimeError` on the timeout, so
  merely adding `timeout=` converts the hang into an exception that still skips the
  result dump.
* it must not publish a partial archive. `tar` killed by SIGTERM leaves a large
  truncated archive behind (measured: 41418752 bytes for a 1s bound over a 400-file
  tree) and the upload path only checks that the file exists, so a truncated archive
  would be published as the logs. The archive is built under a temporary name and
  renamed in on success only.
* it must be able to tell success from failure. The old command was
  `tar -cf - -T <list> | gzip > <archive>`; `Shell.run` uses bash without `pipefail`, so
  only gzip's status was visible and a tar that failed part-way exited 0 (measured: rc 0
  with a 293-byte archive for a list naming one missing path). That made the previous
  `strict=True` unable to catch a tar failure at all, and would make a success-gated
  rename publish a silently incomplete archive. The direct `tar -czf` form reports it.

The watchdog fix is a prerequisite, not separate scope: bounding is what starts passing
`timeout=` to short-lived commands inside long-lived processes, and `_check_timeout` slept
the timeout out unconditionally and then killpg'd the group. A command that finished
immediately therefore left a live watchdog thread that would later signal a possibly
PID-recycled group (measured: two live `_check_timeout` threads in one probe process).
It now waits on an `Event` set when the attempt's child is reaped, with a `poll()`
backstop. The `Event` is created inside the retry loop: a single Event hoisted out of it
would already be set when attempt 2's watchdog started waiting, leaving every retry after
the first with no timeout enforcement at all.

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

    `tar` killed by SIGTERM leaves a valid-looking truncated archive, and the upload
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
        return True

    original_check = Shell.check
    Shell.check = staticmethod(record)
    try:
        Utils.compress_files_gz([str(Path(__file__))], "/dev/null/unused")
    except Exception:
        pass
    finally:
        Shell.check = original_check

    assert calls, "compress_files_gz did not invoke Shell.check"
    for command in calls:
        assert "|" not in command, (
            f"the archiving command pipes tar's output: {command!r}. Without pipefail "
            "only gzip's status is visible, so a failing tar reports success"
        )
        assert "tar -czf" in command, (
            f"unexpected archiving command {command!r}: expected a direct `tar -czf`, "
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
    assert not (
        tmp_path / "logs.tar.gz.tmp"
    ).exists(), "the temporary archive was not cleaned up after a successful rename"


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

    assert rc != 0
    # A bounded second attempt costs about one more timeout; an unbounded one costs the
    # child's full sleep. The ceiling sits well between the two.
    ceiling = retries * MAX_ELAPSED + inter_attempt_delay
    assert elapsed < ceiling, (
        f"{retries} attempts took {elapsed:.1f}s (>= {ceiling}s); attempt 2 ran "
        f"unbounded, which is what a shared timeout Event causes"
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
        test_archive_success_publishes_a_complete_archive,
        test_archive_without_timeout_still_works,
    ):
        with tempfile.TemporaryDirectory() as d:
            fn(Path(d))
        print(f"ok {fn.__name__}")
    for fn in (
        test_timeout_terminates_a_long_running_command,
        test_fast_command_leaves_no_live_watchdog,
        test_every_retry_attempt_is_bounded,
    ):
        fn()
        print(f"ok {fn.__name__}")
    print(f"ok in {time.monotonic() - t0:.1f}s")
