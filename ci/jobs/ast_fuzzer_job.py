#!/usr/bin/env python3
import logging
import os
import random
import select
import signal
import sys
import time
import traceback
from pathlib import Path

from ci.jobs.scripts.clickhouse_service import ClickHouseService
from ci.jobs.scripts.find_tests import Targeting
from ci.jobs.scripts.docker_image import DockerImage
from ci.jobs.scripts.log_parser import FuzzerLogParser
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils

IMAGE_NAME = "clickhouse/fuzzer"

# Maximum number of reproduce commands to display inline before writing to file
MAX_INLINE_REPRODUCE_COMMANDS = 20

cwd = Utils.cwd()
WORKSPACE_PATH = Path(cwd) / "ci/tmp/workspace"

# Paths of artifacts produced by the fuzzer runner script.
# Exported so tests can pre-seed them without duplicating the paths.
JOB_ARTIFACTS = (
    WORKSPACE_PATH / "server.log",
    WORKSPACE_PATH / "fuzzer.log",
    WORKSPACE_PATH / "stderr.log",
    WORKSPACE_PATH / "dmesg.log",
    WORKSPACE_PATH / "fatal.log",
)

# Failure markers written by run-fuzzer.sh when the post-fuzz server is
# memory-stuck (killed by us) or a harness watchdog fired (reap/teardown
# escalation, or a probes-stage declaration that a live server is unanswering).
# Read before anything else consumes the workspace so they drive classification.
MEMORY_STUCK_MARKER = WORKSPACE_PATH / "server_memory_stuck.txt"
HARNESS_WATCHDOG = WORKSPACE_PATH / "harness_watchdog.txt"

# Written by run-fuzzer.sh immediately BEFORE it stops the server, so an abort
# between the graceful stop and the status.tsv write can still be distinguished
# from a startup failure. NOT a failure marker: it never sets a status, it only
# witnesses that a "Received signal 15" in the log is ours.
SERVER_STOPPING = WORKSPACE_PATH / "server_stopping.txt"

# Wall-clock bound on host-side core collection (compress + encrypt, up to 3
# cores). Both callers PERSIST the classified result before collecting, so this
# bound decides core-vs-no-core, never report-vs-no-report.
#
# Still sized against the remaining budget rather than for comfort. Worst case for
# a 60m run with every run-fuzzer.sh bound at its limit: reap 3600+300+90 = 3990s,
# probes 300s, teardown 180s, client grace 10s = 4480s (74.7 min) measured from the
# START of the fuzz budget, i.e. excluding configure/server-startup/gdb-attach,
# against an observed external cancellation at 78.3 min. So the margin is already
# thin without this step: 180s is a few times a healthy collection (zstd streams a
# 1 GiB core, the server's default core_dump.size_limit, in seconds) and small
# enough not to be the reason a run is cut off.
CORE_COLLECTION_DEADLINE = 180

# Structure the collected artifacts must have for the core to be decryptable.
# `openssl enc -aes-256-cbc` emits whole 16-byte blocks; `pkeyutl -encrypt`
# against the 4096-bit key in ci/defs/public.pem emits exactly 512 bytes. Neither
# command's exit code is checked by Utils.encrypt, so this is the only signal that
# a truncated write happened.
AES_BLOCK_BYTES = 16
RSA_WRAPPED_KEY_BYTES = 512

# Stable CIDB signature for the post-fuzz memory-stuck state, reused from
# FuzzerLogParser so a marker row and a parser-classified memory-limit row share
# one countable name regardless of which path produced it.
MEMORY_STUCK_NAME = "Server unresponsive: memory limit exceeded"

# Parser inputs and generated reports that must not survive into a later run on a
# reused praktika worktree (ci/tmp is gitignored; server.log/stderr.log are
# opened in append mode; sanitizer.log.* and core.* are globbed; fuzzerout.sql is
# the BuzzHouse fuzzer_log the parser reads). NOT deleted:
# ci-targeted-queries.txt / fuzz.json / ci-changed-files.txt are written BEFORE
# the container runs and must survive.
_STALE_RUN_STATE = (
    MEMORY_STUCK_MARKER,
    HARNESS_WATCHDOG,
    # Must be cleaned like the markers: a leftover witness would tell the next run
    # that its server's signal line is self-inflicted and suppress a real finding.
    SERVER_STOPPING,
    WORKSPACE_PATH / "status.tsv",
    WORKSPACE_PATH / "server.log",
    WORKSPACE_PATH / "stderr.log",
    WORKSPACE_PATH / "fuzzer.log",
    WORKSPACE_PATH / "fuzzerout.sql",
    WORKSPACE_PATH / "fatal.log",
    WORKSPACE_PATH / "dmesg.log",
)


def _read_marker_text(path: Path, max_bytes: int = 8192) -> str:
    """Bounded read of a small marker file, or '' if absent/unreadable."""
    try:
        return path.read_text(encoding="utf-8", errors="replace")[:max_bytes].strip()
    except OSError:
        return ""


def _memory_stuck_result() -> "Result | None":
    """FAIL Result for a post-fuzz memory-stuck run, or None when no marker."""
    if not MEMORY_STUCK_MARKER.exists():
        return None
    info = _read_marker_text(MEMORY_STUCK_MARKER)
    return Result(
        name=MEMORY_STUCK_NAME,
        status=Result.Status.FAIL,
        info=info or "server-global memory limit exceeded on consecutive idle probes",
    )


def _harness_watchdog_result() -> "Result | None":
    """ERROR Result when a harness watchdog fired (reap/teardown/probes stage), or None."""
    if not HARNESS_WATCHDOG.exists():
        return None
    info = _read_marker_text(HARNESS_WATCHDOG)
    return Result(
        name="Fuzzer harness watchdog fired",
        status=Result.Status.ERROR,
        info=info or "fuzzer harness watchdog fired",
    )


def _watchdog_stage_teardown() -> bool:
    """True iff a TEARDOWN-stage watchdog fired (the harness SIGKILLed the server).

    Only a teardown escalation kills the server; a reap-stage escalation kills
    client-side processes only, so a genuine independent server signal must stay
    reportable there (self_killed_server must NOT be set for a reap-only run).
    """
    return "stage=teardown" in _read_marker_text(HARNESS_WATCHDOG)


def _watchdog_stage_probes() -> bool:
    """True iff the probe stage ended a live-but-unanswering server (zero-answer exhaustion or persistent probe timeouts).

    run-fuzzer.sh then stops that server ITSELF (graceful stop), so the
    normal-termination "Received signal 15" line in the log is self-inflicted
    -- the parser must not attribute it (same reasoning as the teardown stage).
    """
    return "stage=probes" in _read_marker_text(HARNESS_WATCHDOG)


def _unreadable_artifacts(paths) -> "list[Path]":
    """Existing, non-empty artifacts the current user cannot read.

    The in-container `chmod -R a+r` normally covers these, but it cannot run if
    the container died without reaching its EXIT trap (kernel OOM kill, docker
    teardown), so this is the mechanical check that decides whether the host-side
    ownership repair is still needed. Only files that exist and carry content are
    considered -- an absent or empty artifact is nothing to rescue.
    """
    unreadable = []
    for path in paths:
        try:
            if not path.is_file() or path.stat().st_size == 0:
                continue
        except OSError:
            continue
        if not os.access(path, os.R_OK):
            unreadable.append(path)
    return unreadable


def _collectable_cores() -> "list[Path]":
    """The cores ClickHouseService.collect_cores will actually read.

    Mirrors that collector's own selection -- sorted `core.*`, first 3, and only
    THEN skipping the already-processed `.zst`/`.enc` -- so the readability
    decision covers exactly the files it will `zstd`, no more: it raises on an
    unreadable one, and that raise happens after classification, discarding the
    whole Result. Slicing after the filter would inspect cores past the
    collector's cutoff and run the unbounded repair container for a file that is
    never read. Kept out of `paths` on purpose: `paths` is the upload list, and a
    core may only leave the runner compressed and encrypted.
    """
    return [
        core
        for core in sorted(WORKSPACE_PATH.glob("core.*"))[:3]
        if not core.name.endswith(".zst") and not core.name.endswith(".enc")
    ]


def _preseeded_state_is_input() -> bool:
    """True when this workspace was seeded on purpose and must not be cleaned.

    Only ci/tests/test_e2e.py sets this: it writes status.tsv plus the job
    artifacts and then invokes the real job, so for that fixture the files below
    are INPUT rather than leftovers. Deliberately not keyed on the generic
    local-run flag, which is the default for every `ci.praktika run` and would
    stop ordinary local reruns from cleaning up after themselves.
    """
    return os.getenv("PRAKTIKA_FUZZER_KEEP_PRESEEDED_STATE") == "1"


def _clean_stale_run_state() -> None:
    """Delete stale per-run classification inputs before launching the container.

    Fail closed: a surviving marker/status.tsv would misclassify the next run, so
    if any input cannot be removed we raise before the container launches rather
    than running against corrupted state.

    Runs unconditionally otherwise, including for leftovers of a run that
    predates this cleanup, because praktika reuses worktrees: ci/tmp is
    gitignored, so its `git clean -ffd` keeps whatever the last run left there.
    """
    if _preseeded_state_is_input():
        logging.info(
            "PRAKTIKA_FUZZER_KEEP_PRESEEDED_STATE=1: treating existing state in %s "
            "as job input, not cleaning it",
            WORKSPACE_PATH,
        )
        return
    # core.* too: zstd keeps its input, so a raw core outlives its own run, and
    # both the runner tail and collect_cores glob core.* blindly. A later
    # core-less run (a memory-stuck kill writes none) would otherwise compress,
    # encrypt and attach the previous run's core as evidence for this failure.
    # aes.key/aes.key.rsa too: collect_cores reuses an existing aes.key and only
    # wraps a fresh one when the .rsa is ABSENT, so leftovers make the next run
    # encrypt with the previous run's key while attaching the previous run's .rsa
    # -- measured to produce a core nobody can decrypt, plus a stale artifact
    # presented as this failure's evidence (same class as the stale core above).
    stale = (
        list(_STALE_RUN_STATE)
        + sorted(WORKSPACE_PATH.glob("sanitizer.log.*"))
        + sorted(WORKSPACE_PATH.glob("core.*"))
        + [WORKSPACE_PATH / "aes.key", WORKSPACE_PATH / "aes.key.rsa"]
    )
    for path in stale:
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        except OSError as e:
            logging.warning("Failed to remove stale %s: %s", path, e)
    survivors = [p for p in stale if p.exists()]
    if survivors:
        raise RuntimeError(
            "stale classification inputs could not be removed: "
            + ", ".join(str(p) for p in survivors)
        )


def _oom_leniency_granted(
    sanitizer_oom, kernel_oom_kill, marker_result, watchdog_result
):
    """Sanitized-build OOM forgiveness applies only when the harness did not flag the run.

    A marker (post-fuzz memory-stuck kill) or watchdog (reap/teardown escalation
    or probes-stage exhaustion) means we manufactured the OOM-looking signal
    ourselves, so it must never be forgiven as an incidental sanitizer/kernel OOM.
    """
    return bool(sanitizer_oom or kernel_oom_kill) and (
        marker_result is None and watchdog_result is None
    )


def _force_fail_for_markers(status, is_failed, marker_result, watchdog_result):
    """A marker/watchdog run must never report OK.

    A reap-abandon writes fuzzer_exit_code=137, which otherwise rides the benign
    (0, 137, 143) OK branch; a teardown escalation touches only server_exit_code,
    ignored when server_died=0. Force the run failed so it reaches classification.
    Returns the (possibly adjusted) (status, is_failed).
    """
    if marker_result is not None or watchdog_result is not None:
        is_failed = True
        if status == Result.Status.OK:
            status = Result.Status.FAIL
    return status, is_failed


def _should_parse_logs(is_failed, status, server_died) -> bool:
    """True iff the log parser should scrape server.log for a crash signature.

    Restores master's invariant: parse ONLY when the server actually died.
    A reap/teardown watchdog on a HEALTHY server flips OK->FAIL via
    _force_fail_for_markers with server_died=0; if the parser ran there it would
    match the normal-termination "Received signal 15" line that stop_server logs
    at Trace and emit a bogus crash row, suppressing the watchdog ERROR. Gating
    on server_died routes those runs to the sub-results branch instead.
    """
    return bool(is_failed) and status != Result.Status.ERROR and bool(server_died)


def _top_level_status(status, results):
    """Status to pass to Result.create_from, or None to derive it from sub-results.

    create_from derives the status from the sub-results when it gets none, which
    is what lets a parser row report the failure. But a marker sub-result is a
    FAIL, so deriving would DOWNGRADE an ERROR already decided upstream (the
    BuzzHouse 227 branch, a client-side fuzzer failure, or OOM found in dmesg).
    ERROR outranks FAIL in that same derivation, so keeping it is praktika's own
    precedence rather than an exception to it.
    """
    if not results or status == Result.Status.ERROR:
        return status
    return None


def _collect_cores_or_note(deadline: int = CORE_COLLECTION_DEADLINE) -> "tuple[list, str]":
    """(encrypted core artifacts, note) -- never raises, never blocks past `deadline`.

    ClickHouseService.collect_cores shells out to zstd/openssl, so it raises on
    any collection failure (an unreadable core is the measured case) and it can
    also stall: compressing a multi-GiB raw core on a nearly-full box has no
    bound of its own (Shell.check does not forward its timeout to Shell.run).
    Both call sites run AFTER classification, so either failure mode costs the
    whole Result -- marker, watchdog, logs and all -- which is strictly worse than
    losing the core it was trying to save, and a stall specifically recreates the
    artifact-less external cancellation this change exists to remove.

    So run the WHOLE collection in one child process of its own session, and kill
    that session on the deadline. A thread cannot be cancelled: collect_cores
    walks up to three cores through sequential zstd/openssl commands (and
    Utils.encrypt ignores their exit codes), so killing the one process that
    happens to be running still leaves the worker free to launch the next stage --
    measured. Killing the child's whole process group stops every stage at once,
    and nothing survives our exit (Shell.run spawns with start_new_session=True,
    so a stray was measured reparenting to PID 1 and still compressing).

    Success is verified rather than assumed: because those exit codes are ignored,
    a failed wrap or a truncated cipher otherwise yields an UNDECRYPTABLE core
    reported as a clean collection. Anything unusable is reported in the job info
    instead, so a missing core is visible not silent.
    """
    if not _collectable_cores():
        # Nothing to collect: skip the fork entirely. The common case (a
        # memory-stuck kill writes no core) then costs nothing and cannot fail.
        return [], ""
    try:
        read_fd, write_fd = os.pipe()
        pid = os.fork()
    except OSError as e:
        # Never-raise contract: on the hosts this change exists for, fork can fail
        # with EAGAIN/ENOMEM precisely when a core is worth having, and raising
        # here would discard the already-classified Result.
        logging.error("Fuzzer: could not start core collection: %s", e)
        return [], f"WARNING: could not start core collection, cores not attached: {e}"
    if pid == 0:  # pragma: no cover - child process
        status = 1
        try:
            os.close(read_fd)
            os.setsid()  # own session: one killpg stops every collection stage
            files = ClickHouseService.collect_cores(WORKSPACE_PATH)
            payload = "\n".join(str(f) for f in files).encode()
            os.write(write_fd, payload)
            status = 0
        except BaseException:  # noqa: BLE001 - report via exit status, never raise here
            traceback.print_exc()
        finally:
            os._exit(status)

    os.close(write_fd)
    started_at = time.monotonic()
    collected = _read_until_eof(read_fd, deadline)
    if collected is None:
        logging.error("Fuzzer: core collection still running after %ss", deadline)
        # `deadline` is the bound on the WHOLE helper, so the teardown has to come
        # out of the same budget: stopping and reaping the collector each allowed a
        # further 5 s, which would return up to 10 s past the advertised deadline
        # and spend that much of the thin cancellation margin. Give cleanup only
        # what is left (and always a little, so a hung collector is still signalled).
        remaining = max(0.5, deadline - (time.monotonic() - started_at))
        _kill_process_group(pid, timeout=remaining / 2)
        _reap(pid, timeout=remaining / 2)
        return [], (
            f"WARNING: core collection did not finish within {deadline}s, cores "
            "not attached (compressing a multi-GiB core can outlast the job)"
        )
    wait_status = _reap(pid)
    if wait_status:
        logging.error("Fuzzer: core collection failed (wait status %s)", wait_status)
        return [], (
            "WARNING: core collection failed, cores not attached (see the job log "
            f"for the collector traceback; wait status {wait_status})"
        )
    files = [f for f in collected.decode(errors="replace").split("\n") if f]
    unusable = _unusable_core_artifacts(files)
    if unusable:
        logging.error("Fuzzer: core collection incomplete: %s", unusable)
        return [], f"WARNING: core collection incomplete, cores not attached: {unusable}"
    return files, ""


def _read_until_eof(fd: int, deadline: int) -> "bytes | None":
    """All bytes from `fd` until EOF, or None if EOF does not arrive in `deadline`.

    Closes `fd` either way. Used instead of joining a thread so the deadline is
    enforced against a process we can actually kill.
    """
    chunks = []
    give_up_at = time.monotonic() + deadline
    try:
        while True:
            remaining = give_up_at - time.monotonic()
            if remaining <= 0:
                return None
            if not select.select([fd], [], [], remaining)[0]:
                return None
            chunk = os.read(fd, 65536)
            if not chunk:
                return b"".join(chunks)
            chunks.append(chunk)
    except OSError:
        return b"".join(chunks)
    finally:
        try:
            os.close(fd)
        except OSError:
            pass


def _descendants(pid: int) -> "list[int]":
    """`pid` plus every descendant, deepest last, read from /proc.

    Shell.run starts each command with start_new_session=True, so the collector's
    zstd/openssl are NOT in our child's process group -- signalling only that group
    leaves them running (measured). Walk the real parent links instead.
    """
    parents: "dict[int, list[int]]" = {}
    try:
        entries = os.listdir("/proc")
    except OSError:
        return [pid]
    for entry in entries:
        if not entry.isdigit():
            continue
        try:
            with open(f"/proc/{entry}/stat", "rb") as fh:
                # ppid is field 4, but comm (field 2) can contain spaces and
                # parentheses -- split after the last ')'.
                ppid = int(fh.read().rsplit(b")", 1)[1].split()[1])
        except (OSError, IndexError, ValueError):
            continue
        parents.setdefault(ppid, []).append(int(entry))
    ordered = [pid]
    queue = [pid]
    while queue:
        for child in parents.get(queue.pop(), []):
            ordered.append(child)
            queue.append(child)
    return ordered


def _signal_kill(pid: int) -> None:
    """SIGKILL `pid`'s group, else `pid` itself. Best-effort, never raises.

    The group matters because each command Shell.run starts leads its own session.
    Every step is guarded: the process may exit between the /proc walk and the
    signal, and a ProcessLookupError escaping here would discard the classified
    Result this whole wrapper exists to protect.
    """
    try:
        group = os.getpgid(pid)
        # NEVER signal our own group: between fork() and the child's setsid() it
        # still shares ours, so a killpg there SIGKILLs the whole job -- measured,
        # the job dies with 137 and reports nothing at all. Fall back to the single
        # pid in that window; the child sets up its own session immediately, so the
        # window is tiny and its descendants do not exist yet.
        if group == os.getpgid(0):
            raise OSError(f"pid {pid} still shares our process group ({group})")
        os.killpg(group, signal.SIGKILL)
        return
    except OSError as e:
        logging.warning("Fuzzer: not killing core collector group of %s: %s", pid, e)
    try:
        os.kill(pid, signal.SIGKILL)
    except OSError as e:
        logging.warning("Fuzzer: could not kill core collector %s: %s", pid, e)


def _kill_process_group(pid: int, timeout: float = 5.0) -> None:
    """SIGKILL `pid` and everything it spawned. Best-effort, never raises.

    Ordering is the whole difficulty, and each step is load-bearing:

      - STOP the collector first (SIGSTOP). It walks several cores through
        sequential commands, each started in its own session by Shell.run, so while
        it runs it can see the command we are killing finish and launch the next
        stage -- which would be in no snapshot and would outlive us. SIGSTOP
        freezes it without reparenting anything.
      - SNAPSHOT the descendants next, now that the set cannot grow. (Killing the
        collector before snapshotting would reparent its children to init, where
        parent links can no longer find them.)
      - Then kill the snapshot deepest-first, and the collector last -- it is
        already frozen, so it cannot start anything in between.

    Correctness (the classified Result) never depends on this; it is hygiene on a
    path where the job is about to report anyway.
    """
    _await_stopped(pid, timeout=timeout)
    strays = _descendants(pid)[1:]
    for victim in reversed(strays):
        _signal_kill(victim)
    _signal_kill(pid)


def _reap(pid: int, timeout: "float | None" = None) -> int:
    """Wait for `pid` and return its wait status; 0 if already reaped or still alive.

    Idempotent on purpose: the cleanup path's SIGSTOP wait may itself consume the
    exit status of a collector that finished right at the deadline, and a second
    unguarded waitpid would then raise ChildProcessError out of a helper that
    promises never to raise -- discarding the classified Result.

    With a `timeout` the wait is BOUNDED and gives up rather than blocking: even
    SIGKILL does not free a process stuck in uninterruptible I/O, and blocking there
    would recreate the very hang this wrapper exists to prevent. Giving up leaves a
    zombie, which the job's imminent exit disposes of.
    """
    if timeout is None:
        try:
            _, status = os.waitpid(pid, 0)
            return status
        except ChildProcessError:
            return 0
    give_up_at = time.monotonic() + timeout
    while time.monotonic() < give_up_at:
        try:
            waited, status = os.waitpid(pid, os.WNOHANG)
        except ChildProcessError:
            return 0
        if waited == pid:
            return status
        time.sleep(0.01)
    logging.warning("Fuzzer: core collector %s did not exit after being killed", pid)
    return 0


def _await_stopped(pid: int, timeout: float = 5.0) -> None:
    """SIGSTOP `pid` and wait until it is really stopped. Best-effort, never raises.

    kill() only QUEUES the signal, so returning from it does not mean the collector
    has parked -- it can still start the next command, which would then be missing
    from the snapshot taken next. waitpid(WUNTRACED) reports the stop transition, so
    wait for it (or for the process to exit, which is equally fine: an exited
    collector starts nothing either).
    """
    try:
        os.kill(pid, signal.SIGSTOP)
    except OSError as e:
        logging.warning("Fuzzer: could not freeze core collector %s: %s", pid, e)
        return
    give_up_at = time.monotonic() + timeout
    while time.monotonic() < give_up_at:
        try:
            waited, status = os.waitpid(pid, os.WUNTRACED | os.WNOHANG)
        except ChildProcessError:
            return  # already reaped: it cannot spawn anything
        if waited == pid and (os.WIFSTOPPED(status) or os.WIFEXITED(status)):
            return
        time.sleep(0.01)
    logging.warning("Fuzzer: core collector %s did not stop within %ss", pid, timeout)


def _unusable_core_artifacts(files) -> str:
    """"" if the collected artifacts are usable, else why they are not.

    A collected core is only recoverable as the pair (encrypted core, RSA-wrapped
    AES key), and neither half's exit code is checked: Utils.encrypt ignores both
    openssl results and returns the .enc path unconditionally, and collect_cores
    appends the .rsa only `if` it exists. So a failed wrap looks exactly like
    success, and a truncated cipher (measured under an output size limit: openssl
    died leaving a non-empty partial .enc) passes any presence-only test.

    Check the SIZE both halves must have. `openssl enc -aes-256-cbc` is
    deterministic -- a 16-byte salt header plus PKCS#7-padded blocks, i.e.
    16 + (len//16 + 1)*16, verified over plaintexts 0..4096 -- and zstd KEEPS its
    input, so the .zst the cipher was made from is still on disk and gives the
    exact expected size. Block alignment alone would accept a partial write that
    happened to stop on a block boundary (the measured output-limit failure left
    exactly 1024 bytes). `pkeyutl -encrypt` against the 4096-bit key in
    ci/defs/public.pem emits exactly 512 bytes.
    """
    if not files:
        return ""
    names = [Path(str(f)) for f in files]
    missing = [p for p in names if not p.is_file() or p.stat().st_size == 0]
    if missing:
        return f"missing or empty artifact(s): {', '.join(p.name for p in missing)}"
    ciphers = [p for p in names if p.name.endswith(".enc")]
    wrapped = [p for p in names if p.name.endswith(".rsa")]
    if ciphers and not wrapped:
        return "encrypted core without the RSA-wrapped AES key (it would be undecryptable)"
    truncated = [p for p in ciphers if _cipher_is_short(p)]
    if truncated:
        return (
            "truncated encrypted core(s), smaller than the plaintext they should "
            f"contain: {', '.join(p.name for p in truncated)}"
        )
    bad_wrap = [p for p in wrapped if p.stat().st_size != RSA_WRAPPED_KEY_BYTES]
    if bad_wrap:
        return (
            f"wrapped AES key is not {RSA_WRAPPED_KEY_BYTES} bytes, so the core "
            f"could not be decrypted: {', '.join(p.name for p in bad_wrap)}"
        )
    return ""


def _cipher_is_short(cipher: Path) -> bool:
    """True if `cipher` is smaller than encrypting its plaintext would produce.

    `openssl enc -aes-256-cbc -pbkdf2` writes a 16-byte salt header followed by
    PKCS#7-padded blocks, so the size is exactly 16 + (len//16 + 1)*16 -- verified
    over plaintexts 0..4096. The plaintext is the `.zst` the cipher was made from,
    which survives because zstd keeps its input; when it is gone (an already-
    processed core from an earlier run) fall back to block alignment, which is all
    that can be checked without it.
    """
    size = cipher.stat().st_size
    plaintext = cipher.with_suffix("")  # core.N.zst.enc -> core.N.zst
    try:
        plain_size = plaintext.stat().st_size
    except OSError:
        return bool(size % AES_BLOCK_BYTES)
    expected = AES_BLOCK_BYTES + (plain_size // AES_BLOCK_BYTES + 1) * AES_BLOCK_BYTES
    return size < expected


def _early_abort_status(exc: Exception, sub_results):
    """Status for the status.tsv-read failure path, or None to derive from sub-results.

    Only a MISSING/EMPTY status.tsv (FileNotFoundError, which _read_fuzzer_status
    raises for both) means "the runner aborted before reporting" -- exactly the
    state a marker, a watchdog, or a genuine parser finding authoritatively
    classifies, so there the sub-results may set the top-level status. A startup
    crash that dies before any marker is written reaches this path with only a
    parser finding, and reporting that finding is the whole point of parsing every
    early abort; with no sub-result at all the status stays ERROR.

    A parser NO-MATCH is not such a classification. The parser always yields a row
    (UNKNOWN_ERROR / "Lost connection to server") and, with no marker to outrank it,
    that row arrives here as a plain FAIL -- which would downgrade a genuine harness
    fault to "the test failed" for the commonest early abort of all, one whose logs
    say nothing attributable. ERROR is what praktika reserves for that, so only a
    row the parser actually attributed may set the status.

    Every other exception is a fault of the file itself: malformed contents
    (ValueError), an unreadable file (PermissionError, i.e. the ownership repair
    did not cover it), undecodable bytes (UnicodeDecodeError).
    _format_status_error already reports those as harness bugs, and ERROR is the
    status praktika reserves for them; deriving from a marker sub-result (a FAIL)
    would downgrade the run to a plain test failure and hide the harness fault --
    the misattribution this change exists to remove. Same ERROR-outranks-FAIL
    precedence _top_level_status keeps downstream.
    """
    if not sub_results or not isinstance(exc, FileNotFoundError):
        return Result.Status.ERROR
    if all(_is_parser_no_match(r) for r in sub_results):
        return Result.Status.ERROR
    return None


def _early_abort_stopped_by_harness(marker_result, watchdog_result) -> bool:
    """True when the harness had already stopped the server before the early abort.

    Tells the parser whether a "Received signal 15" in the log is self-inflicted.
    Three witnesses, all meaning "we got at least as far as stopping it": a
    memory-stuck marker, a harness watchdog, or the shutdown record run-fuzzer.sh
    writes immediately BEFORE the graceful stop (status.tsv is written only after,
    so an abort in that window has no marker of its own).

    False only for an abort BEFORE the shutdown phase -- a startup failure, where no
    stop ever ran and a genuine server signal may be the only evidence there is.

    Extracted so both polarities are testable: `run_fuzz_job` drives docker and
    cannot run from the unit suite, and inlining this made an inverted witness test
    pass everywhere.
    """
    return (
        marker_result is not None
        or watchdog_result is not None
        or SERVER_STOPPING.exists()
    )


def _is_parser_no_match(result) -> bool:
    """True for the parser's "found nothing attributable" row.

    That row is a FAIL carrying UNKNOWN_ERROR, so it is indistinguishable from a
    real finding by status alone; the name is what separates them.
    """
    return getattr(result, "name", None) == FuzzerLogParser.UNKNOWN_ERROR


def _select_failure_result(
    parsed_name, parsed_info, files, marker_result, watchdog_result, marker_text=""
):
    """Pick the single failure Result, parser-first with the marker as the floor.

    A genuine earlier parser finding (sanitizer / logical error / assertion /
    runtime / segfault) outranks the memory-stuck classification. The marker row
    wins only when the parser found nothing attributable (UNKNOWN_ERROR) or its
    own memory-limit name (same physical state, one stable CIDB signature). The
    watchdog ERROR sits below the marker and above a bare no-match. Returns a
    Result or None.
    """
    parser_found_real = bool(parsed_name) and parsed_name not in (
        FuzzerLogParser.UNKNOWN_ERROR,
        MEMORY_STUCK_NAME,
    )
    if parser_found_real:
        if marker_result is not None and marker_text:
            parsed_info = f"{parsed_info}\n---\n{marker_text}"
        return Result(
            name=parsed_name,
            info=parsed_info,
            status=Result.Status.FAIL,
            files=files,
        )
    if marker_result is not None:
        # marker/watchdog files upload via the top-level `paths` loop; the parser's
        # reproduce files (if any) belong to a parser row, not here.
        return marker_result
    if watchdog_result is not None:
        return watchdog_result
    if parsed_name:
        return Result(
            name=parsed_name,
            info=parsed_info,
            status=Result.Status.FAIL,
            files=files,
        )
    return None


def _parse_and_select_failure(
    server_log,
    stderr_log,
    fuzzer_log,
    buzzhouse,
    marker_result,
    watchdog_result,
    server_stopped_by_harness=False,
):
    """Run the log parser and pick the failure row, or None.

    Shared by the normal classification and the status.tsv early abort: the parser
    reads only the logs, so a genuine sanitizer / logical-error / segfault finding
    is available even when status.tsv never arrived -- and it must still outrank
    the marker there, or a late `set -e` abort after the marker write would
    relabel a real crash as the generic memory-stuck row.

    When the harness itself ended a server it knew was alive (marker, a
    TEARDOWN-stage watchdog, or a probes-stage exhaustion), the parser is told to
    ignore the self-inflicted "Signal" line; a reap-stage watchdog killed only
    client-side processes, so a genuine server signal there stays reportable.

    `server_stopped_by_harness` extends that to callers who know the harness had
    already stopped the server regardless of stage. On the normal path a reap-only
    run reaches the parser only when server_died=1 (_should_parse_logs), i.e. the
    server really died on its own; the status.tsv early abort has no such witness,
    and it can only be reached AFTER the graceful stop, so the SIGTERM in the log
    is ours there and must not become the failure row.
    """
    self_killed_server = (
        server_stopped_by_harness
        or marker_result is not None
        or _watchdog_stage_teardown()
        or _watchdog_stage_probes()
    )
    # Only these two paths actually SIGKILL the server; a probes-stage watchdog and
    # the early abort merely stopped it gracefully, so a watchdog signal-9 record
    # there is the kernel OOM killer or another independent event and must stay
    # reportable (parser-first precedence).
    server_sigkilled_by_harness = marker_result is not None or _watchdog_stage_teardown()
    fuzzer_log_parser = FuzzerLogParser(
        server_log=str(server_log),
        stderr_log=str(stderr_log),
        fuzzer_log=str(WORKSPACE_PATH / "fuzzerout.sql" if buzzhouse else fuzzer_log),
        self_killed_server=self_killed_server,
        server_sigkilled_by_harness=server_sigkilled_by_harness,
    )
    parsed_name, parsed_info, files = fuzzer_log_parser.parse_failure()
    return _select_failure_result(
        parsed_name,
        parsed_info,
        files,
        marker_result,
        watchdog_result,
        marker_text=_read_marker_text(MEMORY_STUCK_MARKER),
    )


def _log_tail(path: Path, max_lines: int = 50, max_bytes: int = 65536) -> str:
    """Last `max_lines` lines of `path` (bounded read), or "" if absent/empty."""
    try:
        size = path.stat().st_size
        if size == 0:
            return ""
        with open(path, "rb") as fh:
            if size > max_bytes:
                fh.seek(-max_bytes, os.SEEK_END)
            data = fh.read()
    except OSError:
        return ""
    return "\n".join(data.decode("utf-8", errors="replace").splitlines()[-max_lines:])


def _read_fuzzer_status(status_path: Path) -> tuple[bool, int, int]:
    """Parse (server_died, server_exit_code, fuzzer_exit_code) from status.tsv.

    Raises FileNotFoundError when the file is missing or empty (the runner
    aborted before writing it) and ValueError when its contents are malformed.
    """
    if not status_path.exists():
        raise FileNotFoundError(f"{status_path} was not produced by the fuzzer runner")
    first_line = status_path.read_text(encoding="utf-8").split("\n", 1)[0]
    if not first_line.strip():
        raise FileNotFoundError(f"{status_path} is empty")
    fields = first_line.split("\t")
    if len(fields) != 3:
        raise ValueError(
            f"expected 3 tab-separated fields, got {len(fields)}: {first_line!r}"
        )
    server_died, server_exit_code, fuzzer_exit_code = fields
    return bool(int(server_died)), int(server_exit_code), int(fuzzer_exit_code)


def _format_status_error(exc: Exception, log_paths) -> str:
    """Actionable job-error text for a missing/malformed status.tsv, with log tails."""
    tails = []
    for path in log_paths:
        tail = _log_tail(path)
        if tail:
            tails.append(f"--- {path.name} (last lines) ---\n{tail}")
    tails_str = ("\n\n" + "\n\n".join(tails)) if tails else ""

    if isinstance(exc, FileNotFoundError):
        return (
            "Fuzzer runner aborted before writing status.tsv. run-fuzzer.sh runs "
            "under 'set -e' and writes status.tsv only at the very end, so any "
            "earlier failure lands here: a server startup failure (e.g. the "
            "clickhouse-server pid file is never created), a fuzzer-harness "
            "error, or an infrastructure problem (job timeout, out of memory, "
            "docker/orchestration). Inspect the log tails below to determine the "
            "cause; a normal fuzzer finding instead writes a complete status.tsv "
            "(the three numeric fields server_died, server_exit_code, "
            "fuzzer_exit_code), which run_fuzz_job then reports as FAIL with a "
            "stack trace parsed from the logs." + tails_str
        )

    tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    return (
        f"Fuzzer runner wrote an unparseable status.tsv ({exc}). This is a "
        f"fuzzer-harness bug. Traceback:\n{tb}" + tails_str
    )


def get_run_command(
    image: DockerImage,
    buzzhouse: bool,
    targeted_queries_file: Path | None = None,
    compatibility_setting: str | None = None,
) -> str:
    from ci.jobs.ci_utils import is_extended_run

    minutes = 60 if is_extended_run() else 30
    envs = [
        f"-e FUZZER_TO_RUN='{'BuzzHouse' if buzzhouse else 'AST Fuzzer'}'",
        f"-e FUZZ_TIME_LIMIT='{minutes}m'",
    ]
    if targeted_queries_file:
        container_queries_file = f"/workspace/{targeted_queries_file.name}"
        envs.append(f"-e TARGETED_QUERIES_FILE='{container_queries_file}'")
    if compatibility_setting:
        envs.append(f"-e FUZZER_COMPATIBILITY='{compatibility_setting}'")

    env_str = " ".join(envs)

    return (
        f"docker run "
        # For sysctl
        "--privileged "
        "--network=host "
        "--tmpfs /tmp/clickhouse:mode=1777 "
        f"--volume={WORKSPACE_PATH}:/workspace "
        f"--volume={cwd}:/repo "
        f"{env_str} "
        "--cap-add syslog --cap-add sys_admin --cap-add=SYS_PTRACE --workdir /repo "
        f"{image} "
        "bash -c './ci/jobs/scripts/fuzzer/run-fuzzer.sh' "
    )


def _collect_targeted_queries(info: Info) -> tuple[list[str], Result]:
    targeter = Targeting(info=info)
    targeter.job_type = Targeting.STATELESS_JOB_TYPE

    # Step 1: changed/new test files in this PR
    changed_tests = targeter.get_changed_tests()
    logging.info(
        "[targeted-fuzzer] Step 1 — changed/new tests (%d): %s",
        len(changed_tests),
        ", ".join(sorted(changed_tests)) or "(none)",
    )

    # Step 2: tests that failed in previous CI runs for this PR
    try:
        previously_failed = targeter.get_previously_failed_tests()
    except Exception as e:
        logging.warning(
            "[targeted-fuzzer] Step 2 — failed to fetch previously-failed tests: %s", e
        )
        previously_failed = []
    logging.info(
        "[targeted-fuzzer] Step 2 — previously failed tests (%d): %s",
        len(previously_failed),
        ", ".join(previously_failed) or "(none)",
    )

    # Step 3: coverage-relevant tests (direct lines, indirect callees, siblings)
    try:
        relevant_tests, relevant_tests_result = targeter.get_most_relevant_tests()
    except Exception as e:
        logging.warning(
            "[targeted-fuzzer] Step 3 — failed to fetch coverage-relevant tests: %s", e
        )
        relevant_tests = []
        relevant_tests_result = Result(
            name="tests found by coverage",
            status=Result.Status.OK,
            info=f"Skipped: {e}",
        )
    logging.info(
        "[targeted-fuzzer] Step 3 — coverage-relevant tests (%d)", len(relevant_tests)
    )

    # Merge all three sets preserving priority order (changed first)
    seen: set = set()
    tests: list = []
    for t in list(changed_tests) + list(previously_failed) + list(relevant_tests):
        if t not in seen:
            seen.add(t)
            tests.append(t)
    logging.info("[targeted-fuzzer] Total unique tests: %d", len(tests))

    stateless_tests_dir = Path(cwd) / "tests/queries/0_stateless"
    available_queries: dict[str, list[str]] = {}

    for query_file in stateless_tests_dir.rglob("*.sql"):
        base_name = query_file.stem
        available_queries.setdefault(base_name, []).append(
            f"/repo/{query_file.relative_to(cwd)}"
        )

    logging.debug(
        "Indexed %d unique SQL query base names from %s",
        len(available_queries),
        stateless_tests_dir,
    )

    targeted_queries: list[str] = []
    seen_queries = set()
    for test in tests:
        base_name = Path(test).stem.rstrip(".")
        matches = available_queries.get(base_name, [])
        if matches:
            logging.debug("  %s -> %s", test, matches)
        else:
            logging.debug("  %s -> no .sql file found (stem: %r)", test, base_name)
        for query_path in matches:
            if query_path not in seen_queries:
                seen_queries.add(query_path)
                targeted_queries.append(query_path)

    if targeted_queries:
        targeted_queries_file = WORKSPACE_PATH / "ci-targeted-queries.txt"
        with open(targeted_queries_file, "w", encoding="utf-8") as f:
            f.write("\n".join(targeted_queries))
        logging.info(
            "Prepared %d targeted queries for AST fuzzer:", len(targeted_queries)
        )
        for qf in targeted_queries:
            logging.info("  %s", qf)
    else:
        logging.info("No targeted queries resolved for AST fuzzer")

    return targeted_queries, relevant_tests_result


def _with_watchdog(selected, watchdog_result):
    """The selected failure row, plus a coexisting watchdog record.

    `_select_failure_result` returns ONE row and the marker outranks the watchdog,
    so when both records exist (a reap watchdog written before the probe loop, then
    a memory-stuck marker) the watchdog would be dropped and its ERROR downgraded
    to the marker's FAIL. Keep it so the harness state is never lost and ERROR
    still outranks FAIL in the derived status. Both the normal and the early-abort
    branch reduce to a single selection, so both need this.
    """
    if selected is None:
        return [] if watchdog_result is None else [watchdog_result]
    if watchdog_result is not None and watchdog_result is not selected:
        return [selected, watchdog_result]
    return [selected]


def _assemble_sub_results(
    is_failed,
    status,
    server_died,
    server_log,
    stderr_log,
    fuzzer_log,
    buzzhouse,
    marker_result,
    watchdog_result,
):
    """The sub-results of a normal (non-early-abort) run, in report order.

    Extracted from `run_fuzz_job` so it can be exercised directly: the branch
    predicate and both attachment guards decide whether a genuine parser finding
    or a watchdog-only `ERROR` survives, and `run_fuzz_job` itself cannot be run
    from the unit suite (it drives docker and the real runner).
    """
    if _should_parse_logs(is_failed, status, server_died):
        # died server - lets fetch failure from log. When the harness itself
        # ended a server it knew was alive (marker, a TEARDOWN-stage watchdog, or a
        # probes-stage exhaustion), tell the parser to ignore the self-inflicted
        # "Signal" line; a reap-stage watchdog killed only client-side processes,
        # so a genuine server signal there must stay reportable (self_killed_server
        # stays False).
        selected = _parse_and_select_failure(
            server_log, stderr_log, fuzzer_log, buzzhouse, marker_result, watchdog_result
        )
        return _with_watchdog(selected, watchdog_result)
    # The parser block was skipped: either status == ERROR (e.g. dmesg OOM), or the
    # server never died (a reap-stage watchdog with a healthy, gracefully-stopped
    # server must not let the parser scrape the normal-termination signal-15 line).
    # Still surface the marker / watchdog as sub-results so the memory-stuck state
    # and the watchdog stage/timings are not lost (they also upload as files).
    return [sub for sub in (marker_result, watchdog_result) if sub is not None]


def run_fuzz_job(check_name: str):
    logging.basicConfig(level=logging.INFO)
    is_targeted = "targeted" in check_name.lower()
    buzzhouse: bool = check_name.lower().startswith("buzzhouse")

    clickhouse_binary = Path(cwd) / "ci/tmp/clickhouse"
    assert clickhouse_binary.exists(), "ClickHouse binary not found"
    clickhouse_binary.chmod(clickhouse_binary.stat().st_mode | 0o111)

    docker_image = DockerImage.get_docker_image(IMAGE_NAME).pull_image()

    WORKSPACE_PATH.mkdir(parents=True, exist_ok=True)

    info = Info()
    extra_results = []
    targeted_queries_file: Path | None = None

    if is_targeted and not buzzhouse:
        targeted_queries, relevant_tests_result = _collect_targeted_queries(info=info)
        extra_results.append(relevant_tests_result)
        if not targeted_queries:
            Result.create_from(
                status=Result.Status.SKIPPED,
                info="No relevant tests found for targeted AST fuzzer",
                results=extra_results,
            ).complete_job()
        targeted_queries_file = WORKSPACE_PATH / "ci-targeted-queries.txt"

    is_old_compatibility = "old_compatibility" in check_name.lower()
    compatibility_setting: str | None = None
    if not buzzhouse:
        if is_old_compatibility:
            # The minimum version is 24.3 because that's when enable_analyzer
            # became enabled by default, and the fuzzer has a readonly constraint
            # on enable_analyzer to avoid wasting cycles on the old interpreter.
            compatibility_setting = "24.3"
        elif is_targeted:
            compatibility_setting = None
        else:
            compatibility_setting = f"{random.randint(24, 27)}.{random.randint(1, 12)}"
        if compatibility_setting:
            logging.info("AST fuzzer compatibility setting: %s", compatibility_setting)
        else:
            logging.info("AST fuzzer compatibility setting is not set")

    # Remove stale per-run classification inputs before the container writes new
    # ones: praktika reuses worktrees (ci/tmp is gitignored, so `git clean -ffd`
    # keeps it), server.log/stderr.log are opened append, and a stale
    # marker/status.tsv/sanitizer.log.* from run N would otherwise misclassify
    # run N+1.
    _clean_stale_run_state()

    run_command = get_run_command(
        docker_image,
        buzzhouse,
        targeted_queries_file=targeted_queries_file,
        compatibility_setting=compatibility_setting,
    )
    logging.info("Going to run %s", run_command)

    is_sanitized = "san" in info.job_name

    changed_files_path = WORKSPACE_PATH / "ci-changed-files.txt"
    with open(changed_files_path, "w") as f:
        changed_files = info.get_changed_files()
        if changed_files is None:
            if info.is_local_run:
                logging.warning(
                    "No changed files available for local run - fuzzing will not be guided by changed test cases"
                )
            changed_files = []
        else:
            logging.info("Found %d changed files to guide fuzzing", len(changed_files))
        f.write("\n".join(changed_files))

    Shell.check(command=run_command, verbose=True)

    # Read the failure markers FIRST -- they drive both classification and the
    # ownership-repair decision below.
    marker_result = _memory_stuck_result()
    watchdog_result = _harness_watchdog_result()

    server_log, fuzzer_log, stderr_log, dmesg_log, fatal_log = JOB_ARTIFACTS
    paths = list(JOB_ARTIFACTS)

    if buzzhouse:
        paths.extend([WORKSPACE_PATH / "fuzzerout.sql", WORKSPACE_PATH / "fuzz.json"])

    # Raw sanitizer reports written via *SAN_OPTIONS=log_path (see run-fuzzer.sh).
    # Their contents are also merged into stderr.log/server.log, but upload the
    # originals too for debugging truncated reports.
    paths.extend(sorted(WORKSPACE_PATH.glob("sanitizer.log.*")))

    # Upload the harness failure markers so investigators see the probe counts,
    # tier, and watchdog stage/timings behind the classification.
    paths.extend(p for p in (MEMORY_STUCK_MARKER, HARNESS_WATCHDOG) if p.exists())

    # Fix file ownership after running docker as root. On a failed (marker or
    # watchdog) run run-fuzzer.sh already `chmod -R a+r`'d the artifacts as root
    # in-container (from its EXIT trap, so even a `set -e` abort is covered), so
    # the unbounded repair container is skipped -- a wedged docker daemon there
    # would recreate the very no-artifact hang this fix removes.
    #
    # The skip is conditioned on the artifacts being ACTUALLY readable, not on
    # the marker alone: if the container never reached its trap (SIGKILL from the
    # kernel OOM killer, docker teardown), the marker can exist while the files
    # are still root-owned 0640, and skipping would silently drop exactly the
    # evidence this change exists to preserve.
    #
    # The cores are checked too, but deliberately NOT added to `paths`: `paths` is
    # the upload list, and cores may only leave the runner compressed+encrypted by
    # collect_cores. An unreadable core makes that collector raise, which aborts
    # the job after classification and discards the whole Result.
    unreadable = _unreadable_artifacts(paths + _collectable_cores())
    if (marker_result is None and watchdog_result is None) or unreadable:
        if unreadable:
            logging.info(
                "Fuzzer: %d artifact(s) not host-readable (%s); running ownership "
                "repair despite the failure marker/watchdog",
                len(unreadable),
                ", ".join(p.name for p in unreadable),
            )
        else:
            logging.info("Fuzzer: Fixing file ownership after running docker as root")
        Utils.fix_ownership_after_docker(cwd, docker_image)
    else:
        logging.info(
            "Fuzzer: failure marker/watchdog present and artifacts are host-readable; "
            "skipping ownership repair"
        )

    server_died = False
    server_exit_code = 0
    fuzzer_exit_code = 0
    try:
        server_died, server_exit_code, fuzzer_exit_code = _read_fuzzer_status(
            WORKSPACE_PATH / "status.tsv"
        )
    except Exception as e:
        # Missing/empty status.tsv -> runner aborted before reporting (server
        # start failure, harness error, or infra); malformed status.tsv ->
        # harness bug. _format_status_error inlines the log tails so the abort
        # cause is visible instead of an opaque FileNotFoundError traceback.
        # Attach available artifacts (incl. sanitizer.log.*) so nothing is lost.
        error_info = _format_status_error(e, paths)
        # If the runner never wrote status.tsv but DID leave a memory-stuck marker
        # or a watchdog record, that is the authoritative classification -- attach
        # it here, because this path calls complete_job() (which sys.exits) and the
        # normal classification below is unreachable.
        #
        # Run the parser too, and keep the same parser-first precedence: it reads
        # only the logs, so a genuine sanitizer / logical-error / segfault finding
        # survives a missing status.tsv, and without this a late `set -e` abort
        # after the marker write would relabel a real crash as the generic
        # memory-stuck row. _select_failure_result keeps the marker as the floor,
        # so a parser no-match still reports the marker/watchdog.
        # Parse on EVERY early abort, not only when a marker/watchdog exists. The
        # server can log a sanitizer report or a logical error and die during
        # startup -- before the probe loop that writes a marker has even been
        # reached, and long before status.tsv -- and `set -e` then aborts here (the
        # PID read and `kill -0` right after the startup wait are the concrete
        # sites). Gating the parser on a marker discarded the stable finding name
        # and its reproduction files for exactly that class, reporting a real crash
        # as a generic harness error.
        # "The harness had already stopped the server" is true for a classified run
        # (status.tsv is written after the graceful stop) AND for any run that got as
        # far as the shutdown phase, which run-fuzzer.sh records before stopping.
        stopped_by_harness = _early_abort_stopped_by_harness(
            marker_result, watchdog_result
        )
        sub_results = [r for r in (marker_result, watchdog_result) if r is not None]
        selected = _parse_and_select_failure(
            server_log,
            stderr_log,
            fuzzer_log,
            buzzhouse,
            marker_result,
            watchdog_result,
            # A startup failure that dies before the shutdown phase has no witness
            # and no stop ever ran, so claiming one would suppress a genuine server
            # signal -- the one line such a crash may consist of. Everything from the
            # shutdown phase onwards does have a witness (see above), including a
            # reap-only run whose server stays healthy and whose stage checks
            # therefore correctly do not fire.
            server_stopped_by_harness=stopped_by_harness,
        )
        if selected is not None:
            # With a marker present the parser outranks it (a late abort after the
            # marker write must not relabel a real crash as the memory-stuck row);
            # _select_failure_result keeps the marker as the floor, so a no-match
            # still reports it. Without one, a genuine finding is all there is.
            sub_results = _with_watchdog(selected, watchdog_result)
        early_result = Result.create_from(
            status=_early_abort_status(e, sub_results),
            info=error_info,
            results=sub_results,
        )
        for file in paths:
            if file.exists() and file.stat().st_size > 0:
                early_result.set_files(file)
        # Persist BEFORE collecting cores, for the same reason as the normal path:
        # everything above is decided, and collection is the one remaining step that
        # can spend real time or be cut short by the external cancellation. Cores are
        # collected here at all because this branch exits the job, so the collection
        # at the end of the normal path is unreachable -- a late `set -e` abort (the
        # failing `status.tsv` write is the concrete case) can leave a genuine crash
        # core, and `paths` cannot carry it since a core may only leave the runner
        # encrypted.
        early_result.dump()
        cores, core_note = _collect_cores_or_note()
        if cores:
            early_result.set_files(cores)
        if core_note:
            early_result.info = f"{early_result.info}\n{core_note}"
        early_result.complete_job()

    # parse runner script exit status
    status = Result.Status.FAIL
    info = []
    is_failed = True
    if server_died:
        # Server died - status will be determined after OOM checks
        is_failed = True
    elif fuzzer_exit_code in (0, 137, 143):
        # normal exit with timeout or OOM kill
        is_failed = False
        status = Result.Status.OK
        if fuzzer_exit_code == 0:
            info.append("Fuzzer exited with success")
        elif fuzzer_exit_code == 137:
            info.append("Fuzzer killed")
        else:
            info.append("Fuzzer exited with timeout")
        info.append("\n")
    elif fuzzer_exit_code in (227,):
        # BuzzHouse exception, it means a query oracle failed, or
        # an unwanted exception was found
        status = Result.Status.ERROR
        error_info = (
            Shell.get_output(
                f"rg --text -o 'DB::Exception: Found disallowed error code.*' {fuzzer_log}"
            )
            or "BuzzHouse fuzzer exception not found, fuzzer issue?"
        )
        info.append(f"ERROR: {error_info}")
    else:
        status = Result.Status.ERROR
        # The server was alive, but the fuzzer returned some error. This might
        # be some client-side error detected by fuzzing, or a problem in the
        # fuzzer itself. Don't grep the server log in this case, because we will
        # find a message about normal server termination (Received signal 15),
        # which is confusing.
        info.append("Client failure (see logs)")
        info.append("---\nFuzzer log (last 200 lines):")
        info.extend(
            Shell.get_output(f"tail -n200 {fuzzer_log}", verbose=False).splitlines()
        )

    if is_failed:
        if is_sanitized:
            sanitizer_oom = Shell.get_output(
                f"rg --text 'Sanitizer:? (out-of-memory|out of memory|failed to allocate)|Child process was terminated by signal 9' {server_log}"
            )
            # Sanitizer shadow memory is invisible to the server's memory tracker,
            # so the kernel OOM killer may SIGKILL the server before any limit
            # fires. It may also kill the watchdog, losing the "terminated by
            # signal 9" message in the server log. A SIGKILLed server (exit 137)
            # with no sanitizer report is an OOM, not a bug.
            has_sanitizer_report = any(WORKSPACE_PATH.glob("sanitizer.log.*"))
            kernel_oom_kill = (
                server_died and server_exit_code == 137 and not has_sanitizer_report
            )
            # Both leniency paths are unsafe when the harness itself killed the
            # server: the "Child process was terminated by signal 9" line
            # (sanitizer_oom) is emitted by the ClickHouse watchdog for OUR kill,
            # and the benign "Sanitizer ... failed to allocate" warning (recoverable
            # under allocator_may_return_null=1) coexists with an alive, stuck
            # server. A marker/watchdog run must not be forgiven.
            if _oom_leniency_granted(
                sanitizer_oom, kernel_oom_kill, marker_result, watchdog_result
            ):
                print("Sanitizer OOM")
                if sanitizer_oom:
                    info.append("WARNING: Sanitizer OOM - test considered passed")
                else:
                    info.append(
                        "WARNING: Server was killed by the kernel OOM killer "
                        "(sanitizer build) - test considered passed"
                    )
                status = Result.Status.OK
                is_failed = False
        else:
            # Check for OOM in dmesg for non-sanitized builds
            if Shell.check(f"dmesg > {dmesg_log}", verbose=True):
                if Shell.check(
                    f"cat {dmesg_log} | grep -a -e 'Out of memory: Killed process' -e 'oom_reaper: reaped process' -e 'oom-kill:constraint=CONSTRAINT_NONE' | tee /dev/stderr | grep -q .",
                    verbose=True,
                ):
                    info.append("ERROR: OOM in dmesg")
                    status = Result.Status.ERROR
            else:
                print("WARNING: dmesg not enabled")

    # A memory-stuck marker or a watchdog record is an abnormal harness state that
    # must never report OK, even when the exit code lands in the benign set.
    status, is_failed = _force_fail_for_markers(
        status, is_failed, marker_result, watchdog_result
    )

    results = _assemble_sub_results(
        is_failed,
        status,
        server_died,
        server_log,
        stderr_log,
        fuzzer_log,
        buzzhouse,
        marker_result,
        watchdog_result,
    )

    result = Result.create_from(
        results=extra_results + results,
        status=_top_level_status(status, results),
        info=info,
    )

    if is_failed:
        # generate fatal log
        Shell.check(f"rg --text '\\s<Fatal>\\s' {server_log} > {fatal_log}")
        for file in paths:
            if file.exists() and file.stat().st_size > 0:
                result.set_files(file)
        # PERSIST BEFORE COLLECTING CORES. The classification and every log are
        # complete at this point, and core collection is the one remaining step that
        # can consume real time (compressing and encrypting up to three cores) or be
        # cut short by the external job cancellation. Writing the result first means
        # the worst case is a report without its core, never the artifact-less
        # cancellation this whole change exists to remove -- and it makes the
        # collection deadline a matter of core-vs-no-core rather than
        # report-vs-no-report. set_files keeps the persisted file up to date
        # afterwards (Result._dump_if_persisted).
        result.dump()
        cores, core_note = _collect_cores_or_note()
        if cores:
            result.set_files(cores)
        if core_note:
            result.info = f"{result.info}\n{core_note}" if result.info else core_note

    result.complete_job()


if __name__ == "__main__":
    check_name = sys.argv[1] if len(sys.argv) > 1 else os.getenv("CHECK_NAME")
    assert (
        check_name
    ), "Check name must be provided as an input arg or in CHECK_NAME env"

    run_fuzz_job(check_name)
