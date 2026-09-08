#!/usr/bin/env python3
import argparse
import logging
import os
import random
import re
import shutil
import traceback
from pathlib import Path

from ci.jobs.scripts.clickhouse_service import ClickHouseService
from ci.jobs.scripts.find_tests import Targeting
from ci.jobs.scripts.docker_image import DockerImage
from ci.jobs.scripts.log_parser import (
    SANITIZER_OOM_PATTERN,
    SANITIZER_OOM_REPORT_PATTERN,
    FuzzerLogParser,
)
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils

IMAGE_NAME = "clickhouse/fuzzer"

# Maximum number of reproduce commands to display inline before writing to file
MAX_INLINE_REPRODUCE_COMMANDS = 20

# The runner agent lives on the host, outside this container, so it is only safe if the container cannot take the whole box.
RUNNER_MEMORY_RESERVE = 8 * 1024**3

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


# A server-transmitted exception is printed by the client prefixed with
# "Received from <host>." A client-side 241 raised under
# --max_memory_usage_in_client prints "Code: 241. DB::Exception: ..." with no
# such prefix, so this signature keeps only genuine server-survived limits.
# The server error text can appear as the enum "(MEMORY_LIMIT_EXCEEDED)" or the
# prose "... memory limit exceeded" (the two are printed on separate lines: the
# "Received from" line carries the prose message, the enum trails on its own
# line), so match either form -- both are server-origin because of the prefix.
SERVER_MLE_SIGNATURE = r"Received from.*(?:MEMORY_LIMIT_EXCEEDED|memory limit exceeded)"

# A client-origin 241 line: "Code: 241" with NO "Received from" on the same line.
# clickhouse-client raises 241 for its own --max_memory_usage_in_client cap (see
# tests/queries/0_stateless/02003_memory_limit_in_client.sh) and prints it as
# "Code: 241. DB::Exception: ..." with no transmission prefix, whereas a server-
# transmitted 241 always carries "Received from <host>" on that line. Used only
# in the no-marker fallback below to reject a client/harness 241 that a server
# limit recovered from earlier in the same read tail must not mask.
CLIENT_241_SIGNATURE = re.compile(r"^(?!.*Received from).*Code: 241\b", re.MULTILINE)

# The client prints "Fuzzing step <n> out of <m>" to stderr before each fuzz
# step (programs/client/FuzzLoop.cpp), so the text after the LAST such marker is
# the terminal query block -- the only step whose outcome sets the exit code.
# This anchors on stderr, not the "Dump of fuzzed AST:" line: that dump is
# printed to stdout, which is block-buffered when redirected to a file, so
# run-fuzzer.sh's "> fuzzer.log 2>&1" flushes the terminal step's dump at process
# exit -- AFTER its own (unbuffered stderr) exception -- and a dump-based anchor
# would land on that trailing re-dump, past the evidence.
# The AST fuzzer swallows query-side server MEMORY_LIMIT_EXCEEDED and keeps going
# (Client::processASTFuzzerStep returns success), so a 30-minute fuzzer.log
# accumulates many recovered "Received from ... memory limit exceeded" lines that
# did NOT terminate the run; each sits in its own (non-terminal) step block. A
# fixed line/byte tail can still hold such a swallowed limit together with a
# later client-side 241 when only a few stack frames separate the two steps, so
# anchor on the terminal step block instead of a fixed-size window.
STEP_MARKER = re.compile(r"^Fuzzing step \d+ out of \d+$", re.MULTILINE)

# Bounded read for the terminal block. A single fuzz step's dump plus its
# transmitted exception is small; 256 KiB comfortably covers the last marker and
# everything after it without loading a 30-minute log. Everything within a
# tail-of-file window is by construction after the last marker, so even when the
# terminal step's output is larger than this bound the window stays inside the
# terminal block and never leaks an earlier step's swallowed limit.
TERMINAL_BLOCK_MAX_BYTES = 262144


def _terminal_query_block(fuzzer_log: Path) -> str:
    """Text of fuzzer.log after the last 'Fuzzing step <n> out of <m>' marker.

    Returns the read tail as-is when no marker is present (the run exited before
    any AST fuzz step, e.g. a startup/handshake error, or BuzzHouse which does
    not print step markers)."""
    try:
        size = fuzzer_log.stat().st_size
        if size == 0:
            return ""
        with open(fuzzer_log, "rb") as fh:
            if size > TERMINAL_BLOCK_MAX_BYTES:
                fh.seek(-TERMINAL_BLOCK_MAX_BYTES, os.SEEK_END)
            text = fh.read().decode("utf-8", errors="replace")
    except OSError:
        return ""
    matches = list(STEP_MARKER.finditer(text))
    return text if not matches else text[matches[-1].start():]


def _fuzzer_log_terminal_block_has_server_mle(fuzzer_log: Path) -> bool:
    """True when a server-origin MEMORY_LIMIT_EXCEEDED explains the terminal 241.

    The terminal block is anchored on the last 'Fuzzing step' marker (or the
    whole read tail for a startup/handshake 241 or a BuzzHouse run, which print
    no marker). It can still hold a server limit the run recovered from earlier
    followed by a later client/harness 241 -- a recovered query limit then a
    client-side reconnect/handshake 241 within a step, or the same across a
    markerless tail. Treat it as benign only when no client-origin 241 line (a
    "Code: 241" line with no "Received from" prefix) appears AFTER the last
    server-origin MLE: that later 241 is the real exit cause and must surface,
    while an earlier recovered client 241 does not veto a genuinely terminal
    server limit."""
    block = _terminal_query_block(fuzzer_log)
    server_mles = [m.start() for m in re.finditer(SERVER_MLE_SIGNATURE, block)]
    if not server_mles:
        return False
    return not any(
        m.start() > server_mles[-1] for m in CLIENT_241_SIGNATURE.finditer(block)
    )


# BUZZHOUSE_ORACLE in Common/ErrorCodes.cpp. main() returns the error code but the OS
# keeps only its low byte, so 1018 reaches the job as exit 249. Oracle findings use their
# own code precisely so they are never confused with a BUZZHOUSE (739) config error.
BUZZHOUSE_ORACLE_ERROR_CODE = 1018
BUZZHOUSE_ORACLE_EXIT_CODE = BUZZHOUSE_ORACLE_ERROR_CODE & 0xFF

# BUZZHOUSE (739) truncated the same way: the fuzzer found a disallowed error code, or bailed
# out on its own.
BUZZHOUSE_EXCEPTION_ERROR_CODE = 739
BUZZHOUSE_EXCEPTION_EXIT_CODE = BUZZHOUSE_EXCEPTION_ERROR_CODE & 0xFF

# What the AST fuzzer client passes to `_exit` after its oracle finds a wrong result
# (programs/client/FuzzLoop.cpp). Not a dedicated code: LOGICAL_ERROR is also 49 and
# `mainEntryClickHouseClient` returns an exception code verbatim, so the marker block the
# client writes just before exiting is what tells the two apart.
AST_FUZZER_ORACLE_EXIT_CODE = 49
AST_FUZZER_ORACLE_MARKER = "AST FUZZER ORACLE MISMATCH"

# Genuine (non-OOM) failure signals that veto the OOM-is-success downgrade, so a crash on
# one node isn't hidden by a benign OOM on another. `is_memory_limit_exceeded` is excluded
# (surviving the memory cap is itself benign), and bare signal numbers are excluded too (the
# "Signal" pattern already requires the fatal handler's "(from thread N)" prefix). Bare
# "<Fatal>" is included so a crash type with no signature here yet still vetoes;
# `benign_pattern` below strips the two expected "<Fatal>" lines (OOM report, end-of-run
# SIGKILL) back out.
SANITIZER_NON_OOM_PATTERN = "|".join(
    [
        "AddressSanitizer|UndefinedBehaviorSanitizer|ThreadSanitizer"
        "|MemorySanitizer|SIGSEGV|SIGABRT",
        "<Fatal>",
        *(
            pattern
            for _, flag_name, pattern in FuzzerLogParser.ERROR_PATTERNS
            if flag_name != "is_memory_limit_exceeded"
        ),
    ]
)


def _is_benign_memory_limit(
    server_died: bool, fuzzer_exit_code: int, terminal_block_has_server_mle: bool
) -> bool:
    """True when the fuzzer exited only because the SERVER hit its memory cap.

    A fuzzed query pushes the server over its memory limit; the memory tracker
    rejects the allocation with Code 241 (MEMORY_LIMIT_EXCEEDED) and the server
    stays up (server_died=0). The server transmits that exception to the client,
    which prints it prefixed with "Received from <host>. ... (MEMORY_LIMIT_
    EXCEEDED)" and exits with the server error code (241). This is the tracker
    working as intended, not a crash or a finding -- run-fuzzer.sh's liveness
    loop already treats a 241 as "alive, busy".

    The evidence must be server-origin (SERVER_MLE_SIGNATURE) AND come from the
    TERMINAL query block (after the last 'Fuzzing step' marker). clickhouse-client
    itself can raise 241 under --max_memory_usage_in_client (a client-side cap;
    see tests/queries/0_stateless/02003_memory_limit_in_client.sh) and
    mainEntryClickHouseClient returns that code verbatim -- such a client/harness
    241 has no "Received from" prefix. And because the fuzzer swallows earlier
    server limits and keeps running, only a server MLE in the terminal block
    actually explains the exit; a swallowed one from an earlier step must not
    mask a terminal client/harness 241, or a real regression would be missed.
    """
    return (
        not server_died
        and fuzzer_exit_code == 241
        and terminal_block_has_server_mle
    )


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
    enable_oracle: bool = False,
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
    if enable_oracle:
        envs.append("-e FUZZER_ORACLE_ENABLED=1")

    env_str = " ".join(envs)

    return (
        f"docker run "
        # For sysctl
        "--privileged "
        "--network=host "
        f"--memory={Utils.physical_memory() - RUNNER_MEMORY_RESERVE} "
        "--tmpfs /tmp/clickhouse:mode=1777 "
        f"--volume={WORKSPACE_PATH}:/workspace "
        f"--volume={cwd}:/repo "
        f"{env_str} "
        "--cap-add syslog --cap-add sys_admin --cap-add=SYS_PTRACE --workdir /repo "
        f"{image} "
        "bash -c './ci/jobs/scripts/fuzzer/run-fuzzer.sh' "
    )


def _classify_sanitizer_oom(
    server_logs: list[Path],
    stderr_logs: list[Path],
    server_died: bool,
    server_exit_code: int,
    workspace_path: Path,
    error_logs: list[Path] | None = None,
) -> tuple[bool, list[str]]:
    """Decide whether a failed sanitizer run is an OOM (i.e. should pass).

    A sanitizer OOM report (e.g. "AddressSanitizer: out-of-memory") or a kernel
    SIGKILL of the server (exit 137 with no sanitizer report written) is treated
    as OOM, not a bug. It is only downgraded to success when no log of the run also
    shows a genuine non-OOM failure signal, so a run that hits both an OOM and a
    real crash still fails. Each node is judged for an OOM by its server.log,
    stderr.log and error log together: the AST/Buzz runner merges sanitizer output
    into server.log, but the Dolor cluster does not - there the report lands only in
    stderr.log, while a `<Fatal>` or `Logical error` may reach only
    clickhouse-server.err.log. `server_logs` is the full list the parser gets, the
    per-node primaries first and anything rotated appended after.
    Returns (is_oom_success, warning_messages).
    """
    # Only a real sanitizer OOM report proves an OOM. The expected SIGKILL line is logged by
    # a healthy teardown too, so it may not stand in for one here; a kernel OOM that leaves
    # only that line is recognised by exit code 137 on the `kernel_oom_kill` path below.
    oom_pattern = SANITIZER_OOM_REPORT_PATTERN
    # Wider than `oom_pattern` on purpose: benign lines to drop from the non-OOM scan. The
    # expected SIGKILL must not veto a genuine OOM either.
    benign_pattern = SANITIZER_OOM_PATTERN
    non_oom_pattern = SANITIZER_NON_OOM_PATTERN
    primary_server_logs = server_logs[: len(stderr_logs)]
    oom_nodes = []
    all_logs = []
    # Which node OOMed is per-node: only a report in that node's current logs
    # (server.log, stderr.log, error log) downgrades it, and the warning names it.
    for i, server_log in enumerate(primary_server_logs):
        stderr_log = stderr_logs[i] if i < len(stderr_logs) else None
        error_log = error_logs[i] if error_logs and i < len(error_logs) else None
        node_logs = " ".join(
            str(log)
            for log in (server_log, stderr_log, error_log)
            if log is not None and Path(log).exists()
        )
        if not node_logs:
            continue
        all_logs.append(node_logs)
        if Shell.get_output(f"rg -z --text '{oom_pattern}' {node_logs}"):
            print(f"Sanitizer OOM on server {i}")
            oom_nodes.append(i)
    # The veto is not per node: any genuine failure anywhere in the run blocks the
    # downgrade, so this scan must reach every log of it, the rotated ones included. A
    # crash can rotate out of server.log while a later benign OOM stays in it, and the
    # workspace is wiped before the run, so nothing here predates it. Scan the OOM-marked
    # nodes too: one node can hit both an OOM and a genuine crash, and the crash must not
    # be masked. Drop the benign lines themselves (an OOM report matches non_oom_pattern
    # via "AddressSanitizer" etc., and the expected SIGKILL via the "Signal" pattern) so
    # neither is miscounted as a failure. `-z` because rotation gzips all but the newest.
    all_logs.extend(
        str(log) for log in server_logs[len(stderr_logs) :] if Path(log).exists()
    )
    non_oom_failure_found = bool(
        all_logs
        and Shell.get_output(
            f"rg -z --text '{non_oom_pattern}' {' '.join(all_logs)}"
            f" | rg --text -v '{benign_pattern}'"
        )
    )
    # Sanitizer shadow memory is invisible to the server's memory tracker, so the
    # kernel OOM killer may SIGKILL the server (exit 137) before any limit fires.
    # It may also kill the watchdog, losing the "terminated by signal 9" message
    # in the server log. A SIGKILLed server with no sanitizer report is an OOM.
    kernel_oom_kill = (
        server_died
        and server_exit_code == 137
        and not any(Path(workspace_path).glob("sanitizer.log.*"))
    )
    if non_oom_failure_found or not (oom_nodes or kernel_oom_kill):
        return False, []
    messages = [
        f"WARNING: Sanitizer OOM on server {i} - test considered passed"
        for i in oom_nodes
    ]
    if kernel_oom_kill and not oom_nodes:
        messages.append(
            "WARNING: Server was killed by the kernel OOM killer "
            "(sanitizer build) - test considered passed"
        )
    return True, messages


def analyze_job_logs(
    paths: list[Path],
    server_died: bool,
    server_exit_code: int,
    fuzzer_exit_code: int,
    is_sanitized: bool,
    fuzzer_out: Path,
    fuzzer_log: Path,
    dmesg_log: Path,
    server_logs: list[Path],
    stderr_logs: list[Path],
    fatal_logs: list[Path],
    extra_results: list[Result],
    sw: Utils.Stopwatch,
    server_fuzzer: bool,
    error_logs: list[Path] | None = None,
    buzzhouse: bool = False,
) -> Result:
    """`error_logs`, when given, holds the current clickhouse-server.err.log of each node,
    in the same per-node order as `stderr_logs`, so the OOM classifier can name the node a
    failure that reached only the error log belongs to. Callers that merge those into
    `server_logs` for the parser must still pass them here: only the
    `server_logs[:len(stderr_logs)]` slice is treated per node. Anything appended past it
    is still read - it is where the rotated logs go, and they can veto the OOM downgrade -
    but it is not attributed to a node."""
    # parse runner script exit status
    status = Result.Status.FAIL
    info = []
    is_failed = True
    # A wrong-result finding, not a crash: it must skip the OOM checks and the crash log
    # parser below. The exit code alone cannot prove one - it is truncated to 8 bits, so
    # 249, 505 and 761 all look like BUZZHOUSE_ORACLE (1018) - hence the log marker too.
    # Only the tail: BuzzHouse exits on the oracle error, so the one that ended the run is
    # at the end of the log, and an older match is from a step that already finished.
    oracle_error = (
        Shell.get_output(
            f"tail -n1000 {fuzzer_log}"
            f" | rg --text -o 'Code: {BUZZHOUSE_ORACLE_ERROR_CODE}[.].*'"
            " | tail -n1"
        ).strip()
        if fuzzer_exit_code == BUZZHOUSE_ORACLE_EXIT_CODE
        else ""
    )
    oracle_finding = not server_died and bool(oracle_error)
    # Same reasoning for the other two client-side findings, and the same reason the marker
    # is part of the contract rather than mere evidence: exit 49 is equally LOGICAL_ERROR,
    # and a BuzzHouse exit 227 is either a disallowed error code or the fuzzer giving up.
    # Without the marker line neither is a finding, and the generic branch below is right.
    ast_oracle_error = (
        Shell.get_output(f"rg --text -A 30 '{AST_FUZZER_ORACLE_MARKER}' {fuzzer_log}")
        if fuzzer_exit_code == AST_FUZZER_ORACLE_EXIT_CODE
        and not server_died
        and not buzzhouse
        and not server_fuzzer
        else ""
    )
    buzzhouse_error = (
        Shell.get_output(
            f"rg --text -o 'DB::Exception: Found disallowed error code.*' {fuzzer_log}"
        )
        if fuzzer_exit_code == BUZZHOUSE_EXCEPTION_EXIT_CODE and not server_died
        else ""
    )
    # A finding the client reported and left proof of in its log. A sanitizer OOM elsewhere in
    # the run explains none of them, so all three skip the OOM downgrade below - not only
    # `oracle_finding`, which would otherwise let an OOM rewrite an already-classified
    # wrong-result or disallowed-error finding to OK.
    client_finding = oracle_finding or bool(ast_oracle_error) or bool(buzzhouse_error)
    if server_died:
        # Server died - status will be determined after OOM checks
        is_failed = True
    elif fuzzer_exit_code in (
        (-9, -15, -2, 0, 32, 130, 137, 143, 210) if server_fuzzer else (0, 137, 143)
    ):
        # normal exit with timeout or OOM kill
        is_failed = False
        status = Result.Status.OK
        messages = {
            0: "Fuzzer exited with success",
            -2: "Fuzzer killed with SIGINT",
            -9: "Fuzzer killed with SIGKILL",
            -15: "Fuzzer killed with SIGTERM",
            32: "Fuzzer exited after ATTEMPT_TO_READ_AFTER_EOF error",
            130: "Fuzzer killed with SIGINT",
            137: "Fuzzer killed with SIGKILL",
            143: "Fuzzer killed with SIGTERM",
            210: "Fuzzer exited with network timeout",
        }
        if fuzzer_exit_code in messages:
            info.append(messages[fuzzer_exit_code])
        else:
            info.append("Fuzzer exited with timeout")
        info.append("\n")
    elif _is_benign_memory_limit(
        server_died,
        fuzzer_exit_code,
        _fuzzer_log_terminal_block_has_server_mle(fuzzer_log),
    ):
        # Server hit its memory cap on a fuzzed query but stayed alive; see
        # _is_benign_memory_limit. Not a crash or a finding.
        is_failed = False
        status = Result.Status.OK
        info.append("Server hit its memory limit (Code 241) but stayed alive")
        info.append("\n")
    elif oracle_finding:
        # BuzzHouse caught the server misbehaving: an oracle's two queries that must
        # agree returned different results, or the health check found errors in the
        # system tables. `oracle_error` is the matched log line, kept verbatim.
        status = Result.Status.FAIL
        info.append(f"FAIL: {oracle_error}")
    elif fuzzer_exit_code == BUZZHOUSE_EXCEPTION_EXIT_CODE:
        # BuzzHouse exception: an unwanted exception was found, or the fuzzer
        # itself failed. Oracle failures have their own exit code above.
        status = Result.Status.ERROR
        info.append(
            f"ERROR: {buzzhouse_error or 'BuzzHouse fuzzer exception not found, fuzzer issue?'}"
        )
    elif ast_oracle_error:
        # AST fuzzer client called _exit(49) after the server-side oracle
        # reported a wrong-result mismatch. The fuzzer log contains a clearly
        # delimited "AST FUZZER ORACLE MISMATCH (fatal)" block with the
        # reproducer query and the server-side oracle output.
        status = Result.Status.ERROR
        info.append(f"ERROR: AST fuzzer oracle mismatch\n{ast_oracle_error}")
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

    # server_logs = primary logs (one per node) + rotated logs appended after.
    # stderr_logs has exactly one entry per node, so slicing by its length
    # isolates the primary logs, which is what per-node semantics need
    # (the fatal log of a node below; the OOM classifier slices it likewise).
    primary_server_logs = server_logs[: len(stderr_logs)]

    if is_failed and not client_finding:
        if is_sanitized:
            is_oom_success, oom_messages = _classify_sanitizer_oom(
                server_logs,
                stderr_logs,
                server_died,
                server_exit_code,
                WORKSPACE_PATH,
                error_logs=error_logs,
            )
            if is_oom_success:
                info.extend(oom_messages)
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

    results = []
    if oracle_finding:
        # A named row, so the report shows what failed instead of only job-level info,
        # and CIDB gets a stable test name to aggregate on. The verbatim line is the info.
        results.append(
            Result(
                name="BuzzHouse oracle failure",
                info=oracle_error,
                status=Result.Status.FAIL,
            )
        )
    if is_failed and status != Result.Status.ERROR and not client_finding:
        # died server - lets fetch failure from log
        fuzzer_log_parser = FuzzerLogParser(
            server_logs=server_logs,
            stderr_logs=stderr_logs,
            fuzzer_log=fuzzer_out,
        )
        # A genuine match first, same as the OOM classifier above: `server_logs` now
        # carries Dolor's rotated logs too, and a sanitizer OOM report that lives only in
        # one is exactly what that classifier already refuses to call current-log
        # evidence (see its docstring - it may belong to a restart that already
        # finished). Naming it here as a plain FAIL would make this function return a
        # specific, non-reclassifiable verdict, and the Dolor wrapper's own rotated-log
        # judgement (`_classify_rotated_logs`) only ever runs when this function returns
        # OK. So an expected-only match is named only to check what it is, not to report
        # a failure from it.
        parsed_name, parsed_info, files = fuzzer_log_parser.parse_failure()
        if parsed_name == FuzzerLogParser.UNKNOWN_ERROR:
            # `UNKNOWN_ERROR` only says no `ERROR_PATTERNS` entry matched, not that the
            # expected lines are the whole story: a `<Fatal>` the parser cannot classify
            # gives the same verdict. Downgrading on an OOM found elsewhere would drop it,
            # so the flip below is gated on there being no such evidence left.
            unnamed_fatals = fuzzer_log_parser.find_unnamed_fatals()
            expected_name, expected_info, expected_files = (
                fuzzer_log_parser.parse_failure(allow_expected_only=True)
            )
            if unnamed_fatals:
                parsed_info += "Unclassified fatal:\n" + "\n".join(unnamed_fatals) + "\n"
            elif expected_name != FuzzerLogParser.UNKNOWN_ERROR and re.search(
                SANITIZER_OOM_REPORT_PATTERN, expected_info
            ):
                info.append(
                    f"WARNING: {expected_name} - only found in a log this function does "
                    "not treat as current-log evidence - test considered passed"
                )
                status = Result.Status.OK
                is_failed = False
            else:
                parsed_name, parsed_info, files = (
                    expected_name,
                    expected_info,
                    expected_files,
                )

        if is_failed and parsed_name:
            results.append(
                Result(
                    name=parsed_name,
                    info=parsed_info,
                    status=Result.Status.FAIL,
                    files=files,
                )
            )

    result = Result.create_from(
        results=extra_results + results,
        status=status if not results else None,
        info=info,
        stopwatch=sw,
    )

    if is_failed:
        # generate fatal log
        for server_log, fatal_log in zip(primary_server_logs, fatal_logs):
            if not Shell.check(f"rg --text '\\s<Fatal>\\s' {server_log} > {fatal_log}"):
                Path(fatal_log).unlink(missing_ok=True)

        # Encrypt and attach any core dumps found under WORKSPACE_PATH. Without this
        # step the report carries only the logs and the e2e test (ci/tests/test_e2e.py)
        # fails because no `.zst.enc` / `.rsa` artifact is produced for cores.
        result.set_files(ClickHouseService.collect_cores(WORKSPACE_PATH))

    # Attach logs whenever the fuzzer did not finish cleanly. A clean finish is
    # exit code 0; any non-zero exit (real failure, oracle mismatch, SIGTERM /
    # SIGKILL from the FUZZ_TIME_LIMIT timeout wrapper) is informative enough
    # that we want the artifacts uploaded — otherwise timeouts look like silent
    # passes with no logs to diagnose them from. Not for the Dolor wrapper: it
    # ends every run with a kill and attaches artifacts itself when it reports.
    if is_failed or (not server_fuzzer and fuzzer_exit_code != 0):
        for file in paths:
            if file.exists() and file.stat().st_size > 0:
                result.set_files(file)

    return result


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

    targeted_queries: list[str] = []
    seen_queries = set()
    for test in tests:
        # Resolves the rendered names CI reports for templates (`<name>.gen` from failures,
        # `<name>.gen.sql` from coverage) back to the `.sql.j2` source. Shell/Python/expect
        # tests resolve too, but carry no SQL corpus to fuzz.
        source_file = Targeting.functional_test_source_file(test)
        if source_file is None or not source_file.endswith((".sql", ".sql.j2")):
            logging.debug("  %s -> no SQL source (resolved: %r)", test, source_file)
            continue

        # `run-fuzzer.sh` renders every template to `<name>.gen.sql` before the fuzzer starts,
        # so target the rendered file - a `.sql.j2` is Jinja, not SQL.
        query_file = stateless_tests_dir / re.sub(
            r"\.sql\.j2$", ".gen.sql", source_file
        )
        query_path = f"/repo/{query_file.relative_to(cwd)}"
        logging.debug("  %s -> %s", test, query_path)
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


def run_fuzz_job(check_name: str):
    sw = Utils.Stopwatch()
    logging.basicConfig(level=logging.INFO)
    is_targeted = "targeted" in check_name.lower()
    is_oracle = "oracle" in check_name.lower()
    buzzhouse: bool = check_name.lower().startswith("buzzhouse")

    clickhouse_binary = Path(cwd) / "ci/tmp/clickhouse"
    assert clickhouse_binary.exists(), "ClickHouse binary not found"
    clickhouse_binary.chmod(clickhouse_binary.stat().st_mode | 0o111)

    docker_image = DockerImage.get_docker_image(IMAGE_NAME).pull_image()

    shutil.rmtree(WORKSPACE_PATH, ignore_errors=True)
    WORKSPACE_PATH.mkdir(parents=True, exist_ok=True)

    if buzzhouse:
        # After the wipe, never before it: `run-fuzzer.sh` reads `--buzz-house-config=fuzz.json`
        # from the workspace, so a config written by the caller would be deleted here. Imported
        # locally because `buzzhouse_job` imports this module.
        from ci.jobs.buzzhouse_job import generate_buzz_config

        generate_buzz_config(WORKSPACE_PATH, log_path="/workspace/fuzzerout.sql")

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
                stopwatch=sw,
            ).complete_job()
            return
        targeted_queries_file = WORKSPACE_PATH / "ci-targeted-queries.txt"

    is_old_compatibility = "old_compatibility" in check_name.lower()
    compatibility_setting: str | None = None
    if not buzzhouse:
        if is_old_compatibility:
            # The minimum version is 24.3 because that's when enable_analyzer
            # became enabled by default, and the fuzzer profile constrains
            # enable_analyzer to >= 1 to avoid wasting cycles on the old
            # interpreter. An older compatibility version would revert the
            # setting instead of tripping the constraint.
            compatibility_setting = "24.3"
        elif is_targeted:
            compatibility_setting = None
        else:
            compatibility_setting = f"{random.randint(24, 27)}.{random.randint(1, 12)}"
        if compatibility_setting:
            logging.info("AST fuzzer compatibility setting: %s", compatibility_setting)
        else:
            logging.info("AST fuzzer compatibility setting is not set")

    run_command = get_run_command(
        docker_image,
        buzzhouse,
        targeted_queries_file=targeted_queries_file,
        compatibility_setting=compatibility_setting,
        enable_oracle=is_oracle,
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

    # Fix file ownership after running docker as root
    logging.info("Fuzzer: Fixing file ownership after running docker as root")
    Utils.fix_ownership_after_docker(cwd, docker_image)

    server_log, fuzzer_log, stderr_log, dmesg_log, fatal_log = JOB_ARTIFACTS
    paths = list(JOB_ARTIFACTS)
    if buzzhouse:
        paths.extend([WORKSPACE_PATH / "fuzzerout.sql", WORKSPACE_PATH / "fuzz.json"])

    # Raw sanitizer reports written via *SAN_OPTIONS=log_path (see run-fuzzer.sh).
    # Their contents are also merged into stderr.log/server.log, but upload the
    # originals too for debugging truncated reports.
    paths.extend(sorted(WORKSPACE_PATH.glob("sanitizer.log.*")))

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
        early_result = Result.create_from(
            status=Result.Status.ERROR, info=error_info, stopwatch=sw
        )
        for file in paths:
            if file.exists() and file.stat().st_size > 0:
                early_result.set_files(file)
        early_result.complete_job()
        return

    result = analyze_job_logs(
        paths,
        server_died,
        server_exit_code,
        fuzzer_exit_code,
        is_sanitized,
        WORKSPACE_PATH / "fuzzerout.sql" if buzzhouse else fuzzer_log,
        fuzzer_log,
        dmesg_log,
        [server_log],
        [stderr_log],
        [fatal_log],
        extra_results,
        sw,
        False,
        buzzhouse=buzzhouse,
    )

    result.complete_job()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("check_name")
    args = parser.parse_args()

    run_fuzz_job(args.check_name)
