#!/usr/bin/env python3
import logging
import os
import random
import re
import sys
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
    logging.info("[targeted-fuzzer] Step 1 — changed/new tests (%d): %s",
                 len(changed_tests), ", ".join(sorted(changed_tests)) or "(none)")

    # Step 2: tests that failed in previous CI runs for this PR
    try:
        previously_failed = targeter.get_previously_failed_tests()
    except Exception as e:
        logging.warning("[targeted-fuzzer] Step 2 — failed to fetch previously-failed tests: %s", e)
        previously_failed = []
    logging.info("[targeted-fuzzer] Step 2 — previously failed tests (%d): %s",
                 len(previously_failed), ", ".join(previously_failed) or "(none)")

    # Step 3: coverage-relevant tests (direct lines, indirect callees, siblings)
    try:
        relevant_tests, relevant_tests_result = targeter.get_most_relevant_tests()
    except Exception as e:
        logging.warning("[targeted-fuzzer] Step 3 — failed to fetch coverage-relevant tests: %s", e)
        relevant_tests = []
        relevant_tests_result = Result(name="tests found by coverage", status=Result.Status.OK, info=f"Skipped: {e}")
    logging.info("[targeted-fuzzer] Step 3 — coverage-relevant tests (%d)", len(relevant_tests))

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

    logging.debug("Indexed %d unique SQL query base names from %s", len(available_queries), stateless_tests_dir)

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


def run_fuzz_job(check_name: str):
    logging.basicConfig(level=logging.INFO)
    is_targeted = "targeted" in check_name.lower()
    is_oracle = "oracle" in check_name.lower()
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
            # became enabled by default, and the fuzzer profile constrains
            # enable_analyzer to >= 1 to avoid wasting cycles on the old
            # interpreter. An older compatibility version would revert the
            # setting instead of tripping the constraint.
            compatibility_setting = "24.3"
        elif is_targeted:
            compatibility_setting = None
        else:
            compatibility_setting = (
                f"{random.randint(24, 27)}.{random.randint(1, 12)}"
            )
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
        early_result = Result.create_from(status=Result.Status.ERROR, info=error_info)
        for file in paths:
            if file.exists() and file.stat().st_size > 0:
                early_result.set_files(file)
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
    elif fuzzer_exit_code == 49 and not buzzhouse:
        # AST fuzzer client called _exit(49) after the server-side oracle
        # reported a wrong-result mismatch. The fuzzer log contains a clearly
        # delimited "AST FUZZER ORACLE MISMATCH (fatal)" block with the
        # reproducer query and the server-side oracle output.
        status = Result.Status.ERROR
        error_info = Shell.get_output(
            f"rg --text -A 30 'AST FUZZER ORACLE MISMATCH' {fuzzer_log}"
        )
        if not error_info:
            error_info = (
                "AST fuzzer oracle mismatch detected, but the marker block was "
                "not found in the fuzzer log (see attached fuzzer.log)."
            )
        info.append(f"ERROR: AST fuzzer oracle mismatch\n{error_info}")
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
            if sanitizer_oom or kernel_oom_kill:
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

    results = []
    if is_failed and status != Result.Status.ERROR:
        # died server - lets fetch failure from log
        fuzzer_log_parser = FuzzerLogParser(
            server_log=str(server_log),
            stderr_log=str(stderr_log),
            fuzzer_log=str(
                WORKSPACE_PATH / "fuzzerout.sql" if buzzhouse else fuzzer_log
            ),
        )
        parsed_name, parsed_info, files = fuzzer_log_parser.parse_failure()

        if parsed_name:
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
    )

    if is_failed:
        # generate fatal log
        Shell.check(f"rg --text '\\s<Fatal>\\s' {server_log} > {fatal_log}")
        result.set_files(ClickHouseService.collect_cores(WORKSPACE_PATH))

    # Attach logs whenever the fuzzer did not finish cleanly. A clean finish is
    # exit code 0; any non-zero exit (real failure, oracle mismatch, SIGTERM /
    # SIGKILL from the FUZZ_TIME_LIMIT timeout wrapper) is informative enough
    # that we want the artifacts uploaded — otherwise timeouts look like silent
    # passes with no logs to diagnose them from.
    if is_failed or fuzzer_exit_code != 0:
        for file in paths:
            if file.exists() and file.stat().st_size > 0:
                result.set_files(file)

    result.complete_job()


if __name__ == "__main__":
    check_name = sys.argv[1] if len(sys.argv) > 1 else os.getenv("CHECK_NAME")
    assert (
        check_name
    ), "Check name must be provided as an input arg or in CHECK_NAME env"

    run_fuzz_job(check_name)
