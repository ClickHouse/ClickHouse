import csv
import logging
import os
import re
import shutil
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional, Tuple

from ci.jobs.scripts.clickhouse_service import ClickHouseService
from ci.jobs.scripts.docker_image import DockerImage
from ci.jobs.scripts.log_parser import SANITIZER_OOM_REPORT_PATTERN, FuzzerLogParser
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils

# Every log of the server family, the rotated ones included, and always scanned with `rg -z`
# because the logger gzips on rotation. This has to be the same family the log parser below
# is handed: a crash that rotated out of the current log still belongs to this run, and a
# narrower trigger would skip the parser for exactly the runs where only a `.log.1.gz`
# holds the evidence.
SERVER_LOG_FAMILY_GLOB = "clickhouse-server*.log*"

# The live log pair of one server process - `clickhouse-server.log` and its `.err.` sibling -
# per replica. Requiring a dotless suffix is what excludes both the logger's rotated files and
# the phase logs the runners archive by renaming (`clickhouse-server.{initial,stress,final,
# upgrade}.log`): a `signal 9` in either belongs to an incarnation that was already replaced
# and restarted, and the run records its own verdict for that (`Possible deadlock on
# shutdown`), so reading it as an OOM would excuse that very failure.
LIVE_SERVER_LOG_RE = re.compile(r"^clickhouse-server[^.]*(\.err)?\.log$")

# Failing rows an out-of-memory run cannot produce, matched case-insensitively against the
# result name. A failed test case, a server that would not come back up and a non-zero script
# exit are all ordinary collateral of a server killed mid-run, and the OOM downgrade exists
# for them. These are findings in their own right - the writers are `stress.py` (hung check),
# `tests/docker_scripts/stress_tests.lib` (the rest) and the log parser below - so the
# downgrade must not bury them even though no crash was named.
NON_OOM_FINDING_MARKERS = (
    "hung check",
    "possible deadlock",
    "logical error",
    "sanitizer",
    "lost forever",
    "no such key",
)


def _names_a_non_oom_finding(results: List[Result]) -> bool:
    return any(
        marker in (r.name or "").lower()
        for r in results
        for marker in NON_OOM_FINDING_MARKERS
    )


# The watchdog's record of a SIGKILL. A kernel OOM kill leaves exactly this line and nothing
# else - no `Logical error`, no sanitizer report - so it is the only in-log evidence of one.
KILL_LINE = " <Fatal> Application: Child process was terminated by signal 9"

# The harness sends the same SIGKILL itself when a server ignores SIGTERM (`stop_server` in
# `tests/docker_scripts/stress_tests.lib` ends in `clickhouse stop --force`), and records
# each such kill as a `Warning: server did not stop yet` row before sending it. The watchdog
# cannot tell the two senders apart, so the rows are the way to discount the harness's own
# kills from the kill lines.
HARNESS_KILL_MARKER = "server did not stop yet"


def count_harness_kills(results: List[Result]) -> int:
    """How many SIGKILLs the harness itself sent, as recorded in the results."""
    return sum(1 for r in results if HARNESS_KILL_MARKER in (r.name or "").lower())


def _live_log_stem(name: str) -> str:
    """The server process a live log belongs to, with its two channels folded together."""
    return name[: -len(".log")].removesuffix(".err")


def count_kill_lines(server_log_path: Path) -> int:
    """Kill lines in the live server logs, counted once per server process.

    The watchdog logs the line at `Fatal`, which both the main log and the `.err.` one
    receive, so a process's pair is folded to its larger count rather than summed - or a
    single kill would read as two and pass the run as an OOM on its own. Separate replicas
    stay separate, since each can be killed in its own right.
    """
    logs = _log_family(server_log_path, LIVE_SERVER_LOG_RE.match)
    if not logs:
        return 0
    # `--with-filename` forces the `path:count` form that a single file would otherwise
    # print bare; a file with no match is left out of the output entirely.
    output = Shell.get_output(
        f"rg -Fac --with-filename -- '{KILL_LINE}' "
        + " ".join(f"'{log}'" for log in logs)
    )
    per_process: dict[str, int] = {}
    for line in output.splitlines():
        if not line.strip():
            continue
        path, _, count = line.rpartition(":")
        stem = _live_log_stem(Path(path).name)
        per_process[stem] = max(per_process.get(stem, 0), int(count))
    return sum(per_process.values())


def server_log_reports_oom(server_log_path: Path, results: List[Result]) -> bool:
    """Whether the live server logs hold a SIGKILL the harness did not send.

    Each harness kill is announced by a `Warning: server did not stop yet` row, so only
    a kill line beyond those can be the kernel's - and only that one may pass the run
    as an out-of-memory one. The harness's kill is the mark of a server that would not
    stop, and reading it as an OOM would rewrite whatever failed after it to OK.

    The rows span the whole run, so the kill lines have to as well, or an announced kill
    would cancel out a later real one. They do: the runners archive the plain log at each
    phase boundary but never touch `clickhouse-server.err.log`, which the logger never
    rotates either (`tests/config/config.d/logging_no_rotate.xml`), and the watchdog logs
    the kill at Fatal - so that one live file holds every incarnation's.
    """
    kill_lines = count_kill_lines(server_log_path)
    if kill_lines == 0:
        return False
    harness_kills = count_harness_kills(results)
    if kill_lines <= harness_kills:
        print(
            f"{kill_lines} kill line(s) in the live server logs, all accounted for by "
            f"{harness_kills} harness-initiated kill(s): not an OOM"
        )
        return False
    return True


def oom_explains_failure(
    is_oom: bool, crash_named: bool, failed_results: List[Result]
) -> bool:
    """Whether a failing run may be passed as an out-of-memory one.

    Running out of memory is allowed in stress tests, so it passes the run - but it does
    not explain a crash. A kernel OOM kill writes no `Logical error`, no assertion and no
    sanitizer report, so when the parser named one of those the run found a real bug and
    the downgrade must not bury it. Nor when a failing row names one itself: the parser only
    runs under `server_died or crash_evidence`, so a hung check or a lost-key error reported
    by the suite alone leaves `crash_named` False and would otherwise be rewritten to OK.
    """
    if not is_oom or crash_named:
        return False
    if _names_a_non_oom_finding(failed_results):
        print("A failing result names a finding an OOM does not explain")
        return False
    return True


def _log_family(directory: Path, matches) -> List[Path]:
    """Every file of one log family in `directory`, sorted, tolerating a missing directory."""
    if not directory.exists():
        return []
    return sorted(p for p in directory.iterdir() if p.is_file() and matches(p.name))


def _replica_logs(logs: List[Path], replica: str | None) -> List[Path]:
    """The subset of `logs` belonging to a shared-catalog replica, or to the main one."""
    if replica is None:
        return [p for p in logs if "sc1" not in p.name and "sc2" not in p.name]
    return [p for p in logs if replica in p.name]


# The parser's own two report patterns, so that the stderr scan below fires exactly when the
# parser has something to say about the file rather than on a substring of its own choosing.
STDERR_REPORT_PATTERN = (
    f"{FuzzerLogParser.SANITIZER_ERROR_PATTERN}|{FuzzerLogParser.RUNTIME_ERROR_PATTERN}"
)


def stderr_reports_sanitizer_error(stderr_logs: List[Path]) -> bool:
    """Whether a stderr log holds a sanitizer or runtime-error report that is not an OOM.

    A sanitizer report never goes through the logger, and TSan - like a UBSan build that
    recovers - prints one and lets the server run on. Such a report leaves neither a
    `<Fatal>` record nor a dead process, so it is its own trigger for the log parser.

    The out-of-memory report is left out: it is benign, and reaching the parser for it
    would also let the parser's OOM verdict rewrite unrelated failing rows to OK.
    """
    if not stderr_logs:
        return False
    files = " ".join(f"'{p}'" for p in stderr_logs)
    # `-z` because the logger gzips on rotation. Filtering the OOM reports out of the match
    # stream rather than bounding the search keeps a real report that sits behind them.
    hit = Shell.get_output(
        f"rg -z --text --no-filename -- '{STDERR_REPORT_PATTERN}' {files}"
        f" | rg -v -m 1 -- '{SANITIZER_OOM_REPORT_PATTERN}'"
    )
    return bool(hit.strip())


# The runner agent lives on the host, outside this container, so it is only safe if the container cannot take the whole box.
RUNNER_MEMORY_RESERVE = 8 * 1024**3


def container_memory_limit() -> int:
    visible = Utils.physical_memory()
    limit = visible - RUNNER_MEMORY_RESERVE
    if limit <= 0:
        raise RuntimeError(
            f"Not enough RAM to run this job: {RUNNER_MEMORY_RESERVE} bytes are reserved for the "
            f"runner agent outside the container and this host has {visible}. Docker refuses a "
            f"negative --memory and reads 0 as no limit at all, so there is no safe cap to pass."
        )
    return limit


def sanitize_test_result_line(line: str) -> str:
    # Drop bare CR in addition to escaping NUL. The writer escapes
    # `\0\t\n` into backslash forms (`escape_tsv_info`) but not `\r`,
    # and the apt-get / dpkg progress frames captured into the
    # `Hung check failed` info field by `_ensure_lldb_installed` use
    # bare CR to overwrite the previous "(Reading database ... N%)"
    # frame. Left in place, those CRs are translated to LF by
    # universal-newlines mode and fragment the row.
    return line.replace("\0", "\\0").replace("\r", "")


def read_test_results(results_path: Path, with_raw_logs: bool = True):
    """Parse the stress-job `test_results.tsv` file.

    Returns `(results, malformed)`:
      - `results`: valid `Result` rows.
      - `malformed`: list of `(line_number, raw_first_cell)` tuples for
        rows that had fewer than 2 tab-separated cells.

    Malformed rows are skipped rather than fatal. They commonly appear
    when something writes stray output into the file (e.g. an
    `apt-get install` log line leaking into the result directory), and
    raising on the first one would discard every valid row above and
    below it — including the real failure that triggered the job.
    Callers should surface the count back to investigators via a
    separate `Result` entry so the corruption is still noticed.
    """
    results = []
    malformed = []
    # Split on LF only (not on CR or CRLF) so bare CR bytes that the
    # writer failed to escape stay inside their row instead of
    # fragmenting it. `sanitize_test_result_line` then strips those
    # CRs before csv parsing. Python's default text-mode `open` would
    # turn every bare CR into LF (universal-newlines), and `newline=""`
    # still splits lines on CR — both behaviours produce the "row
    # exploded into many fragments" failure observed on PR #105243
    # Stress test (arm_debug).
    with open(results_path, "rb") as descriptor:
        raw = descriptor.read().decode("utf-8", errors="replace")
    lines = raw.split("\n")
    reader = csv.reader(
        (sanitize_test_result_line(line) for line in lines),
        delimiter="\t",
    )
    for line_number, line in enumerate(reader, start=1):
        # Blank lines (typically a trailing newline at end of file, or
        # a separator artifact between writes) carry no information —
        # skip them silently.
        if not line:
            continue
        if len(line) < 2:
            malformed.append((line_number, line[0]))
            continue
        name = line[0]
        status = line[1]
        time = None
        if len(line) >= 3 and line[2] and line[2] != "\\N":
            # The value can be empty, but when it's not,
            # it's the time spent on the test
            try:
                time = float(line[2])
            except ValueError:
                pass

        result = Result(name, status, duration=time)
        if not (result.is_ok() or result.is_failure() or result.is_error()):
            # Unknown status — treat as a malformed row rather than
            # aborting the whole file. Aborting would re-introduce the
            # `Unknown job error` failure mode this parser is here to
            # eliminate: a single unexpected-status row would discard
            # every valid neighbour, including the real failure that
            # triggered the job. (The pre-existing assert referenced
            # `is_failure`/`is_error` as attributes — unbound method
            # objects, always truthy — so it never fired and let any
            # status through silently. Fix to call the methods.)
            malformed.append((line_number, line[0]))
            continue
        if len(line) == 4 and line[3]:
            # The value can be empty, but when it's not,
            # the 4th value is a pythonic list, e.g. ['file1', 'file2']
            if with_raw_logs:
                # Python does not support TSV, so we unescape manually.
                # The writer (`escape_tsv_info`) emits `\\r` for CR, so
                # unescape it here too. Without this, dpkg/apt-get
                # progress markers in the info field would leak the
                # literal two-character `\r` sequence into the displayed
                # log.
                result.set_info(
                    line[3]
                    .replace("\\t", "\t")
                    .replace("\\r", "\r")
                    .replace("\\n", "\n")
                )
            else:
                result.set_info(line[3])
        results.append(result)
    return results, malformed


def _format_malformed_summary(
    results_path: Path,
    malformed: List[Tuple[int, str]],
) -> str:
    sample = malformed[0]
    preview = sample[1][:200]
    return (
        f"{len(malformed)} malformed row(s) in {results_path} skipped; "
        f"first at line {sample[0]}: {preview!r}"
    )


def get_additional_envs(info, check_name: str) -> List[str]:
    from ci.jobs.ci_utils import is_extended_run

    result = []
    # some cloud-specific features require feature flags enabled
    # so we need this ENV to be able to disable the randomization
    # of feature flags
    result.append("RANDOMIZE_KEEPER_FEATURE_FLAGS=1")
    if "azure" in check_name:
        result.append("USE_AZURE_STORAGE_FOR_MERGE_TREE=1")

    if "s3" in check_name:
        result.append("USE_S3_STORAGE_FOR_MERGE_TREE=1")

    result.append(f"STRESS_GLOBAL_TIME_LIMIT={'3600' if is_extended_run() else '1200'}")

    return result


def get_run_command(
    build_path: Path,
    result_path: Path,
    repo_tests_path: Path,
    server_log_path: Path,
    cores_path: Path,
    additional_envs: List[str],
    image: DockerImage,
    upgrade_check: bool,
) -> str:
    envs = [f"-e {e}" for e in additional_envs]
    env_str = " ".join(envs)

    if upgrade_check:
        run_script = "/repo/tests/docker_scripts/upgrade_runner.sh"
    else:
        run_script = "/repo/tests/docker_scripts/stress_runner.sh"

    cmd = (
        "docker run --cap-add=SYS_PTRACE "
        # For dmesg and sysctl
        "--privileged "
        # azurite-rs (in-process Azure Blob Storage emulator) needs many fds under parallel load
        "--ulimit nofile=1048576:1048576 "
        f"--memory={container_memory_limit()} "
        # a static link, don't use S3_URL or S3_DOWNLOAD
        "-e S3_URL='https://s3.amazonaws.com/clickhouse-datasets' "
        "--tmpfs /tmp/clickhouse:mode=1777 "
        f"--volume={build_path}:/package_folder "
        f"--volume={result_path}:/test_output "
        f"--volume={repo_tests_path}/..:/repo "
        f"--volume={server_log_path}:/var/log/clickhouse-server "
        f"--volume={cores_path}:/cores "
        f"{env_str} {image} {run_script}"
    )

    return cmd


def process_results(
    result_directory: Path, server_log_path: Path
) -> Tuple[str, str, List[Result], List[Path]]:
    test_results = []
    additional_files = []
    # Just upload all files from result_folder.
    # If task provides processed results, then it's responsible for content
    # of result_folder.
    if result_directory.exists():
        additional_files = [p for p in result_directory.iterdir() if p.is_file()]

    if server_log_path.exists():
        additional_files = additional_files + [
            p for p in server_log_path.iterdir() if p.is_file()
        ]

    results_path = result_directory / "test_results.tsv"
    malformed: List[Tuple[int, str]] = []
    try:
        test_results, malformed = read_test_results(results_path, True)
        if len(test_results) == 0 and not malformed:
            raise ValueError("Empty results")
    except Exception as e:
        test_results = [
            Result(
                name="Unknown job error",
                status=Result.Status.ERROR,
                info=f"Cannot parse test_results.tsv ({e})",
            )
        ]
        return test_results, additional_files

    if not test_results:
        # The file existed and had content, but every row was malformed.
        # Surface the corruption directly instead of "Unknown job error".
        test_results = [
            Result(
                name="Corrupt test_results.tsv",
                status=Result.Status.ERROR,
                info=_format_malformed_summary(results_path, malformed),
            )
        ]
        return test_results, additional_files

    if malformed:
        # Some rows were unreadable but valid rows survived. Add a
        # FAIL entry so investigators see the corruption without
        # losing the real test results (in particular, the
        # `Hung check failed, possible deadlock found` row that
        # otherwise gets swallowed when the file is polluted by
        # stray output such as `apt-get install` logs).
        test_results.append(
            Result(
                name="Corrupt test_results.tsv",
                status=Result.Status.FAIL,
                info=_format_malformed_summary(results_path, malformed),
            )
        )

    return test_results, additional_files


# One `(name, description, files)` finding as `FuzzerLogParser.parse_failure` returns it.
Finding = Tuple[str, str, List[str]]


@dataclass
class ReplicaFailures:
    """What `select_replica_failures` found across every replica.

    `results` is the list to report: every distinct specific classification when there is
    at least one; otherwise the one `<Fatal>` the parser could not name; otherwise the one
    memory-limit verdict; otherwise the one expected-only / "Unknown error" fallback; empty
    when nothing could be parsed at all. The flags say which tier `results` came from,
    because the caller treats the tiers differently: a crash - named or not - is a bug that
    no OOM downgrade may bury, an out-of-memory verdict passes the run outright, and an
    expected-only line names a run that something else already declared failed and is not
    a failure of its own.
    """

    results: List[Finding] = field(default_factory=list)
    # `results` holds a crash: a specific classification, or a `<Fatal>` the parser
    # could not classify. Not the memory limit - that is what an out-of-memory run
    # reports, not a bug.
    crash_named: bool = False
    # `results` holds only the expected-only fallback, and that fallback has a name: an
    # `EXPECTED_PATTERNS` line (the end-of-run SIGKILL, a sanitizer OOM report) was all
    # the replicas had to say.
    expected_only: bool = False
    # `expected_only`, and the line is a sanitizer OOM report rather than the kill line.
    expected_only_oom: bool = False
    # `results` holds the memory-limit verdict: the server refused an allocation over its
    # own cap and said so. Ranked below every crash for that reason, and out of memory in
    # the same sense the sanitizer report is.
    memory_limit: bool = False

    @property
    def reports_oom(self) -> bool:
        """Whether the tier `results` came from is an out-of-memory verdict.

        Both ways the parser can reach one: the sanitizer's report among the expected-only
        lines, and the server's own memory cap. Neither is a bug, and running out of
        memory passes a stress run - so the caller must not tell the two tiers apart.
        """
        return self.expected_only_oom or self.memory_limit


# Ranking inside the expected-only tier, which needs one of its own because every verdict
# in it is "named": a sanitizer OOM report is the only one that can pass the run - it says
# the run ran out of memory - so another replica's routine kill line must not mask it, and
# both say more than a nameless verdict. Without this the tier is first-come, and which
# replica the job happens to scan first decides whether the run is allowed its OOM.
_FALLBACK_UNKNOWN, _FALLBACK_NAMED, _FALLBACK_OOM = 0, 1, 2


def _fallback_rank(name: str, description: str) -> int:
    if name == FuzzerLogParser.UNKNOWN_ERROR:
        return _FALLBACK_UNKNOWN
    if re.search(SANITIZER_OOM_REPORT_PATTERN, description):
        return _FALLBACK_OOM
    return _FALLBACK_NAMED


def select_replica_failures(
    replica_log_pairs: List[Tuple[str, List[Path], List[Path]]],
) -> ReplicaFailures:
    """Pick the failures to report from every replica's server and stderr log families.

    Each triple is `(replica_name, server_log_files, stderr_log_files)`, both lists with
    the rotated (`.gz`) files included and handed to a single parser call, so the parser
    defers an expected kill line across every log of the replica at once.

    A failure on one replica must not hide a (possibly higher-signal) failure on another,
    so every replica is scanned - never breaking early - and the verdicts are ranked only
    afterwards: a named crash anywhere beats a `<Fatal>` the parser could not classify,
    which beats the memory limit, which beats the expected-only lines. All specific
    classifications (sanitizer, logical error, oracle mismatch, ...) are collected and
    every distinct one is reported, because replicated setups surface the same failure on
    several replicas; the lower tiers each report a single finding.
    """
    specific_results: List[Finding] = []
    seen_specific_names = set()
    fatal_result: Optional[Finding] = None
    memory_limit_result: Optional[Finding] = None
    fallback_result: Optional[Finding] = None

    for replica_name, server_log_files, stderr_log_files in replica_log_pairs:
        log_parser = FuzzerLogParser(
            server_logs=server_log_files or None,
            stderr_logs=stderr_log_files or None,
        )
        file_names = ", ".join(p.name for p in (*server_log_files, *stderr_log_files))
        try:
            file_pair_info = f"Log files: {file_names}"
            # A real failure first, so that one replica's expected kill line never names
            # the run before another replica's crash is seen: an expected-only line may
            # still name the run as it did before - but only as the last resort below.
            name, description, files = log_parser.parse_failure()
            description = f"{file_pair_info}\n{description}"
            if name == FuzzerLogParser.MEMORY_LIMIT_ERROR:
                # Named, but an out-of-memory run rather than a crash: another replica
                # may still hold the crash this run would otherwise be downgraded past.
                if memory_limit_result is None:
                    memory_limit_result = (name, description, files)
                continue
            if name != FuzzerLogParser.UNKNOWN_ERROR:
                if log_parser.is_generic_fatal:
                    # A `<Fatal>` the parser cannot classify is crash evidence, so it
                    # outranks any replica's benign verdict - but it is a lower-confidence
                    # signal than a known classification, so it never joins them.
                    if fatal_result is None:
                        fatal_result = (name, description, files)
                elif name not in seen_specific_names:
                    seen_specific_names.add(name)
                    specific_results.append((name, description, files))
                continue
            # `UNKNOWN_ERROR` says only that no pattern got a genuine match. A `<Fatal>`
            # record left unclassified is still crash evidence and has to outrank the
            # expected lines.
            unnamed_fatals = log_parser.find_unnamed_fatals()
            if unnamed_fatals and fatal_result is None:
                fatal_result = (
                    name,
                    f"{description}Unclassified fatal:\n"
                    + "\n".join(unnamed_fatals)
                    + "\n",
                    files,
                )
            name, description, files = log_parser.parse_failure(
                allow_expected_only=True
            )
            # Keep the highest-ranked fallback rather than the first, so the tier does not
            # depend on the order the replicas are scanned in.
            candidate: Finding = (name, f"{file_pair_info}\n{description}", files)
            if fallback_result is None or _fallback_rank(
                candidate[0], candidate[1]
            ) > _fallback_rank(fallback_result[0], fallback_result[1]):
                fallback_result = candidate
        except Exception as e:
            print(
                f"ERROR: Failed to parse failure logs for {replica_name} "
                f"({file_names}): {e}\n"
                f"Server logs should still be collected."
            )

    if specific_results:
        return ReplicaFailures(results=specific_results, crash_named=True)
    if fatal_result is not None:
        return ReplicaFailures(results=[fatal_result], crash_named=True)
    if memory_limit_result is not None:
        return ReplicaFailures(results=[memory_limit_result], memory_limit=True)
    if fallback_result is not None:
        name, description, _ = fallback_result
        # Read off the same rank the selection used, so what was picked and what it is
        # reported as can never disagree.
        rank = _fallback_rank(name, description)
        return ReplicaFailures(
            results=[fallback_result],
            expected_only=rank != _FALLBACK_UNKNOWN,
            expected_only_oom=rank == _FALLBACK_OOM,
        )
    return ReplicaFailures()


def run_stress_test(upgrade_check: bool = False) -> None:
    info = Info()
    logging.basicConfig(level=logging.INFO)

    stopwatch = Utils.Stopwatch()
    temp_path = Path(Utils.cwd()) / "ci/tmp"
    repo_tests_path = Path(Utils.cwd()) / "tests"

    check_name = sys.argv[1] if len(sys.argv) > 1 else os.getenv("CHECK_NAME")
    assert (
        check_name
    ), "Check name must be provided as an input arg or in CHECK_NAME env"

    packages_path = temp_path

    docker_image = DockerImage.get_docker_image("clickhouse/stress-test").pull_image()

    server_log_path = temp_path / "server_log"
    result_path = temp_path / "result_path"
    cores_path = temp_path / "cores"

    # Wiped, not just created: the scans below take whole log families, so a rotated log a
    # previous local run left here would be read as this run's evidence. Not `temp_path`
    # itself - the packages to install live directly in it.
    for path in (server_log_path, result_path, cores_path):
        shutil.rmtree(path, ignore_errors=True)
        path.mkdir(parents=True, exist_ok=True)

    additional_envs = get_additional_envs(info, check_name)

    run_command = get_run_command(
        packages_path,
        result_path,
        repo_tests_path,
        server_log_path,
        cores_path,
        additional_envs,
        docker_image,
        upgrade_check,
    )
    logging.info("Going to run stress test: %s", run_command)

    exit_code = Shell.run(run_command)

    Utils.fix_ownership_after_docker(temp_path, docker_image)

    core_files = ClickHouseService.collect_cores(cores_path)

    is_oom = False

    if Path(result_path / "dmesg.log").is_file():
        is_oom = Shell.check(
            "grep -q -F -e 'Out of memory: Killed process' -e 'oom_reaper: reaped process' -e 'oom-kill:constraint=CONSTRAINT_NONE' "
            f"{result_path}/dmesg.log"
        )

    # Generate fatal.log from all server logs
    fatal_log = result_path / "fatal.log"
    crash_evidence = False
    if server_log_path.exists():
        Shell.check(
            f"rg -z --text '\\s<Fatal>\\s' {server_log_path}/{SERVER_LOG_FAMILY_GLOB}"
            f" > {fatal_log}"
        )
        # rg also exits non-zero when it did match but could not read some file of
        # the glob, so key on the collected content rather than on its exit code.
        crash_evidence = fatal_log.is_file() and fatal_log.stat().st_size > 0

    test_results, additional_logs = process_results(result_path, server_log_path)

    # Check for OOM (signal 9) in server logs. This sets `is_oom`, which rewrites the whole
    # job to OK at the end, so it reads the live logs only - a kill line in a rotated or
    # archived phase log describes an already-restarted server rather than this run's
    # outcome - and discounts the kills the harness itself announced in `test_results`.
    is_oom = is_oom or server_log_reports_oom(server_log_path, test_results)

    server_died = False
    # Set once the log parser names a crash, so the OOM downgrade at the end cannot bury it.
    crash_named = False
    failed_results = []
    for test_result in test_results:
        if test_result.name == "Server died":
            server_died = True
            continue
        if not test_result.is_ok():
            failed_results.append(test_result)

    # The runner moves the current `stderr.log` to the result directory and leaves the
    # rotated ones in the server log directory, so the family spans both.
    stderr_logs = _log_family(
        result_path, lambda n: n.startswith("stderr")
    ) + _log_family(server_log_path, lambda n: n.startswith("stderr"))
    # The third trigger, beside a dead server and a `<Fatal>` record: `crash_evidence` reads
    # the server logs alone, so a report that only ever reached stderr - and that killed
    # nothing - would otherwise never be looked at and the run would finish green.
    stderr_evidence = stderr_reports_sanitizer_error(stderr_logs)

    if server_died or crash_evidence or stderr_evidence:
        # Both whole log families per replica, rotated files included, each handed to a single
        # parser call: the parser defers an expected kill line only across the logs it gets at
        # once.
        replica_log_pairs: list[tuple[str, list[Path], list[Path]]] = []
        # The full `clickhouse-server*.log*` family, not just `.err.`: `crash_evidence` above
        # is already set from a fatal anywhere in that whole family, so a fatal that landed
        # only in the plain (non-`.err.`) log must reach the parser too, or it is never seen.
        server_logs_family = _log_family(
            server_log_path, lambda n: n.startswith("clickhouse-server") and ".log" in n
        )

        for replica_name, replica in (("main", None), ("sc1", "sc1"), ("sc2", "sc2")):
            replica_server_logs = _replica_logs(server_logs_family, replica)
            replica_stderr_logs = _replica_logs(stderr_logs, replica)
            # Either family on its own is enough to classify the replica: a crash can write a
            # sanitizer report to stderr and never create an err log at all.
            if replica_server_logs or replica_stderr_logs:
                replica_log_pairs.append(
                    (replica_name, replica_server_logs, replica_stderr_logs)
                )

        if not replica_log_pairs:
            failed_results.append(
                Result.create_from(
                    name="Unknown error",
                    info="no server logs found",
                    status=Result.Status.FAIL,
                )
            )
        else:
            failures = select_replica_failures(replica_log_pairs)
            # A crash the parser recognised, or a `<Fatal>` it could not - either is a bug.
            # The memory limit is not: it is what an out-of-memory run reports.
            crash_named = failures.crash_named
            # An expected-only verdict names a run that something else already declared
            # failed. When only a log scan brought us here it declared nothing: with
            # rotated logs in scope the `<Fatal>` it found can be the expected kill itself,
            # and reporting that would fail a run for its own restart. A `<Fatal>` the
            # parser cannot name is not expected-only, and reports as a crash above.
            expected_only = failures.expected_only and not server_died
            # OOM is allowed in stress tests outright - `is_oom` above already passes the
            # run for a report in a current log or dmesg. The parser reaches one the scan
            # above cannot: a report that rotated out of the current log, and the server's
            # own memory cap, which leaves no SIGKILL and no dmesg line to find at all.
            # `server_died` says only that the process crashed, not why, so this is
            # checked independently of the `not server_died` guard above.
            if failures.reports_oom:
                is_oom = True
                print(
                    "Only an out-of-memory verdict in the server logs: "
                    f"{failures.results[0][0]}"
                )
            elif expected_only:
                print(
                    f"Only expected messages in the server logs: {failures.results[0][0]}"
                )
            elif failures.results:
                for name, description, files in failures.results:
                    failed_results.append(
                        Result.create_from(
                            name=name,
                            info=description,
                            status=Result.Status.FAIL,
                            files=files,
                        )
                    )
            else:
                failed_results.append(
                    Result.create_from(
                        name="Parse failure error",
                        info="All log parsing attempts failed",
                        status=Result.Status.FAIL,
                    )
                )

    if server_died and not failed_results:
        failed_results.append(
            Result.create_from(
                name="Server died",
                info="Server died and no specific error was extracted",
                status=Result.Status.FAIL,
            )
        )

    if exit_code != 0:
        failed_results.append(
            Result.create_from(
                name="Check failed",
                info=f"Check failed with exit code {exit_code}",
                status=Result.Status.FAIL,
            )
        )

    all_results = failed_results + [r for r in test_results if r.is_ok()]
    r = Result.create_from(
        results=all_results,
        status=Result.Status.OK if not failed_results else "",
        stopwatch=stopwatch,
    )
    if not r.is_ok() and oom_explains_failure(is_oom, crash_named, failed_results):
        r.set_status(Result.Status.OK)
        r.set_info("OOM error (allowed in stress tests)")

    if r.is_ok() and exit_code != 0 and not is_oom:
        r.set_failed().set_info(
            f"Unknown error: Test script failed with exit code {exit_code}"
        )

    r.set_files(additional_logs).set_files(core_files).complete_job()


if __name__ == "__main__":
    run_stress_test()
