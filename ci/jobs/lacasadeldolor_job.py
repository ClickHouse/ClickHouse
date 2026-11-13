import argparse
import gzip
import os
import random
import re
import secrets
import shlex
import shutil
import subprocess
import time
import xml.etree.ElementTree as ET
from pathlib import Path

from ci.jobs.ast_fuzzer_job import (
    JOB_ARTIFACTS,
    WORKSPACE_PATH,
    analyze_job_logs,
    triage_failure,
)
from ci.jobs.buzzhouse_job import generate_buzz_config
from ci.jobs.scripts.clickhouse_service import ClickHouseService
from ci.jobs.scripts.integration_tests_configs import IMAGES_ENV
from ci.jobs.scripts.log_parser import FuzzerLogParser
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils
from tests.casa_del_dolor.binary import detect_private_binary

repo_dir = Utils.cwd()

# Clean and graceful shutdowns: 0 and `SIGTERM`, as reported by `exec_inspect`.
GRACEFUL_EXIT_CODES = (0, -15, 143)
SIGKILL_EXIT_CODES = (137, -9)

# Minutes of load: the `--timeout` of `dolor.py` and the `time_to_run` of BuzzHouse, which must
# match, or `dolor.py` kills BuzzHouse before it is done.
DOLOR_RUN_MINUTES = 15

# Wall-clock ceiling on `dolor.py`, far above its internal --timeout plus
# restarts/setup/shutdown overhead. A run that reaches it is hung, whatever the logs say.
DOLOR_WATCHDOG_SEC = 4 * 3600

# The reasons `dolor.py` logs when it fails a run, and the result name each is reported
# under. Only the stable part of each message is matched, as the rest has been reworded.
# A stop or start that Dolor had to force ends with its own SIGKILL, never a kernel OOM's.
FORCED_STOP_MESSAGE = "did not shut down gracefully and had to be force killed"
START_FAILED_MESSAGE = "failed to start during a scheduled restart"
DOLOR_FAILURES = (
    (FORCED_STOP_MESSAGE, "Server shutdown"),
    ("failed to stop during a scheduled restart", "Server shutdown"),
    ("still running after a stop attempt", "Server shutdown"),
    ("Exit code unaccounted for", "Server shutdown"),
    (START_FAILED_MESSAGE, "Server startup"),
    ("exited on its own with code", "Load generator"),
)
# Reasons found in the server logs, named by the log parser instead. An OOM the log
# analysis already passed the run for explains a crash or a sanitizer report, but never a
# logical error.
SERVER_LOG_FAILURES = ("Crash in instance", "Sanitizer error in instance")
LOGICAL_ERROR_FAILURE = "Logical error in instance"
# Teardown failures, which fail the run on their own. Such an OOM explains one only when it is
# the teardown re-raising the OOM's sanitizer report (a Keeper's is reported by `_keeper_failures`).
CLEANUP_FAILURES = (
    "Cluster shutdown failed or found a problem",
    "Failed to close the Spark HTTP server",
)
CLEANUP_SANITIZER_ASSERT = "Exception: Sanitizer assert found for instance"
# What the watchdog logs as `<Fatal>` when Dolor SIGKILLs the server (`EXPECTED_KILL_FATAL` of `dolor.py`).
EXPECTED_KILL_FATAL = "Child process was terminated by signal 9 (KILL)"

# What the AI triage of a failure is told about reproducing it
TRIAGE_REPRO_HINT = (
    "`fuzzerout.sql` is the full log of the statements BuzzHouse sent; the last ones "
    "before the failure are the suspects. They often depend on tables, settings or "
    "dictionaries created by earlier statements, so replay the tail first and grow the "
    "replayed prefix until the failure reproduces. Unlike a plain BuzzHouse run, the "
    "servers run with a randomized configuration (`effective_config<N>.xml`, "
    "`effective_users<N>.xml`, `keeper<N>.xml`) and are restarted during the run, as "
    "`dolor.log` records; `buzzhouse.json` holds the BuzzHouse configuration and "
    "`server.sh` the server command line. When a repro with default settings fails, "
    "start a server with the effective configuration."
)


def _start_docker_in_docker():
    with open("./ci/tmp/docker-in-docker.log", "w") as log_file:
        dockerd_proc = subprocess.Popen(
            "./ci/jobs/scripts/docker_in_docker.sh",
            stdout=log_file,
            stderr=subprocess.STDOUT,
        )
    retries = 20
    for i in range(retries):
        if Shell.check("docker info > /dev/null", verbose=True):
            break
        if dockerd_proc.poll() is not None:
            # The script exited instead of starting the daemon, e.g. it refused because the
            # cgroup containment it was asked for could not be established. Say so now rather
            # than after 40 more seconds of a timeout that names the wrong cause.
            raise RuntimeError(
                f"docker_in_docker.sh exited early (rc={dockerd_proc.returncode}); "
                "see ./ci/tmp/docker-in-docker.log"
            )
        if i == retries - 1:
            raise RuntimeError(
                f"Docker daemon did not respond after {retries} attempts"
            )
        time.sleep(2)
    print(f"Started docker-in-docker asynchronously with PID {dockerd_proc.pid}")


def parse_args():
    parser = argparse.ArgumentParser(description="La Casa del Dolor Job")
    parser.add_argument(
        "--path",
        help="Optional. Path to custom clickhouse binary",
        type=str,
        default="",
    )
    parser.add_argument(
        "--path_1",
        help="Optional. Path to custom server config",
        type=str,
        default="",
    )
    parser.add_argument(
        "--param",
        help=(
            "Optional. Comma-separated KEY=VALUE pairs to inject as environment "
            "variables (e.g. --param KEY=VALUE,OTHER=1)"
        ),
        type=str,
        default="",
    )
    return parser.parse_args()


def _dolor_instances_dir() -> str:
    """Where `ClickHouseCluster` put the per-node directories for the `dolor` cluster, as the
    cluster helper names it. Imported lazily: `helpers.cluster` needs the integration-test
    dependencies, only present in the runner image."""
    from tests.integration.helpers.cluster import get_instances_dir

    return f"{repo_dir}/tests/casa_del_dolor/{get_instances_dir('dolor')}"


# Node logs a healthy run never produces, so their absence is not worth a warning.
# `gdb.log` is only written when `stop_clickhouse` had to force kill a hung server.
OPTIONAL_NODE_LOGS = frozenset({"gdb.log"})


def get_node_container_logs():
    logs_dir = f"{_dolor_instances_dir()}/node0/logs"
    return [
        # ClickHouse server log file (after final restart)
        Path(f"{logs_dir}/clickhouse-server.log"),
        # ClickHouse server error log file (after final restart)
        Path(f"{logs_dir}/clickhouse-server.err.log"),
        # ClickHouse server stdout log file
        Path(f"{logs_dir}/stdout.log"),
        # ClickHouse server stderr log file, where a sanitizer report goes
        Path(f"{logs_dir}/stderr.log"),
        # Backtraces taken by `stop_clickhouse` before it force killed a hung server
        Path(f"{logs_dir}/gdb.log"),
    ]


def get_node_workspace_logs(workspace_path: Path):
    return [
        workspace_path / "server.log",
        workspace_path / "server.err.log",
        workspace_path / "stdout.log",
        workspace_path / "stderr.log",
        workspace_path / "gdb.log",
    ]


# The three Keeper processes `ClickHouseCluster` starts for the `dolor` cluster run the
# tested binary too. Their compose service restarts them, so a Keeper that aborts can leave
# a run that otherwise finishes cleanly: only its logs show it.
KEEPER_INDEXES = range(1, 4)


def get_keeper_container_logs(keeper_index: int):
    logs_dir = f"{_dolor_instances_dir()}/keeper{keeper_index}/log"
    return [
        Path(f"{logs_dir}/clickhouse-keeper.log"),
        Path(f"{logs_dir}/clickhouse-keeper.err.log"),
        # Raw stderr, where a sanitizer report goes
        Path(f"{logs_dir}/stderr.log"),
    ]


def get_keeper_workspace_logs(workspace_path: Path, keeper_index: int):
    return [
        workspace_path / f"keeper{keeper_index}.log",
        workspace_path / f"keeper{keeper_index}.err.log",
        workspace_path / f"keeper_stderr{keeper_index}.log",
    ]


def _copy_node_cores_to_workspace(workspace_path: Path) -> list[Path]:
    """Copy the cores of the Dolor servers and Keepers to `workspace_path`, the only place
    `ClickHouseService.collect_cores` looks. They run in a `cores` directory of their host-mounted
    log directory (`DOLOR_CORE_DIR` in `helpers.cluster`), which survives the container teardown."""
    instances_dir = Path(_dolor_instances_dir())
    if not instances_dir.exists():
        return []
    copied: list[Path] = []
    for src in sorted(
        [
            *instances_dir.glob("node*/logs/cores/core.*"),
            *instances_dir.glob("keeper*/log/cores/core.*"),
        ]
    ):
        if not src.is_file():
            continue
        dst = workspace_path / f"core.dolor.{src.parents[2].name}.{src.name}"
        try:
            shutil.copy2(src, dst)
            copied.append(dst)
        except OSError as e:
            print(f"WARNING: failed to copy core dump {src} -> {dst}: {e}")
    return copied


def _parse_logs(
    server_log: Path,
    stderr_log: Path | None,
    fuzzer_out: Path,
    parser_class: type[FuzzerLogParser] = FuzzerLogParser,
):
    """The log parser's verdict on one process's logs, or None when it names nothing."""
    if not server_log.is_file():
        return None
    name, description, files = parser_class(
        server_log=str(server_log),
        stderr_log=str(stderr_log) if stderr_log and stderr_log.is_file() else "",
        fuzzer_log=str(fuzzer_out),
    ).parse_failure()
    if not name or name == FuzzerLogParser.UNKNOWN_ERROR:
        return None
    return Result(name=name, info=description, status=Result.Status.FAIL, files=files)


def _rotated_logs(log: Path) -> list[Path]:
    """The rotations of `log` next to it (`<name>.N`, or `<name>.N.gz` when compressed),
    oldest first: the newest one is `.0`."""
    pattern = re.compile(re.escape(log.name) + r"\.(\d+)(\.gz)?")
    numbered = []
    for p in log.parent.glob(f"{log.name}.*"):
        m = pattern.fullmatch(p.name)
        if m and p.is_file():
            numbered.append((int(m.group(1)), p))
    return [p for _, p in sorted(numbered, reverse=True)]


def _concatenate(parts: list[Path], combined: Path, skip: bytes = b"") -> Path:
    """`parts` decompressed one after another into `combined`, without the lines holding `skip`."""
    with open(combined, "wb") as out:
        for part in parts:
            if not part.is_file():
                continue
            with (gzip.open if part.suffix == ".gz" else open)(part, "rb") as f:
                if skip:
                    out.writelines(line for line in f if skip not in line)
                else:
                    shutil.copyfileobj(f, out)
    return combined


def _log_with_rotations(log: Path, combined: Path) -> Path:
    """`log` with its rotations decompressed in front of it, written to `combined`, or `log`
    itself when it has none. A process that aborted and was restarted can have its fatal
    record only in a rotation."""
    rotated = _rotated_logs(log)
    return _concatenate([*rotated, log], combined) if rotated else log


class _RotatedLogParser(FuzzerLogParser):
    """A parser for the logs of a server restarted by Dolor, which did not necessarily fail: a
    memory limit exception there was recovered from, so it names no failure."""

    ERROR_PATTERNS = [
        p for p in FuzzerLogParser.ERROR_PATTERNS if p[1] != "is_memory_limit_exceeded"
    ]


def _has_fatal_record(log: Path) -> bool:
    pattern = re.compile(FuzzerLogParser.GENERIC_FATAL_PATTERN)
    with open(log, "r", encoding="utf-8", errors="replace") as f:
        return any(pattern.search(line) for line in f)


def _rotated_node_failure(workspace_path: Path, fuzzer_out: Path):
    """A failure in the rotated server logs, which belong to the servers before the last restart:
    the analysis parses the current logs only. Without `stderr.log`, which every server appends to,
    so that the parser names a failure of those servers, not the sanitizer report of the last one.
    Nor the `<Fatal>` the watchdog logs for every restart `--kill-server-prob` makes.
    Those servers may have been restarted healthy, so only logs with a `<Fatal>` record fail the run.
    The main log comes first: the parser looks up the query there."""
    for name in ("server.log", "server.err.log"):
        log = workspace_path / name
        rotated = _rotated_logs(log)
        if not rotated:
            continue
        combined = _concatenate(
            rotated,
            workspace_path / f"{log.stem}.rotated.log",
            skip=EXPECTED_KILL_FATAL.encode(),
        )
        if _has_fatal_record(combined) and (
            failure := _parse_logs(combined, None, fuzzer_out, _RotatedLogParser)
        ):
            return failure
    return None


def _keeper_failures(workspace_path: Path, fuzzer_out: Path) -> dict[int, Result]:
    """The failure report of each Keeper whose logs hold one, which nothing else reads."""
    failures: dict[int, Result] = {}
    for i in KEEPER_INDEXES:
        _, err_log, stderr_log = get_keeper_workspace_logs(workspace_path, i)
        err_log = _log_with_rotations(
            err_log, workspace_path / f"keeper{i}.err.combined.log"
        )
        files = " ".join(str(p) for p in (err_log, stderr_log) if p.is_file())
        # A Fatal other than the watchdog's SIGKILL record, or a raw sanitizer report: one
        # cut short by the abort may be only its `CHECK failed` or `DEADLYSIGNAL` line.
        if files and Shell.get_output(
            "rg --text '<Fatal>|^(==[0-9]+==)?(ERROR|WARNING|SUMMARY): [a-zA-Z]+Sanitizer:"
            f"|[a-zA-Z]+Sanitizer: CHECK failed:|[a-zA-Z]+Sanitizer:DEADLYSIGNAL|: runtime error: ' {files}"
            " | rg --text -v 'Child process was terminated by signal 9'"
        ):
            failures[i] = _parse_logs(err_log, stderr_log, fuzzer_out) or Result(
                name="Keeper",
                info=f"A failure report in keeper{i} the log parser cannot name. "
                "Check the keeper*.log artifacts.",
                status=Result.Status.FAIL,
            )
    return failures


def _dolor_failure_records(dolor_log: Path) -> list[str]:
    """The ERROR records of `dolor.log`, each with its continuation lines."""
    records: list[str] = []
    current = None
    record_start = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d+ - ")
    with open(dolor_log, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            if record_start.match(line):
                current = [line.rstrip("\n")] if " - ERROR - " in line else None
                if current is not None:
                    records.append(current)
            elif current is not None:
                current.append(line.rstrip("\n"))
    return ["\n".join(record) for record in records]


def _cleanup_failure_not_from_oom(record: str) -> bool:
    """Whether a `dolor.log` ERROR record is a teardown failure an accepted OOM does not explain."""
    if not any(m in record for m in CLEANUP_FAILURES):
        return False
    # Only the teardown's own sanitizer check, not another exception raised along with it.
    return not (
        CLEANUP_FAILURES[0] in record
        and CLEANUP_SANITIZER_ASSERT in record
        and "During handling of the above exception" not in record
        and "The above exception was the direct cause" not in record
    )


def _classify_dolor_failure(
    dolor_log: Path,
    passed_as_oom: bool,
    watchdog_expired: bool,
    server_log: Path,
    stderr_log: Path,
    fuzzer_out: Path,
    server_failure_named: bool = False,
):
    """Name why `dolor.py` failed the run, or return None when the log analysis already
    explains it: an OOM it passed the run for, with no teardown failure the OOM does not
    account for, or a server failure it named (`server_failure_named`)."""
    if watchdog_expired:
        return Result(
            name="Watchdog timeout",
            info=f"dolor.py did not finish within {DOLOR_WATCHDOG_SEC // 3600} hours "
            "and was killed by the watchdog. Check fuzzer.log.",
            status=Result.Status.FAIL,
        )
    records = _dolor_failure_records(dolor_log) if dolor_log.is_file() else []
    for marker, name in DOLOR_FAILURES:
        matching = [r for r in records if marker in r]
        if matching:
            return Result(
                name=name, info="\n".join(matching[:20]), status=Result.Status.FAIL
            )
    if server_failure_named:
        # The server-log reports and the non-zero exit below are that failure's.
        return None
    found_in_server_logs = [
        r
        for r in records
        if LOGICAL_ERROR_FAILURE in r
        or (not passed_as_oom and any(m in r for m in SERVER_LOG_FAILURES))
    ]
    if found_in_server_logs:
        # The analysis reads the logs only when the server died, so name it here.
        return _parse_logs(server_log, stderr_log, fuzzer_out) or Result(
            name="Unclassified failure report",
            info="\n".join(found_in_server_logs[:20]),
            status=Result.Status.FAIL,
        )
    if passed_as_oom:
        unexplained = [r for r in records if _cleanup_failure_not_from_oom(r)]
        if not unexplained:
            return None
        return Result(
            name="Cluster shutdown",
            info="The teardown failed for a reason the accepted OOM does not explain.\n"
            + "\n".join(unexplained[:20]),
            status=Result.Status.FAIL,
        )
    return Result(
        name="Unclassified failure",
        info="dolor.py exited with non-zero code but no specific error was identified. "
        "Check fuzzer.log.\n" + "\n".join(records[:20]),
        status=Result.Status.FAIL,
    )


def _all_failures(
    failures: list[Result | None],
    rotated_failure: Result | None,
    workspace_path: Path,
    stderr_log: Path,
    fuzzer_out: Path,
) -> list[Result]:
    """Every failure of the run, each name and stack once: the triage may pass a run on a known
    issue, so it must see them all, also two sharing a name (it keeps only the STID's frames) but
    not the qualified stack. `failures` are those of the current server logs and of `dolor.py`,
    `rotated_failure` the one of the rotated server logs. The parser names one report per log, so
    when a process logged more reports than were named, a failure no issue covers keeps the run red."""
    keeper_failures = _keeper_failures(workspace_path, fuzzer_out)
    distinct: dict[tuple, Result] = {}
    for failure in [*failures, rotated_failure, *keeper_failures.values()]:
        if failure is not None:
            key = (failure.name, FuzzerLogParser.qualified_stack(failure.info))
            distinct.setdefault(key, failure)
    if not distinct:
        return []

    # A failure without a stack, such as one of `dolor.py`, keeps the run red anyway.
    server_err_log = workspace_path / "server.err.log"
    processes = [
        (
            "the server",
            [*_rotated_logs(server_err_log), server_err_log],
            stderr_log,
            FuzzerLogParser.named_reports(f.info for f in failures if f)
            + (rotated_failure is not None),
        )
    ]
    for i in KEEPER_INDEXES:
        _, err_log, keeper_stderr = get_keeper_workspace_logs(workspace_path, i)
        processes.append(
            (f"keeper{i}", [*_rotated_logs(err_log), err_log], keeper_stderr, int(i in keeper_failures))
        )
    result = list(distinct.values())
    if inventory := FuzzerLogParser.incomplete_inventory(processes):
        name, info = inventory
        result.append(Result(name=name, info=info, status=Result.Status.FAIL))
    return result


def _complete_with_artifacts(
    failures: list[Result],
    sw: Utils.Stopwatch,
    workspace_path: Path,
    paths: list[Path],
    info: str = "",
) -> None:
    """Report the failures this wrapper collected, with the artifacts `analyze_job_logs`
    attaches to the failures it reports."""
    result = Result.create_from(
        results=failures, info=info or failures[0].name, stopwatch=sw
    )
    result.set_files(ClickHouseService.collect_cores(workspace_path))
    for file in paths:
        if file.exists() and file.stat().st_size > 0:
            result.set_files(file)
    triage_failure(result, "La Casa del Dolor", paths, TRIAGE_REPRO_HINT)
    result.complete_job()


def main():
    sw = Utils.Stopwatch()
    info = Info()
    args = parse_args()
    is_sanitized = "san" in info.job_name

    if args.param:
        for item in args.param.split(","):
            print(f"Setting env variable: {item}")
            key, _, value = item.partition("=")
            key = key.strip()
            if not key:
                continue
            os.environ[key] = value.strip()

    clickhouse_path = f"{Utils.cwd()}/ci/tmp/clickhouse"
    clickhouse_server_config_dir = f"{Utils.cwd()}/programs/server"
    if info.is_local_run:
        if args.path:
            clickhouse_path = args.path
        else:
            paths_to_check = [
                clickhouse_path,  # it's set for CI runs, but we need to check it
                f"{Utils.cwd()}/build/programs/clickhouse",
                f"{Utils.cwd()}/clickhouse",
            ]
            for path in paths_to_check:
                if Path(path).is_file():
                    clickhouse_path = path
                    break
            else:
                raise FileNotFoundError(
                    "ClickHouse binary not found in any of the paths: "
                    + ", ".join(paths_to_check)
                    + ". You can also specify path to binary via --path argument"
                )
        if args.path_1:
            clickhouse_server_config_dir = args.path_1
    assert Path(
        clickhouse_server_config_dir
    ).exists(), f"ClickHouse config dir does not exist [{clickhouse_server_config_dir}]"
    print(f"Using ClickHouse binary at [{clickhouse_path}]")

    Shell.check(f"chmod +x {clickhouse_path}", verbose=True, strict=True)
    Shell.check(f"{clickhouse_path} --version", verbose=True, strict=True)

    if not Shell.check("docker info > /dev/null", verbose=True):
        _start_docker_in_docker()
    Shell.check("docker info > /dev/null", verbose=True, strict=True)

    # Setup environment variables for tests
    for image_name, env_name in IMAGES_ENV.items():
        tag = info.docker_tag(image_name)
        if tag:
            print(f"Setting environment variable [{env_name}] to [{tag}]")
            os.environ[env_name] = tag
        else:
            assert False, f"No tag found for image [{image_name}]"

    test_env = {
        "CLICKHOUSE_TESTS_BASE_CONFIG_DIR": clickhouse_server_config_dir,
        "CLICKHOUSE_TESTS_SERVER_BIN_PATH": clickhouse_path,
        "CLICKHOUSE_TESTS_CLIENT_BIN_PATH": clickhouse_path,
        "CLICKHOUSE_IS_SANITIZED": "1" if is_sanitized else "0",
    }
    for key, value in test_env.items():
        print(f"Setting environment variable {key} to {value}")
        os.environ[key] = value

    workspace_path = WORKSPACE_PATH
    # Wipe leftovers from a previous seed so a rerun in the same checkout doesn't pick
    # up a stale log or core via the glob-based post-processing below.
    shutil.rmtree(workspace_path, ignore_errors=True)
    workspace_path.mkdir(parents=True, exist_ok=True)

    session_seed = secrets.randbits(64)
    print(f"Using seed {session_seed} for La Casa del Dolor")
    random.seed(session_seed)

    dolor_log = workspace_path / "dolor.log"  # La Casa del Dolor log file
    buzzconfig = workspace_path / "fuzz.json"  # BuzzHouse config file
    # The node's server and stderr logs, the dolor.py output, dmesg and the fatal log, under
    # the names `analyze_job_logs` reads them by
    server_log, fuzzer_log, stderr_log, _, _ = JOB_ARTIFACTS
    buzz_out = workspace_path / "fuzzerout.sql"  # BuzzHouse generated queries
    server_cmd = workspace_path / "server.sh"  # Command line used for La Casa del Dolor
    # Generated configuration file for servers
    config_xml = workspace_path / "config.xml"
    # Generated user configuration file for servers
    users_xml = workspace_path / "users.xml"
    # Query log files for queries sent to other databases
    postgresql_query_log = workspace_path / "postgresql.sql"
    mysql_query_log = workspace_path / "mysql.sql"
    sqlite_query_log = workspace_path / "sqlite.sql"
    mongodb_query_log = workspace_path / "mongodb.doc"
    node_logs = get_node_workspace_logs(workspace_path)
    paths = [
        *JOB_ARTIFACTS,
        buzzconfig,
        buzz_out,
        server_cmd,
        config_xml,
        users_xml,
        dolor_log,
        postgresql_query_log,
        mysql_query_log,
        sqlite_query_log,
        mongodb_query_log,
        Path("./ci/tmp/docker-in-docker.log"),
        *(p for p in node_logs if p not in JOB_ARTIFACTS),
    ]

    # Generate BuzzHouse config, with the budget `dolor.py` gets as its `--timeout`
    generate_buzz_config(workspace_path, str(buzz_out), time_to_run=DOLOR_RUN_MINUTES)

    ctree = ET.parse(f"{repo_dir}/ci/jobs/scripts/server_fuzzer/config.xml")
    croot = ctree.getroot()
    if croot.tag != "clickhouse":
        raise Exception("<clickhouse> element not found")
    # Share the AST fuzzer's experimental-feature gates, skipping what is built below or
    # randomized by `properties.py` (appending those duplicates them or loses the randomization).
    ftree = ET.parse(f"{repo_dir}/ci/jobs/scripts/fuzzer/fuzz-server-settings.xml")
    generated_here = {
        "allow_experimental_transactions",
        "backups",
        "distributed_ddl",
        "keeper_map_path_prefix",
        "named_collections",
        "remote_servers",
        "shared_database_catalog",
        "tmp_path",
    }
    for felement in ftree.getroot():
        # skip comments, whose `tag` is a callable rather than the element name
        if isinstance(felement.tag, str) and felement.tag not in generated_here:
            croot.append(felement)
    # Under sanitizers Keeper can be slow enough that the default 15s ZK session
    # timeout makes DatabaseReplicated abort during reconnect (the worker waits
    # 3 * session_timeout_ms for stale ephemeral nodes to expire). Give it more
    # headroom on sanitizer builds.
    if is_sanitized:
        zk_xml = ET.SubElement(croot, "zookeeper")
        ET.SubElement(zk_xml, "session_timeout_ms").text = "60000"
    remote_servers = ET.SubElement(croot, "remote_servers")
    next_node = ET.SubElement(remote_servers, "cluster0")
    next_shard = ET.SubElement(next_node, "shard")
    next_replica = ET.SubElement(next_shard, "replica")
    ET.SubElement(next_replica, "host").text = "node0"
    ET.SubElement(next_replica, "port").text = "9000"
    ET.indent(ctree, space="    ", level=0)  # indent tree
    ctree.write(config_xml, encoding="utf-8", xml_declaration=True)

    utree = ET.parse(f"{repo_dir}/ci/jobs/scripts/fuzzer/query-fuzzer-tweaks-users.xml")
    uroot = utree.getroot()
    if uroot.tag != "clickhouse":
        raise Exception("<clickhouse> element not found")
    def_profile = uroot.find("./profiles/default")
    if def_profile is None:
        profiles = ET.SubElement(uroot, "profiles")
        def_profile = ET.SubElement(profiles, "default")

    # `query-fuzzer-tweaks-users.xml` sets `ast_fuzzer_runs`, so the server-side AST fuzzer
    # mutates every query sent here too. Pull in the recursion/`max_threads`/sleep caps
    # installed next to that profile. BuzzHouse currently keeps within them on its own, so
    # this is a backstop for whatever either generator starts emitting later.
    ltree = ET.parse(f"{repo_dir}/ci/jobs/scripts/fuzzer/limit-recursion-settings.xml")
    lprofile = ltree.getroot().find("./profiles/default")
    if lprofile is None:
        raise Exception("<profiles><default> element not found")
    constraints = def_profile.find("constraints")
    for lelement in lprofile:
        if not isinstance(lelement.tag, str):
            continue  # skip comments, whose `tag` is a callable rather than the element name
        if lelement.tag != "constraints":
            def_profile.append(lelement)
        elif constraints is None:
            def_profile.append(lelement)
        else:
            constraints.extend(lelement)

    # Set parallel replicas cluster with 75% probability
    if random.randint(1, 4) != 1:
        cluster_preplicas = ET.SubElement(def_profile, "cluster_for_parallel_replicas")
        cluster_preplicas.text = "cluster0"
    ET.indent(utree, space="    ", level=0)  # indent tree
    utree.write(users_xml, encoding="utf-8", xml_declaration=True)

    # Set up and run La Casa del Dolor, for now without Spark and the datalake catalogs
    # Always exercise the SharedMergeTree disk in private CI; a third of the time in
    # public, where `properties.py` only acts on the flag for a private binary anyway.
    set_smt_disk = detect_private_binary(clickhouse_path) or random.randint(1, 3) == 1
    # Disable server restarts and kills on sanitizer builds: a mid-run restart's graceful stop
    # overruns its window (slow sanitizer shutdown) or hangs on an in-flight async backup.
    if is_sanitized:
        restart_args = "--time-between-shutdowns=86400,86400"
        kill_server_prob = 0
        restart_clickhouse_prob = 0
    else:
        restart_args = "--time-between-shutdowns=240,240"
        kill_server_prob = 50
        restart_clickhouse_prob = 75
    base_command = f"""
python3 {repo_dir}/tests/casa_del_dolor/dolor.py --seed={session_seed} --generator=buzzhouse
--tmp-files-dir={workspace_path}
--server-config={config_xml}
--user-config={users_xml}
--client-binary={clickhouse_path}
--server-binaries={clickhouse_path}
--client-config={buzzconfig}
--log-path={dolor_log}
--timeout={DOLOR_RUN_MINUTES} --server-settings-prob=0
--kill-server-prob={kill_server_prob} --without-monitoring --without-transactions
--replica-values=0 --shard-values=1
--add-remote-server-settings-prob=0
--add-disk-settings-prob=80 --number-disks=1,3 --add-policy-settings-prob=70
--add-filesystem-caches-prob=80 --number-caches=1,1
{restart_args} --restart-clickhouse-prob={restart_clickhouse_prob}
--compare-table-dump-prob=0 --set-locales-prob=80 --set-timezones-prob=80
--keeper-settings-prob=0 --mem-limit=32g
{'--set-shared-mergetree-disk' if set_smt_disk else ''}
{'--with-azurite' if random.randint(1, 5) == 1 else ''}
{'--with-postgresql' if random.randint(1, 5) == 1 else ''}
{'--with-mysql' if random.randint(1, 5) == 1 else ''}
{'--with-sqlite' if random.randint(1, 5) == 1 else ''}
{'--with-mongodb' if random.randint(1, 5) == 1 else ''}
{'--with-redis' if random.randint(1, 5) == 1 else ''}
{'--with-nginx' if random.randint(1, 6) == 1 else ''}
2>&1 | tee {fuzzer_log}
"""

    # Wrap with pipefail so the pipe returns dolor.py's exit code, not tee's
    base_command = base_command.replace("\n", " ").strip()
    base_command = f"bash -o pipefail -c {shlex.quote(base_command)}"
    print(f"Using server fuzzer command: {base_command}")
    with open(server_cmd, "w") as outfile:
        outfile.write("#!/bin/bash\n")
        outfile.write(base_command)
        outfile.write("\n")

    # Bounded so a wedged dolor.py doesn't block the runner until the job-level timeout.
    # `Shell.check` only says the command failed; only the elapsed time shows that the
    # watchdog killed it. Its SIGTERM skips dolor.py's teardown, so no log says so either.
    dolor_started = time.monotonic()
    cmd_ok = Shell.check(command=base_command, verbose=True, timeout=DOLOR_WATCHDOG_SEC)
    watchdog_expired = (
        not cmd_ok and time.monotonic() - dolor_started >= DOLOR_WATCHDOG_SEC
    )
    if watchdog_expired:
        print(f"dolor.py was killed by the {DOLOR_WATCHDOG_SEC}s watchdog")

    # Copy generated configuration files from container to host for further analysis.
    # Each `modify_*_settings` helper and the BuzzHouse generator write the *effective*
    # randomized config to a fresh temporary file (`modify_keeper_settings`: one per Keeper
    # node). Attach all of them under stable names next to the base files the job passed.
    for glob_pattern, artifact_fmt in [
        ("buzzhouse_*.json", "buzzhouse.json"),
        ("config_*.xml", "effective_config{}.xml"),
        ("user_*.xml", "effective_users{}.xml"),
        ("keeper_*.xml", "keeper{}.xml"),
    ]:
        for idx, f in enumerate(
            sorted(p for p in Path(workspace_path).glob(glob_pattern) if p.is_file())
        ):
            artifact = workspace_path / artifact_fmt.format(idx)
            if f.resolve() != artifact.resolve():
                shutil.copy2(f, artifact)
            if artifact not in paths:
                paths.append(artifact)
    # Copy logs from container to host. The ones rotated by every restart are attached for
    # reference only: the analysis below reads the logs of the last server.
    for cont_log, host_log in zip(get_node_container_logs(), node_logs):
        if cont_log.exists():
            shutil.copy2(cont_log, host_log)
        elif cont_log.name not in OPTIONAL_NODE_LOGS:
            print(f"WARNING: File {cont_log} already gone!")
        for rotated in cont_log.parent.glob(f"{cont_log.name}.*"):
            if rotated.is_file():
                dst = workspace_path / f"{host_log.name}{rotated.name[len(cont_log.name):]}"
                shutil.copy2(rotated, dst)
                paths.append(dst)
    # The same for the Keepers, which a run without ZooKeeper does not have at all. Their
    # rotations are scanned too: a Keeper restarts after an abort, and its log rotates on.
    for i in KEEPER_INDEXES:
        for cont_log, host_log in zip(
            get_keeper_container_logs(i), get_keeper_workspace_logs(workspace_path, i)
        ):
            if cont_log.is_file():
                shutil.copy2(cont_log, host_log)
                paths.append(host_log)
            for rotated in _rotated_logs(cont_log):
                dst = workspace_path / f"{host_log.name}{rotated.name[len(cont_log.name):]}"
                shutil.copy2(rotated, dst)
                paths.append(dst)

    # Safety net: detect Python-level crashes in the fuzzer log even if the
    # exit code was somehow swallowed (e.g. future command changes drop pipefail)
    if fuzzer_log.exists():
        # Wide enough for an exception that carries the tails of the server's logs, e.g. the
        # one `wait_until_port_is_ready` raises for a server that exited on startup.
        tail = fuzzer_log.read_text(encoding="utf-8", errors="replace")[-20000:]
        if "Traceback (most recent call last):" in tail:
            # A server that aborted (e.g. on a sanitizer report during startup) makes `dolor.py`
            # raise too: name the server's failure rather than the exception it caused.
            if (failure := _parse_logs(server_log, stderr_log, buzz_out)) is not None:
                _complete_with_artifacts(
                    _all_failures(
                        [failure],
                        _rotated_node_failure(workspace_path, buzz_out),
                        workspace_path,
                        stderr_log,
                        buzz_out,
                    ),
                    sw,
                    workspace_path,
                    paths,
                )
            tb_start = tail.rfind("Traceback (most recent call last):")
            tb_snippet = tail[tb_start:].strip()
            Result.create_from(
                results=[
                    Result(
                        name="dolor.py exception",
                        info=f"Python exception in dolor.py:\n{tb_snippet}",
                        status=Result.Status.FAIL,
                    )
                ],
                info="Python exception in dolor.py",
                files=[str(p) for p in paths if p.exists() and p.stat().st_size > 0],
                stopwatch=sw,
            ).complete_job()

    server_died = False
    killed_by_dolor = False
    fuzzer_exit_code = 0
    server_exit_code = 0
    if dolor_log.is_file():
        generator_exit = re.compile(
            r"(?:Load generator|BuzzHouse) exited with code:\s*(-?\d+)"
        )
        died = re.compile(
            r"(?:Logical error|Crash|Sanitizer error) in instance"
        )
        # `dolor.py` inspects the server exec on shutdown and logs its exit code.
        server_exit = re.compile(r"The server \S+ exited with code:\s*(-?\d+)")
        with open(dolor_log, "r", encoding="utf-8", errors="replace") as logf:
            for line in logf:
                if m := generator_exit.search(line):
                    fuzzer_exit_code = int(m.group(1))
                if died.search(line):
                    server_died = True
                if m := server_exit.search(line):
                    server_exit_code = int(m.group(1))
                if FORCED_STOP_MESSAGE in line or START_FAILED_MESSAGE in line:
                    killed_by_dolor = True
    if server_exit_code in GRACEFUL_EXIT_CODES or (
        killed_by_dolor and server_exit_code in SIGKILL_EXIT_CODES
    ):
        # A SIGKILL Dolor sent itself must not pass for a kernel OOM.
        server_exit_code = 0
    else:
        server_died = True
    print(f"Server exit code: {server_exit_code}, generator exit code: {fuzzer_exit_code}")

    copied_cores = _copy_node_cores_to_workspace(workspace_path)
    if copied_cores:
        print(f"Copied {len(copied_cores)} core dump(s) into workspace for encryption")

    result = analyze_job_logs(
        paths,
        server_died,
        server_exit_code,
        fuzzer_exit_code,
        is_sanitized,
        buzz_out,
        server_fuzzer=True,
    )
    failures: list[Result | None] = [r for r in result.results if not r.is_ok()]
    # A server that failed before a restart can be in the rotated logs only, whatever the current
    # ones show, so it is reported whenever they hold a failure.
    rotated_failure = _rotated_node_failure(workspace_path, buzz_out)
    # Unless the analysis failed the run without naming it from the current logs, the rotated
    # failure is another server's, which leaves those logs to be classified below.
    server_failure_named = any(failures) or (
        server_died and not result.is_ok() and rotated_failure is not None
    )
    # An error the analysis names nothing for stays one: the triage never passes it.
    collect = server_failure_named or not result.is_error()
    if collect and not cmd_ok:
        # The analysis reads no log when the generator ended normally and the server
        # survived, and a failure it names may hide another, so whatever `dolor.py` failed
        # the run on is named here.
        failures.append(
            _classify_dolor_failure(
                dolor_log,
                "test considered passed" in result.info
                or "stayed alive" in result.info,
                watchdog_expired,
                server_log,
                stderr_log,
                buzz_out,
                server_failure_named,
            )
        )
    # `dolor.py` can exit 0 with a Keeper log holding a report.
    if collect and (
        failures := _all_failures(
            failures, rotated_failure, workspace_path, stderr_log, buzz_out
        )
    ):
        _complete_with_artifacts(
            failures,
            sw,
            workspace_path,
            paths,
            info=result.info if not result.is_ok() else "",
        )

    triage_failure(result, "La Casa del Dolor", paths, TRIAGE_REPRO_HINT)
    result.complete_job()


if __name__ == "__main__":
    main()
