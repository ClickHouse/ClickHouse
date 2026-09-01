import argparse
import os
import random
import re
import secrets
import shlex
import shutil
import subprocess
import time
import traceback
import xml.etree.ElementTree as ET
from pathlib import Path

from ci.jobs.ast_fuzzer_job import (
    SANITIZER_NON_OOM_PATTERN,
    SANITIZER_OOM_PATTERN,
    analyze_job_logs,
)
from ci.jobs.buzzhouse_job import generate_buzz_config
from ci.jobs.scripts.clickhouse_service import ClickHouseService
from ci.jobs.scripts.integration_tests_configs import IMAGES_ENV
from ci.jobs.scripts.log_parser import (
    EXPECTED_KILL_PATTERN,
    SANITIZER_OOM_REPORT_PATTERN,
    FuzzerLogParser,
)
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils
from tests.casa_del_dolor.binary import detect_private_binary

repo_dir = Utils.cwd()
temp_path = f"{repo_dir}/ci/tmp"


# Clean and graceful shutdowns: 0 and `SIGTERM`, as reported by `exec_inspect`.
GRACEFUL_EXIT_CODES = (0, -15, 143)
SIGKILL_EXIT_CODES = (137, -9)

# `dolor.py` logs this and fails the run when `ClickHouseInstance.stop_clickhouse` had to
# escalate a graceful shutdown to a `SIGKILL`. That exit is 137 too, so the code alone
# cannot tell a hung shutdown from a kernel OOM - only this message can.
FORCED_STOP_MESSAGE = "did not shut down gracefully and had to be force killed"
# The other way a stop can fail: `stop_clickhouse` returns with the process still up, so it
# never reaches the force-kill above and logs no message of its own (its broad `except` does
# this). `dolor.py` fails the run on it, and it leaves no exit code to collapse either.
STOP_FAILED_MESSAGE = "is still running after stop command"
# Shared prefix of the two exit-bookkeeping failures in `dolor.py`: the exec could not be
# inspected and no code was recorded, or a server stopped without recording one. Neither
# leaves any other trace in the log, so without this marker they are `good_exit = False`
# reasons the wrapper cannot see, and a benign OOM in the same run would pass them off.
EXIT_UNACCOUNTED_MESSAGE = "Exit code unaccounted for"


def collapse_server_exit_code(
    node_exit_codes: list[int], forced_stop: bool = False
) -> int:
    """Collapse the per-node ClickHouse exit codes into the single code the shared
    `analyze_job_logs` expects.

    Graceful codes are ignored. `SIGKILL` is only reported when every abnormal exit is a
    `SIGKILL` that Dolor did not cause itself, because 137 feeds the kernel-OOM heuristic
    that downgrades the job to success: any other abnormal code must win, or a real crash
    on one node is masked by an OOM on another, and a shutdown Dolor had to force is a
    failure rather than an OOM.
    """
    abnormal = [code for code in node_exit_codes if code not in GRACEFUL_EXIT_CODES]
    non_sigkill = [code for code in abnormal if code not in SIGKILL_EXIT_CODES]
    if non_sigkill:
        return non_sigkill[0]
    if forced_stop:
        return 0
    return 137 if abnormal else 0


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
        if i == retries - 1:
            raise RuntimeError(
                f"Docker daemon did not respond after {retries} attempts"
            )
        time.sleep(2)
    print(f"Started docker-in-docker asynchronously with PID {dockerd_proc.pid}")


def parse_args():
    parser = argparse.ArgumentParser(description="ClickHouse Build Job")
    parser.add_argument("--options", help="Job parameters: ...")
    parser.add_argument(
        "--test",
        help="Optional. Test name patterns (space-separated)",
        default=[],
        nargs="+",
        action="extend",
    )
    parser.add_argument(
        "--count",
        help="Optional. Number of times to repeat each test",
        default=None,
        type=int,
    )
    parser.add_argument(
        "--debug",
        help="Optional. Open python debug console on exception",
        default=False,
        action="store_true",
    )
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
        "--workers",
        help="Optional. Number of parallel workers for pytest",
        default=None,
        type=int,
    )
    parser.add_argument(
        "--param",
        help=(
            "Optional. Comma-separated KEY=VALUE pairs to inject as environment "
            "variables for pytest (e.g. --param PYTEST_ADDOPTS=-vv,CUSTOM_FLAG=1)"
        ),
        type=str,
        default="",
    )
    return parser.parse_args()


def _dolor_instances_dir() -> str:
    """Where `ClickHouseCluster` put the per-node directories for the `dolor` cluster.

    Ask the cluster helper rather than rebuilding the name here: it quotes
    `INTEGRATION_TESTS_RUN_ID`, which `--run-id` passes through verbatim, so any open-coded
    copy silently looks in the wrong place for a run id needing quoting - and drifts again
    the next time the naming changes. Imported lazily because `helpers.cluster` pulls in the
    whole integration-test dependency set, which exists in the runner image (`dolor.py`
    imports it too) but not everywhere this module is imported from.
    """
    from tests.integration.helpers.cluster import get_instances_dir

    return f"{repo_dir}/tests/casa_del_dolor/{get_instances_dir('dolor')}"


# Node logs a healthy run never produces, so their absence is not worth a warning.
# `gdb.log` is only written when `stop_clickhouse` had to force kill a hung server.
OPTIONAL_NODE_LOGS = frozenset({"gdb.log"})


def get_node_container_logs(node_index: int):
    instances_dir = _dolor_instances_dir()
    return [
        # ClickHouse server log file (after final restart)
        Path(f"{instances_dir}/node{node_index}/logs/clickhouse-server.log"),
        # ClickHouse server error log file (after final restart)
        Path(f"{instances_dir}/node{node_index}/logs/clickhouse-server.err.log"),
        # ClickHouse server stdout log file
        Path(f"{instances_dir}/node{node_index}/logs/stdout.log"),
        # ClickHouse server stderr log file
        Path(f"{instances_dir}/node{node_index}/logs/stderr.log"),
        # Backtraces taken by `stop_clickhouse` before it force killed a hung server
        Path(f"{instances_dir}/node{node_index}/logs/gdb.log"),
    ]


def get_node_workspace_logs(workspace_path: Path, node_index: int):
    return [
        # ClickHouse server log file (after final restart)
        workspace_path / f"server{node_index}.log",
        # ClickHouse server error log file (after final restart)
        workspace_path / f"server{node_index}.err.log",
        # ClickHouse server stdout log file
        workspace_path / f"stdout{node_index}.log",
        # ClickHouse server stderr log file
        workspace_path / f"stderr{node_index}.log",
        # Backtraces taken by `stop_clickhouse` before it force killed a hung server
        workspace_path / f"gdb{node_index}.log",
    ]


def _copy_node_cores_to_workspace(workspace_path: Path) -> list[Path]:
    """Find core dumps under the per-node Dolor instance directories and copy them to
    `workspace_path` with unique names. `ClickHouseService.collect_cores` only inspects
    `workspace_path/core.*` non-recursively, so cores produced inside per-node subdirs
    would otherwise be lost. Names start with `core.` so the glob in `collect_cores`
    matches them. Returns the list of copied destinations."""
    instances_dir = Path(_dolor_instances_dir())
    if not instances_dir.exists():
        return []
    copied: list[Path] = []
    for src in instances_dir.rglob("core.*"):
        if not src.is_file():
            continue
        # Build a unique name that includes the relative path so multiple nodes don't collide.
        relative = src.relative_to(instances_dir).as_posix().replace("/", "_")
        dst = workspace_path / f"core.dolor.{relative}"
        try:
            shutil.copy2(src, dst)
            copied.append(dst)
        except OSError as e:
            print(f"WARNING: failed to copy core dump {src} -> {dst}: {e}")
    return copied


def _classify_rotated_logs(
    rotated_logs: list[Path], fuzzer_out: Path, sw: Utils.Stopwatch
) -> tuple[Result | None, bool]:
    """Classify what only the rotated logs hold, as the current logs are classified.

    `analyze_job_logs` judges the current per-node logs alone, on purpose: a sanitizer
    signal in a rotated log can belong to a restart that already finished. `dolor.py` sets
    its exit code from `stderr.log*` and `clickhouse-server.log*` though, so when it fails a
    run the report may live only in a rotated file, and dropping that on the floor turns
    both a benign and a genuine report into the same nondescript wrapper error.

    Returns (failure, is_oom_only): a genuine non-OOM report yields the `FuzzerLogParser`
    verdict for it as a `Result` still to be given the caller's artifacts, a report that is
    only an OOM yields (None, True) so the caller can pass the run, and no report at all
    yields (None, False).
    """
    if not rotated_logs:
        return None, False
    paths_to_scan = " ".join(str(p) for p in rotated_logs)
    # `-z` because rotation gzips all but the newest file, and a report in a `.gz` is
    # exactly the one this looks for. The second `rg` filters the already-decompressed
    # pipe, so it needs no `-z`.
    # `-H` so the surviving lines name their file: which file matched is what the parser
    # has to be pointed at, see below.
    genuine_matches = Shell.get_output(
        f"rg -z --text -H '{SANITIZER_NON_OOM_PATTERN}' {paths_to_scan}"
        f" | rg --text -v '{SANITIZER_OOM_PATTERN}'"
    )
    if genuine_matches:
        print("Genuine failure found in a rotated log")
        # Hand the parser only the files that survived the OOM filter. It defers the expected
        # `Child process was terminated by signal 9 (KILL)` on its own now, but this filter is
        # the wider net, so it still points the parser at the file holding the genuine report.
        by_path = {str(p): p for p in rotated_logs}
        genuine_logs: list[Path] = []
        for line in genuine_matches.splitlines():
            path = by_path.get(line.split(":", 1)[0])
            if path is not None and path not in genuine_logs:
                genuine_logs.append(path)
        if not genuine_logs:
            genuine_logs = rotated_logs
        # Rotated stderr logs are passed as `server_logs`: `parse_failure` searches
        # `stderr_logs + server_logs` for a sanitizer report either way, and none of these
        # files is the current log of a node that `stderr_logs` pairs up by index.
        # `fuzzer_out` is what the reproduce commands are built from, so it goes in here
        # exactly as `analyze_job_logs` passes it for a current-log failure.
        # `genuine_matches` above already found a real report, so let an expected-only line
        # name it if the parser's own patterns disagree with the filter, as before.
        name, description, files = FuzzerLogParser(
            server_logs=genuine_logs, fuzzer_log=fuzzer_out
        ).parse_failure(allow_expected_only=True)
        if not name:
            # Nothing nameable despite the signal - let the caller report its own error.
            return None, False
        return (
            Result.create_from(
                results=[
                    Result(
                        name=name,
                        info=description,
                        status=Result.Status.FAIL,
                        files=files,
                    )
                ],
                info="Failure found only in a rotated log",
                stopwatch=sw,
            ),
            False,
        )
    # Report pattern only: `SANITIZER_OOM_PATTERN` also matches the watchdog's SIGKILL fatal,
    # which Dolor writes on purpose on every killed restart and forced stop, and passing a run
    # needs a real OOM report rather than a line the run was always going to produce.
    if Shell.get_output(
        f"rg -z --text '{SANITIZER_OOM_REPORT_PATTERN}' {paths_to_scan}"
    ):
        return None, True
    return None, False


def _is_expected_kill_only_failure(result: Result) -> bool:
    """True when the only thing `analyze_job_logs` failed on is Dolor's own end-of-run
    `SIGKILL` line.

    `parse_failure(allow_expected_only=True)` names a run after that line when nothing else
    matched, and the name it builds is the matched line itself. Such a verdict identifies
    nothing, so it must not shadow the teardown verdict the caller can report instead.
    """
    if result.is_ok() or not result.results:
        return False
    failing = [sub for sub in result.results if not sub.is_ok()]
    return bool(failing) and all(EXPECTED_KILL_PATTERN in sub.name for sub in failing)


def _has_specific_failure_verdict(
    forced_stop: bool,
    stop_failed: bool,
    generator_early_exit_code: int | None,
    exit_unaccounted: bool = False,
) -> bool:
    """True when `_classify_failed_run` is guaranteed to name the failure itself.

    Only then may an expected-kill-only verdict be handed over for reclassification: each of
    these produces a named failure there, so a nameless verdict is never traded for a vaguer
    one - or for no verdict at all.
    """
    return (
        forced_stop
        or stop_failed
        or generator_early_exit_code is not None
        or exit_unaccounted
    )


def _classify_failed_run(
    result_info: str,
    rotated_logs: list[Path],
    fuzzer_out: Path,
    sw: Utils.Stopwatch,
    forced_stop: bool = False,
    stop_failed: bool = False,
    generator_early_exit_code: int | None = None,
    exit_unaccounted: bool = False,
) -> tuple[Result | None, str | None]:
    """Decide what a non-zero `dolor.py` exit means when `analyze_job_logs` returned OK.

    Returns (failure, info_override): the failure to report in place of the OK verdict, or
    None when the run may pass, plus the `info` to put on that OK verdict when a rotated OOM
    is what passes it.
    """
    # dolor.py exits non-zero on any sanitizer line or unexpected fuzzer exit code,
    # including the ones analyze_job_logs deliberately downgrades to OK (sanitizer or
    # kernel OOM, survived memory limit). Only fail when the OK carries no such verdict.
    benign_downgrade = any(
        marker in result_info
        for marker in (
            "test considered passed",
            "Server hit its memory limit (Code 241) but stayed alive",
        )
    )
    # Whatever dolor.py failed on may live only in a rotated log, which the verdict above
    # never looked at. A genuine report there wins over a benign current-log verdict and is
    # reported by name; one that is only an OOM passes the run the same way
    # `_classify_sanitizer_oom` passes an OOM in a current log.
    failed_result, rotated_oom_only = _classify_rotated_logs(
        rotated_logs, fuzzer_out, sw
    )
    info_override = None
    if rotated_oom_only and not benign_downgrade:
        info_override = (
            "WARNING: Sanitizer OOM in a rotated log - test considered passed"
        )
        benign_downgrade = True
    # A forced shutdown leaves no report anywhere and is a failure in its own right, not
    # something a benign OOM verdict explains: `collapse_server_exit_code` already withheld its
    # 137 from the kernel-OOM heuristic, so it overrides `benign_downgrade` instead of yielding.
    # Both teardown failures go out as a named sub-result, so the report shows a failed
    # row naming what broke instead of only job-level info text next to green rows.
    if failed_result is None and forced_stop:
        failed_result = Result.create_from(
            results=[
                Result(
                    name="Server shutdown",
                    info="A server did not shut down gracefully and had to be force killed. Check fuzzer.log.",
                    status=Result.Status.FAIL,
                )
            ],
            info="A server did not shut down gracefully and had to be force killed",
            stopwatch=sw,
        )
    # A stop that left the server running is the same kind of teardown failure, and it is even
    # less visible: no force-kill message, and no exit code for the node that never stopped.
    if failed_result is None and stop_failed:
        failed_result = Result.create_from(
            results=[
                Result(
                    name="Server shutdown",
                    info="A server was still running after the stop command. Check fuzzer.log.",
                    status=Result.Status.FAIL,
                )
            ],
            info="A server was still running after the stop command",
            stopwatch=sw,
        )
    # The generator dying before cleanup is its own failure, whatever the servers did: an OOM
    # in a rotated log explains a server going away, never the generator process exiting on
    # its own. So it overrides `benign_downgrade` the way the teardown failures above do,
    # rather than being passed off as the benign OOM that happened to be in the same run.
    if failed_result is None and generator_early_exit_code is not None:
        message = (
            f"The load generator exited on its own with code {generator_early_exit_code} "
            "before the run finished"
        )
        failed_result = Result.create_from(
            results=[
                Result(
                    name="Load generator",
                    info=f"{message}. Check fuzzer.log.",
                    status=Result.Status.FAIL,
                )
            ],
            info=message,
            stopwatch=sw,
        )
    # `dolor.py` could not account for how a server exited. That is a teardown failure of its
    # own and no OOM elsewhere explains it, so like the cases above it overrides
    # `benign_downgrade` rather than being passed off by it.
    if failed_result is None and exit_unaccounted:
        message = "Could not account for how a server exited (no exit code recorded)"
        failed_result = Result.create_from(
            results=[
                Result(
                    name="Server shutdown",
                    info=f"{message}. Check fuzzer.log.",
                    status=Result.Status.FAIL,
                )
            ],
            info=message,
            stopwatch=sw,
        )
    if failed_result is None and not benign_downgrade:
        failed_result = Result.create_from(
            results=[
                Result(
                    name="Unclassified failure",
                    info="dolor.py exited with non-zero code but no specific error was identified. Check fuzzer.log.",
                    status=Result.Status.FAIL,
                )
            ],
            info="dolor.py exited with non-zero code but no specific error was identified",
            stopwatch=sw,
        )
    if failed_result is not None:
        # The override only ever labels an OK verdict; printing "test considered passed" on a
        # run this function fails would contradict the report.
        info_override = None
    return failed_result, info_override


def main():
    sw = Utils.Stopwatch()
    info = Info()
    args = parse_args()
    job_params = args.options.split(",") if args.options else []
    job_params = [to.strip() for to in job_params]
    use_old_analyzer = False
    use_distributed_plan = False
    use_database_disk = False
    is_sanitized = "san" in info.job_name

    if args.param:
        for item in args.param.split(","):
            print(f"Setting env variable: {item}")
            key, _, value = item.partition("=")
            key = key.strip()
            if not key:
                continue
            os.environ[key] = value.strip()

    # Resolve the Java binary non-interactively. `update-alternatives --config java` is
    # interactive and unreliable in CI — it can return empty output or just the menu text,
    # leaving JAVA_PATH invalid and silently failing the Spark-dependent branch. Fail fast
    # if Java cannot be located rather than carrying on with an empty path.
    java_path = Shell.get_output("readlink -f /usr/bin/java", verbose=True).strip()
    if not java_path or not Path(java_path).exists():
        raise RuntimeError(
            f"Cannot resolve Java binary: readlink -f /usr/bin/java returned {java_path!r}. "
            "Ensure java is installed in the integration-tests-runner image."
        )

    for to in job_params:
        if to == "old analyzer":
            use_old_analyzer = True
        elif to == "distributed plan":
            use_distributed_plan = True
        elif to == "db disk":
            use_database_disk = True
        else:
            assert False, f"Unknown job option [{to}]"

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
        "CLICKHOUSE_BINARY": clickhouse_path,  # some test cases support alternative binary location
        "CLICKHOUSE_TESTS_CLIENT_BIN_PATH": clickhouse_path,
        "CLICKHOUSE_USE_DATABASE_DISK": "1" if use_database_disk else "0",
        "PYTEST_CLEANUP_CONTAINERS": "1",
        "JAVA_PATH": java_path,
        "CLICKHOUSE_IS_SANITIZED": "1" if is_sanitized else "0",
    }
    # cluster.py enables these two on presence rather than on value, so passing "0" would
    # still write the configs and leave the default analyzer / plan path never fuzzed.
    for name, enabled in (
        ("CLICKHOUSE_USE_OLD_ANALYZER", use_old_analyzer),
        ("CLICKHOUSE_USE_DISTRIBUTED_PLAN", use_distributed_plan),
    ):
        if enabled:
            test_env[name] = "1"
        else:
            os.environ.pop(name, None)

    # Apply environment
    for key, value in (test_env or {}).items():
        print(f"Setting environment variable {key} to {value}")
        os.environ[key] = value

    temp_dir = Path(f"{Utils.cwd()}/ci/tmp/")
    workspace_path = temp_dir / "workspace"
    # Wipe leftovers from a previous seed so a rerun in the same checkout doesn't pick
    # up a stale rotated log or core via the glob-based post-processing below.
    shutil.rmtree(workspace_path, ignore_errors=True)
    workspace_path.mkdir(parents=True, exist_ok=True)

    session_seed = secrets.randbits(64)
    print(f"Using seed {session_seed} for La Casa del Dolor")
    random.seed(session_seed)

    # Set up remote servers configuration for La Casa del Dolor
    number_of_nodes = random.randint(1, 3)

    core_file = workspace_path / "core.zst"  # Core dump file
    dolor_log = workspace_path / "dolor.log"  # La Casa del Dolor log file
    buzzconfig = workspace_path / "fuzz.json"  # BuzzHouse config file
    # La Casa del Dolor stdout and stderr (BuzzHouse output)
    fuzzer_log = workspace_path / "fuzzer.log"
    # dmesg log file
    dmesg_log = workspace_path / "dmesg.log"
    # Fatal log file if ClickHouse server crashes
    buzz_out = workspace_path / "fuzzerout.sql"  # BuzzHouse generated queries
    server_cmd = workspace_path / "server.sh"  # Command line used for La Casa del Dolor
    # Generated configuration file for servers
    config_xml = workspace_path / "config.xml"
    # Generated user configuration file for servers
    users_xml = workspace_path / "users.xml"
    # Generated Keeper configuration files (`keeper_*.xml`, one per Keeper node) are
    # collected after the run under stable `keeper<N>.xml` names, see below.
    # Query log files for queries sent to other databases
    postgresql_query_log = workspace_path / "postgresql.sql"
    mysql_query_log = workspace_path / "mysql.sql"
    sqlite_query_log = workspace_path / "sqlite.sql"
    mongodb_query_log = workspace_path / "mongodb.doc"
    paths = [
        core_file,
        fuzzer_log,
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
        dmesg_log,
    ]
    # Copied server logs from container
    for i in range(number_of_nodes):
        paths.extend(get_node_workspace_logs(workspace_path, i))
        paths.append(workspace_path / f"fatal{i}.log")

    # Generate BuzzHouse config
    generate_buzz_config(workspace_path)

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
    for i in range(number_of_nodes):
        next_node = ET.SubElement(remote_servers, f"cluster{i}")
        next_shard = ET.SubElement(next_node, "shard")
        next_replica = ET.SubElement(next_shard, "replica")
        host = ET.SubElement(next_replica, "host")
        host.text = f"node{i}"
        port = ET.SubElement(next_replica, "port")
        port.text = "9000"
    # Add all nodes cluster with 75% probability. It doesn't follow the `cluster<N>` naming
    # used above on purpose: `properties.py` reads the cluster names from this configuration
    # with `get_cluster_names`, so a cluster can be named after what it holds.
    has_all_cluster = random.randint(1, 4) != 1
    if has_all_cluster:
        next_node = ET.SubElement(remote_servers, "allnodes")
        next_shard = ET.SubElement(next_node, "shard")
        for i in range(number_of_nodes):
            next_replica = ET.SubElement(next_shard, "replica")
            host = ET.SubElement(next_replica, "host")
            host.text = f"node{i}"
            port = ET.SubElement(next_replica, "port")
            port.text = "9000"
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

    # Set parallel replicas cluster
    if has_all_cluster:
        cluster_preplicas = ET.SubElement(def_profile, "cluster_for_parallel_replicas")
        cluster_preplicas.text = "allnodes"
    ET.indent(utree, space="    ", level=0)  # indent tree
    utree.write(users_xml, encoding="utf-8", xml_declaration=True)

    # Set up and run La Casa del Dolor
    # glue/rest/hms catalog connectors require Spark to be configured in BuzzHouse
    # (generators.py only emits catalog config when --with-spark is active), so gate them on it.
    # No spark for now
    with_spark = False  # random.randint(1, 4) == 1
    # Always exercise the SharedMergeTree disk in private CI; a third of the time in
    # public, where `properties.py` only acts on the flag for a private binary anyway.
    set_smt_disk = detect_private_binary(clickhouse_path) or random.randint(1, 3) == 1
    # Sanitizer servers overrun the stop/start windows the restart cycle budgets for (a
    # freshly restarted TSan server needs >30s to stop gracefully, so the final shutdown
    # force kills it and fails the run). Disable all mid-run restarts there: an interval
    # of a day never fires within the 30-minute --timeout.
    restart_args = (
        "--time-between-shutdowns=86400,86400"
        if is_sanitized
        else "--time-between-shutdowns=240,240"
    )
    # No datalake catalogs for now
    # with_glue = with_spark and random.randint(1, 4) == 1
    # with_rest = with_spark and random.randint(1, 4) == 1
    # with_hms = with_spark and random.randint(1, 4) == 1
    base_command = f"""
python3 {repo_dir}/tests/casa_del_dolor/dolor.py --seed={session_seed} --generator=buzzhouse
--tmp-files-dir={workspace_path}
--server-config={config_xml}
--user-config={users_xml}
--client-binary={clickhouse_path}
--server-binaries={clickhouse_path}
--client-config={buzzconfig}
--log-path={dolor_log}
--timeout=30 --server-settings-prob=0
--kill-server-prob=50 --without-monitoring --without-transactions
--replica-values={','.join(str(i) for i in range(number_of_nodes))}
--shard-values={','.join(str(1) for _ in range(number_of_nodes))}
--add-remote-server-settings-prob=0
--add-disk-settings-prob=80 --number-disks=1,3 --add-policy-settings-prob=70
--add-filesystem-caches-prob=80 --number-caches=1,1
{restart_args} --restart-clickhouse-prob=75
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
{'--with-spark' if with_spark else ''}
2>&1 | tee {fuzzer_log}
"""
    # No datalake catalogs for now
    # {'--with-glue' if with_glue else ''}
    # {'--with-rest' if with_rest else ''}
    # {'--with-hms' if with_hms else ''}

    # Wrap with pipefail so the pipe returns dolor.py's exit code, not tee's
    base_command = base_command.replace("\n", " ").strip()
    base_command = f"bash -o pipefail -c {shlex.quote(base_command)}"
    print(f"Using server fuzzer command: {base_command}")
    with open(server_cmd, "w") as outfile:
        outfile.write("#!/bin/bash\n")
        outfile.write(base_command)
        outfile.write("\n")

    # 4-hour wall-clock ceiling so a wedged dolor.py doesn't block the runner until the
    # job-level timeout. Comfortably above the internal --timeout=30 (minutes) plus
    # restarts/setup/shutdown overhead.
    cmd_ok = Shell.check(command=base_command, verbose=True, timeout=4 * 3600)

    # Copy generated configuration files from container to host for further analysis.
    # Each `modify_*_settings` helper and the BuzzHouse generator write the *effective*
    # randomized config to a fresh temporary file (`modify_keeper_settings`: one per Keeper
    # node). Attach all of them under stable names; copying them over the base files the job
    # passed to dolor.py would drop the run's starting point from the report, and a single
    # destination would keep only whichever file the glob happened to visit last.
    # The BuzzHouse config is unnumbered: `BuzzHouseGenerator` is built once per run, so an
    # index would only suggest siblings that never exist.
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
    # Copy logs from container to host, the rotated ones (clickhouse-server.log.0.gz,
    # stderr.log.1.gz, etc.) included: `dolor.py` decides the exit code with
    # `zgrep ... <log>*`, so the glob at server_logs collection has to see the rotated
    # files too. Their names are derived from the same two lists as the live copy, so a
    # log the live copy handles cannot go missing here.
    for i in range(number_of_nodes):
        for cont_log, host_log in zip(
            get_node_container_logs(i), get_node_workspace_logs(workspace_path, i)
        ):
            if cont_log.exists():
                shutil.copy2(cont_log, host_log)
            elif cont_log.name not in OPTIONAL_NODE_LOGS:
                print(f"WARNING: File {cont_log} already gone!")
            for rotated in cont_log.parent.glob(f"{cont_log.name}.*"):
                if not rotated.is_file():
                    continue
                suffix = rotated.name[len(cont_log.name) :]
                dst = workspace_path / f"{host_log.name}{suffix}"
                shutil.copy2(rotated, dst)
                paths.append(dst)

    # Safety net: detect Python-level crashes in the fuzzer log even if the
    # exit code was somehow swallowed (e.g. future command changes drop pipefail)
    if fuzzer_log.exists():
        tail = fuzzer_log.read_text(encoding="utf-8", errors="replace")[-2000:]
        if "Traceback (most recent call last):" in tail:
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
            return

    server_died = False
    forced_stop = False
    stop_failed = False
    exit_unaccounted = False
    fuzzer_exit_code = 0
    generator_early_exit_code: int | None = None
    node_exit_codes: list[int] = []
    try:
        pattern1 = re.compile(
            r"(?:Load generator|BuzzHouse) exited with code:\s*(-?\d+)"
        )
        # Broadened: previously matched only "(Logical error|Crash|Sanitizer error) in instance",
        # which missed OOM kills, raw signals (SEGV/ABRT), and explicit "Server died" messages.
        pattern2 = re.compile(
            r"(?:Logical error|Crash|Sanitizer error) in instance"
            r"|Aborted \(core dumped\)"
            r"|Child process was terminated by signal"
            r"|Out of memory: Killed process"
            r"|Server died"
            r"|Received signal (?:SIGSEGV|SIGABRT|SIGKILL|6|9|11)"
            # A server that vanished leaving no exit information logs no exit code at
            # all, so this is the only evidence it died (dolor.py checks the pid first).
            # Only the stable part of that message is matched, because the tail naming
            # what was missing has already been reworded once.
            r"|is unexpectedly gone"
        )
        # `dolor.py` inspects the ClickHouse exec of every node on shutdown and logs
        # "The server node0 exited with code: 137". Collect those codes so the sanitizer
        # OOM heuristic in `analyze_job_logs` can still see a kernel `SIGKILL`.
        pattern3 = re.compile(r"The server \S+ exited with code:\s*(-?\d+)")
        # `dolor.py` logs this only when the generator died before cleanup could kill it.
        # The numeric code alone cannot show that: a generator killed on purpose reports the
        # codes `validate_exit_code` accepts, and so does one that died on its own. Match the
        # stable middle of the message, since the tail has been reworded before.
        pattern4 = re.compile(r"exited on its own with code\s*(-?\d+)")

        with open(dolor_log, "r", encoding="utf-8") as logf:
            for line in logf:
                m = pattern1.search(line)
                if m:
                    fuzzer_exit_code = int(m.group(1))
                n = pattern2.search(line)
                if n:
                    server_died = True
                e = pattern3.search(line)
                if e:
                    node_exit_codes.append(int(e.group(1)))
                g = pattern4.search(line)
                if g:
                    generator_early_exit_code = int(g.group(1))
                if FORCED_STOP_MESSAGE in line:
                    forced_stop = True
                if EXIT_UNACCOUNTED_MESSAGE in line:
                    exit_unaccounted = True
                if STOP_FAILED_MESSAGE in line:
                    stop_failed = True
    except Exception:
        Result.create_from(
            status=Result.Status.ERROR,
            info=f"Unknown error in fuzzer runner script. Traceback:\n{traceback.format_exc()}",
            files=[str(p) for p in paths if p.exists() and p.stat().st_size > 0],
            stopwatch=sw,
        ).complete_job()
        return

    if forced_stop:
        print("A server had to be force killed on shutdown - not a kernel OOM")
    if stop_failed:
        print("A server was still running after the stop command")
    server_exit_code = collapse_server_exit_code(node_exit_codes, forced_stop)
    # An abnormal exit code IS the server dying, and a pure kernel OOM logs no message
    # the patterns above match, so derive the flag rather than string-match for it.
    # `collapse_server_exit_code` returns 0 for the clean and graceful codes.
    if server_exit_code != 0:
        server_died = True
    if node_exit_codes:
        print(f"Server exit codes: {node_exit_codes}, using {server_exit_code}")

    # Pull any core dumps out of per-node Dolor instance dirs into workspace_path so
    # ClickHouseService.collect_cores (called by analyze_job_logs) can find and encrypt
    # them — its glob is non-recursive and only sees workspace_path/core.* directly.
    copied_cores = _copy_node_cores_to_workspace(workspace_path)
    if copied_cores:
        print(f"Copied {len(copied_cores)} core dump(s) into workspace for encryption")

    # Gather logs to analyze
    server_logs = []
    stderr_logs = []
    error_logs = []
    fatal_logs = []
    for i in range(number_of_nodes):
        log_paths = get_node_workspace_logs(workspace_path, i)
        server_logs.append(log_paths[0])
        error_logs.append(log_paths[1])
        stderr_logs.append(log_paths[3])
        fatal_logs.append(workspace_path / f"fatal{i}.log")
    # Also scan the error and rotated/compressed logs, so an error that only reached
    # clickhouse-server.err.log or was rotated away is still found. These must stay behind
    # the per-node primary logs: analyze_job_logs pairs server_logs with stderr_logs and
    # fatal_logs by index, so `stderr_logs` keeps exactly one entry per node. Appending an
    # error log here only lets the log parser read it - `error_logs` is what puts it in
    # front of the OOM classifier, which sees the per-node slice alone.
    rotated_logs = []
    for i in range(number_of_nodes):
        if error_logs[i].is_file():
            server_logs.append(error_logs[i])
        rotated = sorted(
            [
                p
                for pattern in (
                    f"server{i}.log.*",
                    f"server{i}.err.log.*",
                    f"stderr{i}.log.*",
                )
                for p in workspace_path.glob(pattern)
                if p.is_file()
            ],
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        server_logs.extend(rotated)
        rotated_logs.extend(rotated)

    result = analyze_job_logs(
        paths,
        server_died,
        server_exit_code,
        fuzzer_exit_code,
        is_sanitized,
        buzz_out,
        fuzzer_log,
        dmesg_log,
        server_logs,
        stderr_logs,
        fatal_logs,
        [],
        sw,
        True,
        error_logs=error_logs,
    )
    # A teardown failure, or a generator that died on its own, also reaches here already
    # failing and named after the expected kill line that the forced stop or the end-of-run
    # kill wrote. Let those be reclassified too, but only when there is a specific verdict to
    # put in their place.
    expected_kill_only = _has_specific_failure_verdict(
        forced_stop, stop_failed, generator_early_exit_code, exit_unaccounted
    ) and _is_expected_kill_only_failure(result)
    if not cmd_ok and (result.is_ok() or expected_kill_only):
        failed_result, info_override = _classify_failed_run(
            result.info,
            rotated_logs,
            buzz_out,
            sw,
            forced_stop,
            stop_failed,
            generator_early_exit_code,
            exit_unaccounted,
        )
        if info_override and not expected_kill_only:
            print(info_override)
            result.set_info(info_override)
        if failed_result is not None:
            # `analyze_job_logs` attaches the artifacts only to a failure it reports itself,
            # and it returned OK here, so repeat that finalization for the failure this
            # wrapper reports instead: encrypt the cores and attach every non-empty artifact.
            # Without it the findings this path exists to preserve would go out with a poorer
            # report than every other failing path.
            failed_result.set_files(ClickHouseService.collect_cores(workspace_path))
            for file in paths:
                if file.exists() and file.stat().st_size > 0:
                    failed_result.set_files(file)
            failed_result.complete_job()
            return

    result.complete_job()


if __name__ == "__main__":
    main()
