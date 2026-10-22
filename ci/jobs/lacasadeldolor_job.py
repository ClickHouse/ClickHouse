import argparse
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

from ci.jobs.ast_fuzzer_job import JOB_ARTIFACTS, WORKSPACE_PATH, analyze_job_logs
from ci.jobs.buzzhouse_job import generate_buzz_config
from ci.jobs.ci_utils import is_extended_run
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

# Wall-clock ceiling on `dolor.py`, far above its internal --timeout (30 or 60 minutes) plus
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
    """Copy the core dumps under the Dolor instance directories to `workspace_path`, as
    `ClickHouseService.collect_cores` only inspects `workspace_path/core.*` non-recursively."""
    instances_dir = Path(_dolor_instances_dir())
    if not instances_dir.exists():
        return []
    copied: list[Path] = []
    for src in instances_dir.rglob("core.*"):
        if not src.is_file():
            continue
        relative = src.relative_to(instances_dir).as_posix().replace("/", "_")
        dst = workspace_path / f"core.dolor.{relative}"
        try:
            shutil.copy2(src, dst)
            copied.append(dst)
        except OSError as e:
            print(f"WARNING: failed to copy core dump {src} -> {dst}: {e}")
    return copied


def _parse_logs(server_log: Path, stderr_log: Path, fuzzer_out: Path):
    """The log parser's verdict on one process's logs, or None when it names nothing."""
    if not server_log.is_file():
        return None
    name, description, files = FuzzerLogParser(
        server_log=str(server_log),
        stderr_log=str(stderr_log) if stderr_log.is_file() else "",
        fuzzer_log=str(fuzzer_out),
    ).parse_failure()
    if not name or name == FuzzerLogParser.UNKNOWN_ERROR:
        return None
    return Result(name=name, info=description, status=Result.Status.FAIL, files=files)


def _keeper_failure(workspace_path: Path, fuzzer_out: Path):
    """A failure report in a Keeper log, which nothing else reads."""
    for i in KEEPER_INDEXES:
        _, err_log, stderr_log = get_keeper_workspace_logs(workspace_path, i)
        files = " ".join(str(p) for p in (err_log, stderr_log) if p.is_file())
        # A Fatal other than the watchdog's SIGKILL record, or a raw sanitizer report: one
        # cut short by the abort may be only its `CHECK failed` or `DEADLYSIGNAL` line.
        if files and Shell.get_output(
            "rg --text '<Fatal>|^(==[0-9]+==)?(ERROR|WARNING|SUMMARY): [a-zA-Z]+Sanitizer:"
            f"|[a-zA-Z]+Sanitizer: CHECK failed:|[a-zA-Z]+Sanitizer:DEADLYSIGNAL|: runtime error: ' {files}"
            " | rg --text -v 'Child process was terminated by signal 9'"
        ):
            return _parse_logs(err_log, stderr_log, fuzzer_out) or Result(
                name="Keeper",
                info=f"A failure report in keeper{i} the log parser cannot name. "
                "Check the keeper*.log artifacts.",
                status=Result.Status.FAIL,
            )
    return None


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


def _classify_dolor_failure(
    dolor_log: Path,
    passed_as_oom: bool,
    watchdog_expired: bool,
    server_log: Path,
    stderr_log: Path,
    fuzzer_out: Path,
):
    """Name why `dolor.py` failed a run that the log analysis passed, or return None when
    the analysis already explains it (an OOM it passed the run for)."""
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
        return None
    return Result(
        name="Unclassified failure",
        info="dolor.py exited with non-zero code but no specific error was identified. "
        "Check fuzzer.log.\n" + "\n".join(records[:20]),
        status=Result.Status.FAIL,
    )


def _complete_with_artifacts(
    failure: Result, sw: Utils.Stopwatch, workspace_path: Path, paths: list[Path]
) -> None:
    """Report a failure this wrapper found itself, with the artifacts `analyze_job_logs`
    attaches to the failures it reports."""
    result = Result.create_from(results=[failure], info=failure.name, stopwatch=sw)
    result.set_files(ClickHouseService.collect_cores(workspace_path))
    for file in paths:
        if file.exists() and file.stat().st_size > 0:
            result.set_files(file)
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

    # Generate BuzzHouse config
    generate_buzz_config(workspace_path, str(buzz_out))

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
    # Same budget `generate_buzz_config` gave the BuzzHouse config's `time_to_run`, or on an
    # extended run `dolor.py` kills BuzzHouse halfway through.
    dolor_timeout_minutes = 60 if is_extended_run() else 30
    base_command = f"""
python3 {repo_dir}/tests/casa_del_dolor/dolor.py --seed={session_seed} --generator=buzzhouse
--tmp-files-dir={workspace_path}
--server-config={config_xml}
--user-config={users_xml}
--client-binary={clickhouse_path}
--server-binaries={clickhouse_path}
--client-config={buzzconfig}
--log-path={dolor_log}
--timeout={dolor_timeout_minutes} --server-settings-prob=0
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
    # The same for the Keepers, which a run without ZooKeeper does not have at all.
    for i in KEEPER_INDEXES:
        for cont_log, host_log in zip(
            get_keeper_container_logs(i), get_keeper_workspace_logs(workspace_path, i)
        ):
            if cont_log.is_file():
                shutil.copy2(cont_log, host_log)
                paths.append(host_log)

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
    if result.is_ok():
        # Nothing above reads the Keeper logs, and `dolor.py` can exit 0 with one of them
        # holding a report.
        failure = _keeper_failure(workspace_path, buzz_out)
        if failure is None and not cmd_ok:
            # The analysis reads no log when the generator ended normally and the server
            # survived, so whatever `dolor.py` failed the run on is named here.
            failure = _classify_dolor_failure(
                dolor_log,
                "test considered passed" in result.info
                or "stayed alive" in result.info,
                watchdog_expired,
                server_log,
                stderr_log,
                buzz_out,
            )
        if failure is not None:
            _complete_with_artifacts(failure, sw, workspace_path, paths)

    result.complete_job()


if __name__ == "__main__":
    main()
