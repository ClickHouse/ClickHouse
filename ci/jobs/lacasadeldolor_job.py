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

from ci.jobs.ast_fuzzer_job import analyze_job_logs
from ci.jobs.scripts.integration_tests_configs import IMAGES_ENV
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils
from buzzhouse_job import generate_buzz_config

repo_dir = Utils.cwd()
temp_path = f"{repo_dir}/ci/tmp"
MAX_FAILS_BEFORE_DROP = 5
OOM_IN_DMESG_TEST_NAME = "OOM in dmesg"


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
                f"Docker daemon didn't responded after {retries} attempts"
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


def get_node_container_logs(node_index: int):
    return [
        # ClickHouse server log file (after final restart)
        Path(
            f"{repo_dir}/tests/casa_del_dolor/_instances-dolor/node{node_index}/logs/clickhouse-server.log"
        ),
        # ClickHouse server error log file (after final restart)
        Path(
            f"{repo_dir}/tests/casa_del_dolor/_instances-dolor/node{node_index}/logs/clickhouse-server.err.log"
        ),
        # ClickHouse server stdout log file
        Path(
            f"{repo_dir}/tests/casa_del_dolor/_instances-dolor/node{node_index}/logs/stdout.log"
        ),
        # ClickHouse server stderr log file
        Path(
            f"{repo_dir}/tests/casa_del_dolor/_instances-dolor/node{node_index}/logs/stderr.log"
        ),
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
    ]


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

    java_path = Shell.get_output(
        "update-alternatives --config java | sed -n 's/.*(providing \/usr\/bin\/java): //p'",
        verbose=True,
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
                    "Clickhouse binary not found in any of the paths: "
                    + ", ".join(paths_to_check)
                    + ". You can also specify path to binary via --path argument"
                )
        if args.path_1:
            clickhouse_server_config_dir = args.path_1
    assert Path(
        clickhouse_server_config_dir
    ), f"Clickhouse config dir does not exist [{clickhouse_server_config_dir}]"
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
        "CLICKHOUSE_USE_OLD_ANALYZER": "1" if use_old_analyzer else "0",
        "CLICKHOUSE_USE_DISTRIBUTED_PLAN": "1" if use_distributed_plan else "0",
        "CLICKHOUSE_USE_DATABASE_DISK": "1" if use_database_disk else "0",
        "PYTEST_CLEANUP_CONTAINERS": "1",
        "JAVA_PATH": java_path,
    }
    # Apply environment
    for key, value in (test_env or {}).items():
        print(f"Setting environment variable {key} to {value}")
        os.environ[key] = value

    temp_dir = Path(f"{Utils.cwd()}/ci/tmp/")
    workspace_path = temp_dir / "workspace"
    workspace_path.mkdir(parents=True, exist_ok=True)

    session_seed = secrets.randbits(64)
    print(f"Using seed {session_seed} for La Casa del Dolor")

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
    remote_servers = ET.SubElement(croot, "remote_servers")
    for i in range(number_of_nodes):
        next_node = ET.SubElement(remote_servers, f"cluster{i}")
        next_shard = ET.SubElement(next_node, "shard")
        next_replica = ET.SubElement(next_shard, "replica")
        host = ET.SubElement(next_replica, "host")
        host.text = f"node{i}"
        port = ET.SubElement(next_replica, "port")
        port.text = "9000"
    # Add all nodes cluster with 75% probability
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

    # Set parallel replicas cluster
    utree = ET.parse(f"{repo_dir}/ci/jobs/scripts/server_fuzzer/users.xml")
    if has_all_cluster:
        uroot = utree.getroot()
        if uroot.tag != "clickhouse":
            raise Exception("<clickhouse> element not found")
        profiles = ET.SubElement(uroot, "profiles")
        def_profile = ET.SubElement(profiles, "default")
        cluster_preplicas = ET.SubElement(def_profile, "cluster_for_parallel_replicas")
        cluster_preplicas.text = "allnodes"
    ET.indent(utree, space="    ", level=0)  # indent tree
    utree.write(users_xml, encoding="utf-8", xml_declaration=True)

    # Set up and run La Casa del Dolor
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
--kill-server-prob=50 --without-monitoring
--replica-values={','.join(str(i) for i in range(number_of_nodes))}
--shard-values={','.join(str(1) for _ in range(number_of_nodes))}
--add-remote-server-settings-prob=0
--add-disk-settings-prob=80 --number-disks=1,3 --add-policy-settings-prob=70
--add-filesystem-caches-prob=80 --number-caches=1,1
--time-between-shutdowns=240,240 --restart-clickhouse-prob=75
--compare-table-dump-prob=0 --set-locales-prob=80 --set-timezones-prob=80
--keeper-settings-prob=0 --mem-limit=16g --set-shared-mergetree-disk
{'--with-azurite' if random.randint(1, 5) == 1 else ''}
{'--with-postgresql' if random.randint(1, 5) == 1 else ''}
{'--with-mysql' if random.randint(1, 5) == 1 else ''}
{'--with-sqlite' if random.randint(1, 5) == 1 else ''}
{'--with-mongodb' if random.randint(1, 5) == 1 else ''}
{'--with-redis' if random.randint(1, 5) == 1 else ''}
{'--with-nginx' if random.randint(1, 6) == 1 else ''}
{'--with-spark' if random.randint(1, 4) == 1 else ''}
{'--with-glue' if random.randint(1, 4) == 1 else ''}
{'--with-rest' if random.randint(1, 4) == 1 else ''}
{'--with-hms' if random.randint(1, 4) == 1 else ''}
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

    cmd_ok = Shell.check(command=base_command, verbose=True)

    # Copy generated configuration files from container to host for further analysis
    for pattern in [
        ("buzzhouse_*.json", buzzconfig),
        ("user_*.xml", users_xml),
        ("config_*.xml", config_xml),
    ]:
        for f in Path(workspace_path).glob(pattern[0]):
            shutil.copy2(f, pattern[1])
    # Copy logs from container to host
    for i in range(number_of_nodes):
        for cont_log, host_log in zip(
            get_node_container_logs(i), get_node_workspace_logs(workspace_path, i)
        ):
            if cont_log.exists():
                shutil.copy2(cont_log, host_log)
            else:
                print(f"WARNING: File {cont_log} already gone!")

    # Safety net: detect Python-level crashes in the fuzzer log even if the
    # exit code was somehow swallowed (e.g. future command changes drop pipefail)
    if not cmd_ok and fuzzer_log.exists():
        tail = fuzzer_log.read_text(encoding="utf-8", errors="replace")[-2000:]
        if "Traceback (most recent call last):" in tail:
            tb_start = tail.rfind("Traceback (most recent call last):")
            tb_snippet = tail[tb_start:].strip()
            Result.create_from(
                status=Result.Status.FAILED,
                info=f"Python exception in dolor.py:\n{tb_snippet}",
                files=[str(p) for p in paths if p.exists() and p.stat().st_size > 0],
                stopwatch=sw,
            ).complete_job()
            return

    server_died = False
    fuzzer_exit_code = 0
    try:
        pattern1 = re.compile(r"Load generator exited with code:\s*(\d+)")
        pattern2 = re.compile(r"(Logical error|Crash|Sanitizer error) in instance")

        with open(dolor_log, "r", encoding="utf-8") as logf:
            for line in logf:
                m = pattern1.search(line)
                if m:
                    fuzzer_exit_code = int(m.group(1))
                n = pattern2.search(line)
                if n:
                    server_died = True
    except Exception:
        Result.create_from(
            status=Result.Status.ERROR,
            info=f"Unknown error in fuzzer runner script. Traceback:\n{traceback.format_exc()}",
            files=[str(p) for p in paths if p.exists() and p.stat().st_size > 0],
            stopwatch=sw,
        ).complete_job()
        return

    # Gather logs to analyze
    server_logs = []
    stderr_logs = []
    fatal_logs = []
    for i in range(number_of_nodes):
        log_paths = get_node_workspace_logs(workspace_path, i)
        server_logs.append(log_paths[0])
        stderr_logs.append(log_paths[3])
        fatal_logs.append(workspace_path / f"fatal{i}.log")

    result = analyze_job_logs(
        paths,
        server_died,
        fuzzer_exit_code,
        is_sanitized,
        buzz_out,
        fuzzer_log,
        dmesg_log,
        server_logs,
        stderr_logs,
        fatal_logs,
        sw,
    )
    if not cmd_ok and result.is_ok():
        Result.create_from(
            status=Result.Status.FAILED,
            info="dolor.py exited with non-zero code but no specific error was identified. Check fuzzer.log.",
            files=[str(p) for p in paths if p.exists() and p.stat().st_size > 0],
            stopwatch=sw,
        ).complete_job()
        return

    result.complete_job()


if __name__ == "__main__":
    main()
