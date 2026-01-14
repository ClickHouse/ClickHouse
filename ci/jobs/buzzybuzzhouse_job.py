import argparse
import os
import secrets
import subprocess
import time
from pathlib import Path

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


def main():
    sw = Utils.Stopwatch()
    info = Info()
    args = parse_args()
    job_params = args.options.split(",") if args.options else []
    job_params = [to.strip() for to in job_params]
    use_old_analyzer = False
    use_distributed_plan = False
    use_database_disk = False

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
    server_log = workspace_path / "dolor.log"
    buzzconfig = workspace_path / "fuzz.json"
    # Generate BuzzHouse config
    generate_buzz_config(buzzconfig)

    session_seed = secrets.randbits(64)
    print(f"Using seed {session_seed} for La Casa del Dolor")

    test_results = [
        Result.from_commands_run(
            name="dolor",
            command=f"python3 ./tests/casa_del_dolor/dolor.py --seed={session_seed} --generator=buzzhouse \
                --server-config=./ci/jobs/scripts/server_fuzzer/config.xml \
                --user-config=./ci/jobs/scripts/server_fuzzer/users.xml \
                --client-binary={clickhouse_path} \
                --server-binaries={clickhouse_path} \
                --client-config={buzzconfig} \
                --log-path={server_log} \
                --timeout=30 --server-settings-prob=0 \
                --kill-server-prob=50 --without-monitoring \
                --replica-values=1 --shard-values=1 \
                --add-remote-server-settings-prob=0 \
                --add-disk-settings-prob=99 --number-disks=1,3 --add-policy-settings-prob=80 \
                --add-filesystem-caches-prob=80 --number-caches=1,1 \
                --time-between-shutdowns=180,180 --restart-clickhouse-prob=75 \
                --compare-table-dump-prob=0 --set-locales-prob=80 --set-timezones-prob=80 \
                --keeper-settings-prob=0 --mem-limit=16g --set-shared-mergetree-disk",
        )
    ]

    if not test_results[-1].is_ok():
        pass
        # TODO:
        # died server - lets fetch failure from log
        # fuzzer_log_parser = FuzzerLogParser(
        #     server_log=str(server_log),
        #     stderr_log=str(stderr_log),
        #     fuzzer_log=str(
        #         workspace_path / "fuzzerout.sql" if buzzhouse else fuzzer_log
        #     ),
        # )
        # parsed_name, parsed_info, files = fuzzer_log_parser.parse_failure()

        # if parsed_name:
        #     results.append(
        #         Result(
        #             name=parsed_name,
        #             info=parsed_info,
        #             status=Result.StatusExtended.FAIL,
        #             files=files,
        #         )
        #     )

    attached_files = []
    if not info.is_local_run:
        # TODO: collect needed logs
        if Path("./ci/tmp/docker-in-docker.log").exists():
            attached_files.append("./ci/tmp/docker-in-docker.log")
        print("Dumping dmesg")
        Shell.check("dmesg -T > dmesg.log", verbose=True, strict=True)
        with open("dmesg.log", "rb") as dmesg:
            dmesg = dmesg.read()
            if (
                b"Out of memory: Killed process" in dmesg
                or b"oom_reaper: reaped process" in dmesg
                or b"oom-kill:constraint=CONSTRAINT_NONE" in dmesg
            ):
                test_results.append(
                    Result(
                        name=OOM_IN_DMESG_TEST_NAME, status=Result.StatusExtended.FAIL
                    )
                )
                attached_files.append("dmesg.log")

    R = Result.create_from(results=test_results, stopwatch=sw, files=attached_files)

    R.sort().complete_job()


if __name__ == "__main__":
    main()
