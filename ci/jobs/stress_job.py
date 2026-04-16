import csv
import logging
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import List, Tuple

from ci.jobs.scripts.docker_image import DockerImage
from ci.jobs.scripts.log_parser import FuzzerLogParser
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils


class SensitiveFormatter(logging.Formatter):
    @staticmethod
    def _filter(s):
        return re.sub(
            r"(.*)(AZURE_CONNECTION_STRING.*\')(.*)", r"\1AZURE_CONNECTION_STRING\3", s
        )

    def format(self, record):
        original = logging.Formatter.format(self, record)
        return self._filter(original)


def read_test_results(results_path: Path, with_raw_logs: bool = True):
    results = []
    with open(results_path, "r", encoding="utf-8") as descriptor:
        reader = csv.reader(descriptor, delimiter="\t")
        for line in reader:
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
            assert (
                result.is_ok() or result.is_failure or result.is_error
            ), f"Unexpected status [{result.status}]"
            if len(line) == 4 and line[3]:
                # The value can be empty, but when it's not,
                # the 4th value is a pythonic list, e.g. ['file1', 'file2']
                if with_raw_logs:
                    # Python does not support TSV, so we unescape manually
                    result.set_info(line[3].replace("\\t", "\t").replace("\\n", "\n"))
                else:
                    result.set_info(line[3])
            results.append(result)
    return results


def get_additional_envs(info, check_name: str) -> List[str]:
    from ci.jobs.ci_utils import is_extended_run

    result = []
    if not info.is_local_run:
        azure_connection_string = Shell.get_output(
            f"aws ssm get-parameter --region us-east-1 --name azure_connection_string --with-decryption --output text --query Parameter.Value",
            verbose=True,
            strict=True,
        )
        result.append(f"AZURE_CONNECTION_STRING='{azure_connection_string}'")
    # some cloud-specific features require feature flags enabled
    # so we need this ENV to be able to disable the randomization
    # of feature flags
    result.append("RANDOMIZE_KEEPER_FEATURE_FLAGS=1")
    if "azure" in check_name:
        result.append("USE_AZURE_STORAGE_FOR_MERGE_TREE=1")

    if "s3" in check_name:
        result.append("USE_S3_STORAGE_FOR_MERGE_TREE=1")

    result.append(
        f"STRESS_GLOBAL_TIME_LIMIT={'3600' if is_extended_run() else '1200'}"
    )

    return result


def get_run_command(
    build_path: Path,
    result_path: Path,
    repo_tests_path: Path,
    server_log_path: Path,
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
        # a static link, don't use S3_URL or S3_DOWNLOAD
        "-e S3_URL='https://s3.amazonaws.com/clickhouse-datasets' "
        "--tmpfs /tmp/clickhouse:mode=1777 "
        f"--volume={build_path}:/package_folder "
        f"--volume={result_path}:/test_output "
        f"--volume={repo_tests_path}/..:/repo "
        f"--volume={server_log_path}:/var/log/clickhouse-server {env_str} {image} {run_script}"
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

    try:
        results_path = result_directory / "test_results.tsv"
        test_results = read_test_results(results_path, True)
        if len(test_results) == 0:
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

    return test_results, additional_files


def run_stress_test(upgrade_check: bool = False) -> None:
    info = Info()
    logging.basicConfig(level=logging.INFO)
    for handler in logging.root.handlers:
        # pylint: disable=protected-access
        handler.setFormatter(SensitiveFormatter(handler.formatter._fmt))  # type: ignore

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
    server_log_path.mkdir(parents=True, exist_ok=True)

    result_path = temp_path / "result_path"
    result_path.mkdir(parents=True, exist_ok=True)

    additional_envs = get_additional_envs(info, check_name)

    run_command = get_run_command(
        packages_path,
        result_path,
        repo_tests_path,
        server_log_path,
        additional_envs,
        docker_image,
        upgrade_check,
    )
    logging.info("Going to run stress test: %s", run_command)

    exit_code = Shell.run(run_command)

    subprocess.check_call(f"sudo chown -R ubuntu:ubuntu {temp_path}", shell=True)

    is_oom = False

    if Path(result_path / "dmesg.log").is_file():
        is_oom = Shell.check(
            "grep -q -F -e 'Out of memory: Killed process' -e 'oom_reaper: reaped process' -e 'oom-kill:constraint=CONSTRAINT_NONE' "
            f"{result_path}/dmesg.log"
        )

    # Check for OOM (signal 9) in server logs
    if server_log_path.exists():
        server_log_oom = Shell.check(
            f"rg -Fqa ' <Fatal> Application: Child process was terminated by signal 9' "
            f"{server_log_path}/clickhouse-server*.log"
        )
        is_oom = is_oom or server_log_oom

    test_results, additional_logs = process_results(result_path, server_log_path)

    server_died = False
    failed_results = []
    for test_result in test_results:
        if test_result.name == "Server died":
            # This result from stress.py indicates a server crash - we use it as a flag
            # to trigger detailed log parsing below, but don't include it in the CI report
            # since we'll create a more informative result from the parsed logs
            server_died = True
        elif not test_result.is_ok():
            failed_results.append(test_result)

    if server_died:
        server_err_log = server_log_path / "clickhouse-server.err.log"
        stderr_log = result_path / "stderr.log"
        if not (server_err_log.exists() and stderr_log.exists()):
            failed_results.append(
                Result.create_from(
                    name="Unknown error",
                    info="no server logs found",
                    status=Result.Status.FAILED,
                )
            )
        else:
            log_parser = FuzzerLogParser(
                server_log=server_err_log,
                stderr_log=stderr_log if stderr_log.exists() else "",
                fuzzer_log="",
            )
            try:
                name, description, files = log_parser.parse_failure()
                failed_results.append(
                    Result.create_from(
                        name=name,
                        info=description,
                        status=Result.StatusExtended.FAIL,
                        files=files,
                    )
                )
            except Exception as e:
                print(
                    f"ERROR: Failed to parse failure logs: {e}\nServer logs should still be collected."
                )
                failed_results.append(
                    Result.create_from(
                        name="Parse failure error",
                        info=f"Error parsing failure logs: {e}",
                        status=Result.Status.FAILED,
                    )
                )

    if exit_code != 0:
        failed_results.append(
            Result.create_from(
                name="Check failed",
                info=f"Check failed with exit code {exit_code}",
                status=Result.Status.FAILED,
            )
        )

    r = Result.create_from(
        results=failed_results,
        status=Result.Status.SUCCESS if not failed_results else "",
        stopwatch=stopwatch,
    )
    if not r.is_ok() and is_oom:
        r.set_status(Result.Status.SUCCESS)
        r.set_info("OOM error (allowed in stress tests)")

    if r.is_ok() and exit_code != 0 and not is_oom:
        r.set_failed().set_info(
            f"Unknown error: Test script failed with exit code {exit_code}"
        )

    r.set_files(additional_logs).complete_job()


if __name__ == "__main__":
    run_stress_test()
