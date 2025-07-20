#!/usr/bin/env python3
import logging
import os
import subprocess
import sys
from pathlib import Path

from docker_images_helper import DockerImage, get_docker_image, pull_image
from env_helper import REPO_COPY, TEMP_PATH
from report import FAIL, FAILURE, OK, SUCCESS, JobReport, TestResult
from stopwatch import Stopwatch
from tee_popen import TeePopen
from pr_info import PRInfo

IMAGE_NAME = "clickhouse/fuzzer"


def get_run_command(
    workspace_path: Path,
    image: DockerImage,
    check_name: str,
) -> str:
    fuzzer_name = (
        "BuzzHouse" if check_name.lower().startswith("buzzhouse") else "AST Fuzzer"
    )
    envs = [
        f"-e FUZZER_TO_RUN='{fuzzer_name}'",
    ]

    env_str = " ".join(envs)

    return (
        f"docker run "
        # For sysctl
        "--privileged "
        "--network=host "
        "--tmpfs /tmp/clickhouse "
        f"--volume={workspace_path}:/workspace "
        f"--volume={REPO_COPY}:/repo "
        f"{env_str} "
        "--cap-add syslog --cap-add sys_admin --cap-add=SYS_PTRACE --workdir /repo "
        f"{image} "
        "bash -c './ci/jobs/scripts/fuzzer/run-fuzzer.sh' "
    )


def main():
    logging.basicConfig(level=logging.INFO)

    stopwatch = Stopwatch()

    temp_path = Path(TEMP_PATH)
    temp_path.mkdir(parents=True, exist_ok=True)

    check_name = sys.argv[1] if len(sys.argv) > 1 else os.getenv("CHECK_NAME")
    assert (
        check_name
    ), "Check name must be provided as an input arg or in CHECK_NAME env"

    assert (
        Path(REPO_COPY) / "ci/tmp/clickhouse"
    ).exists(), "ClickHouse binary not found"

    docker_image = pull_image(get_docker_image(IMAGE_NAME))

    workspace_path = temp_path / "workspace"
    workspace_path.mkdir(parents=True, exist_ok=True)

    run_command = get_run_command(workspace_path, docker_image, check_name)
    logging.info("Going to run %s", run_command)

    pr_info = PRInfo(need_changed_files=True)
    pr_info.changed_files

    with open(workspace_path / "ci-changed-files.txt", "w") as f:
        f.write("\n".join(pr_info.changed_files))

    run_log_path = temp_path / "run.log"

    with TeePopen(run_command, run_log_path) as process:
        retcode = process.wait()
        if retcode == 0:
            logging.info("Run successfully")
        else:
            logging.info("Run failed")

    subprocess.check_call(f"sudo chown -R ubuntu:ubuntu {temp_path}", shell=True)

    paths = {
        "core.zst": workspace_path / "core.zst",
        "dmesg.log": workspace_path / "dmesg.log",
        "fatal.log": workspace_path / "fatal.log",
        "stderr.log": workspace_path / "stderr.log",
    }

    compressed_server_log_path = workspace_path / "server.log.zst"
    if compressed_server_log_path.exists():
        paths["server.log.zst"] = compressed_server_log_path
    else:
        # The script can fail before the invocation of `zstd`, but we are still interested in its log:
        not_compressed_server_log_path = workspace_path / "server.log"
        if not_compressed_server_log_path.exists():
            paths["server.log"] = not_compressed_server_log_path

    # Same idea but with the fuzzer log
    compressed_fuzzer_log_path = workspace_path / "fuzzer.log.zst"
    if compressed_fuzzer_log_path.exists():
        paths["fuzzer.log.zst"] = compressed_fuzzer_log_path
    else:
        not_compressed_fuzzer_log_path = workspace_path / "fuzzer.log"
        if not_compressed_fuzzer_log_path.exists():
            paths["fuzzer.log"] = not_compressed_fuzzer_log_path

    # Same idea but with the fuzzer output SQL
    compressed_fuzzer_output_sql_path = workspace_path / "fuzzer_out.sql.zst"
    if compressed_fuzzer_output_sql_path.exists():
        paths["fuzzer_out.sql.zst"] = compressed_fuzzer_output_sql_path
    else:
        not_compressed_fuzzer_output_sql_path = workspace_path / "fuzzer_out.sql"
        if not_compressed_fuzzer_output_sql_path.exists():
            paths["fuzzer_out.sql"] = not_compressed_fuzzer_output_sql_path

    # Try to get status message saved by the fuzzer
    try:
        with open(workspace_path / "status.txt", "r", encoding="utf-8") as status_f:
            status = status_f.readline().rstrip("\n")

        with open(workspace_path / "description.txt", "r", encoding="utf-8") as desc_f:
            description = desc_f.readline().rstrip("\n")
    except:
        status = FAILURE
        description = "Task failed: $?=" + str(retcode)

    test_result = TestResult(description, OK)
    if "fail" in status:
        test_result.status = FAIL

    JobReport(
        description=description,
        test_results=[test_result],
        status=status,
        start_time=stopwatch.start_time_str,
        duration=stopwatch.duration_seconds,
        # test generates its own report.html
        additional_files=[v for _, v in paths.items() if Path(v).is_file()],
    ).dump()

    logging.info("Result: '%s', '%s'", status, description)
    if status != SUCCESS:
        sys.exit(1)


if __name__ == "__main__":
    main()
