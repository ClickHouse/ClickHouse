#!/usr/bin/env python3

import logging
import os
import subprocess
import sys
from pathlib import Path

from github import Github

from build_download_helper import get_build_name_for_check, read_build_urls
from clickhouse_helper import (
    CiLogsCredentials,
    ClickHouseHelper,
    prepare_tests_results_for_clickhouse,
)
from commit_status_helper import (
    RerunHelper,
    format_description,
    get_commit,
    post_commit_status,
)
from docker_images_helper import DockerImage, get_docker_image, pull_image
from env_helper import REPORT_PATH, TEMP_PATH
from get_robot_token import get_best_robot_token
from pr_info import PRInfo
from report import TestResult
from s3_helper import S3Helper
from stopwatch import Stopwatch
from tee_popen import TeePopen
from upload_result_helper import upload_results

IMAGE_NAME = "clickhouse/fuzzer"


def get_run_command(
    pr_info: PRInfo,
    build_url: str,
    workspace_path: Path,
    ci_logs_args: str,
    image: DockerImage,
) -> str:
    envs = [
        f"-e PR_TO_TEST={pr_info.number}",
        f"-e SHA_TO_TEST={pr_info.sha}",
        f"-e BINARY_URL_TO_DOWNLOAD='{build_url}'",
    ]

    env_str = " ".join(envs)

    return (
        f"docker run "
        # For sysctl
        "--privileged "
        "--network=host "
        f"{ci_logs_args}"
        f"--volume={workspace_path}:/workspace "
        f"{env_str} "
        "--cap-add syslog --cap-add sys_admin --cap-add=SYS_PTRACE "
        f"{image}"
    )


def main():
    logging.basicConfig(level=logging.INFO)

    stopwatch = Stopwatch()

    temp_path = Path(TEMP_PATH)
    reports_path = Path(REPORT_PATH)
    temp_path.mkdir(parents=True, exist_ok=True)

    check_name = sys.argv[1] if len(sys.argv) > 1 else os.getenv("CHECK_NAME")
    assert (
        check_name
    ), "Check name must be provided as an input arg or in CHECK_NAME env"

    pr_info = PRInfo()

    gh = Github(get_best_robot_token(), per_page=100)
    commit = get_commit(gh, pr_info.sha)

    rerun_helper = RerunHelper(commit, check_name)
    if rerun_helper.is_already_finished_by_status():
        logging.info("Check is already finished according to github status, exiting")
        sys.exit(0)

    docker_image = pull_image(get_docker_image(IMAGE_NAME))

    build_name = get_build_name_for_check(check_name)
    urls = read_build_urls(build_name, reports_path)
    if not urls:
        raise Exception("No build URLs found")

    for url in urls:
        if url.endswith("/clickhouse"):
            build_url = url
            break
    else:
        raise Exception("Cannot find the clickhouse binary among build results")

    logging.info("Got build url %s", build_url)

    workspace_path = temp_path / "workspace"
    workspace_path.mkdir(parents=True, exist_ok=True)

    ci_logs_credentials = CiLogsCredentials(temp_path / "export-logs-config.sh")
    ci_logs_args = ci_logs_credentials.get_docker_arguments(
        pr_info, stopwatch.start_time_str, check_name
    )

    run_command = get_run_command(
        pr_info,
        build_url,
        workspace_path,
        ci_logs_args,
        docker_image,
    )
    logging.info("Going to run %s", run_command)

    run_log_path = temp_path / "run.log"
    main_log_path = workspace_path / "main.log"

    with TeePopen(run_command, run_log_path) as process:
        retcode = process.wait()
        if retcode == 0:
            logging.info("Run successfully")
        else:
            logging.info("Run failed")

    subprocess.check_call(f"sudo chown -R ubuntu:ubuntu {temp_path}", shell=True)
    ci_logs_credentials.clean_ci_logs_from_credentials(run_log_path)

    check_name_lower = (
        check_name.lower().replace("(", "").replace(")", "").replace(" ", "")
    )
    s3_prefix = f"{pr_info.number}/{pr_info.sha}/fuzzer_{check_name_lower}/"
    paths = {
        "run.log": run_log_path,
        "main.log": main_log_path,
        "fuzzer.log": workspace_path / "fuzzer.log",
        "report.html": workspace_path / "report.html",
        "core.zst": workspace_path / "core.zst",
        "dmesg.log": workspace_path / "dmesg.log",
    }

    compressed_server_log_path = workspace_path / "server.log.zst"
    if compressed_server_log_path.exists():
        paths["server.log.zst"] = compressed_server_log_path

    # The script can fail before the invocation of `zstd`, but we are still interested in its log:

    not_compressed_server_log_path = workspace_path / "server.log"
    if not_compressed_server_log_path.exists():
        paths["server.log"] = not_compressed_server_log_path

    s3_helper = S3Helper()
    urls = []
    report_url = ""
    for file, path in paths.items():
        try:
            url = s3_helper.upload_test_report_to_s3(path, s3_prefix + file)
            report_url = url if file == "report.html" else report_url
            urls.append(url)
        except Exception as ex:
            logging.info("Exception uploading file %s text %s", file, ex)

    # Try to get status message saved by the fuzzer
    try:
        with open(workspace_path / "status.txt", "r", encoding="utf-8") as status_f:
            status = status_f.readline().rstrip("\n")

        with open(workspace_path / "description.txt", "r", encoding="utf-8") as desc_f:
            description = desc_f.readline().rstrip("\n")
    except:
        status = "failure"
        description = "Task failed: $?=" + str(retcode)

    description = format_description(description)

    test_result = TestResult(description, "OK")
    if "fail" in status:
        test_result.status = "FAIL"

    if not report_url:
        report_url = upload_results(
            s3_helper,
            pr_info.number,
            pr_info.sha,
            [test_result],
            [],
            check_name,
            urls,
        )

    ch_helper = ClickHouseHelper()

    prepared_events = prepare_tests_results_for_clickhouse(
        pr_info,
        [test_result],
        status,
        stopwatch.duration_seconds,
        stopwatch.start_time_str,
        report_url,
        check_name,
    )

    ch_helper.insert_events_into(db="default", table="checks", events=prepared_events)

    logging.info("Result: '%s', '%s', '%s'", status, description, report_url)
    print(f"::notice ::Report url: {report_url}")
    post_commit_status(commit, status, report_url, description, check_name, pr_info)


if __name__ == "__main__":
    main()
