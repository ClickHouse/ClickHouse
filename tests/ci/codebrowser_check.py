#!/usr/bin/env python3


import logging
import os
from pathlib import Path

from github import Github

from commit_status_helper import get_commit, post_commit_status
from docker_pull_helper import get_image_with_version, DockerImage
from env_helper import (
    IMAGES_PATH,
    REPO_COPY,
    S3_DOWNLOAD,
    S3_BUILDS_BUCKET,
    S3_TEST_REPORTS_BUCKET,
    TEMP_PATH,
)
from get_robot_token import get_best_robot_token
from pr_info import PRInfo
from report import TestResult
from s3_helper import S3Helper
from stopwatch import Stopwatch
from tee_popen import TeePopen
from upload_result_helper import upload_results

NAME = "Woboq Build"


def get_run_command(
    repo_path: Path, output_path: Path, image: DockerImage, sha: str
) -> str:
    user = f"{os.geteuid()}:{os.getegid()}"
    cmd = (
        f"docker run --rm --user={user} --volume={repo_path}:/build "
        f"--volume={output_path}:/workdir/output --network=host "
        # use sccache, https://github.com/KDAB/codebrowser/issues/111
        f"-e SCCACHE_BUCKET='{S3_BUILDS_BUCKET}' "
        "-e SCCACHE_S3_KEY_PREFIX=ccache/sccache "
        '-e CMAKE_FLAGS="$CMAKE_FLAGS -DCOMPILER_CACHE=sccache" '
        f"-e 'DATA={S3_DOWNLOAD}/{S3_TEST_REPORTS_BUCKET}/codebrowser/data' "
        f"-e SHA={sha} {image}"
    )
    return cmd


def main():
    logging.basicConfig(level=logging.INFO)

    stopwatch = Stopwatch()

    gh = Github(get_best_robot_token(), per_page=100)
    pr_info = PRInfo()
    commit = get_commit(gh, pr_info.sha)
    temp_path = Path(TEMP_PATH)

    if not temp_path.exists():
        os.makedirs(temp_path)

    docker_image = get_image_with_version(IMAGES_PATH, "clickhouse/codebrowser")
    # FIXME: the codebrowser is broken with clang-16, workaround with clang-15
    # See https://github.com/ClickHouse/ClickHouse/issues/50077
    docker_image.version = "49701-4dcdcf4c11b5604f1c5d3121c9c6fea3e957b605"
    s3_helper = S3Helper()

    result_path = temp_path / "result_path"
    if not result_path.exists():
        os.makedirs(result_path)

    run_command = get_run_command(
        Path(REPO_COPY), result_path, docker_image, pr_info.sha[:12]
    )

    logging.info("Going to run codebrowser: %s", run_command)

    run_log_path = result_path / "run.log"

    state = "success"
    with TeePopen(run_command, run_log_path) as process:
        retcode = process.wait()
        if retcode == 0:
            logging.info("Run successfully")
        else:
            logging.info("Run failed")
            state = "failure"

    report_path = result_path / "html_report"
    logging.info("Report path %s", report_path)

    s3_path_prefix = "codebrowser"
    index_template = (
        f'<a href="{S3_DOWNLOAD}/{S3_TEST_REPORTS_BUCKET}/{s3_path_prefix}/index.html">'
        "{}</a>"
    )
    additional_logs = [path.absolute() for path in result_path.glob("*.log")]
    test_results = [
        TestResult(
            index_template.format("Generate codebrowser site"),
            state,
            stopwatch.duration_seconds,
            additional_logs,
        )
    ]

    if state == "success":
        stopwatch.reset()
        _ = s3_helper.fast_parallel_upload_dir(
            report_path, s3_path_prefix, S3_TEST_REPORTS_BUCKET
        )
        test_results.append(
            TestResult(
                index_template.format("Upload codebrowser site"),
                state,
                stopwatch.duration_seconds,
            )
        )

    # Check if the run log contains `FATAL Error:`, that means the code problem
    stopwatch.reset()
    fatal_error = "FATAL Error:"
    logging.info("Search for '%s' in %s", fatal_error, run_log_path)
    with open(run_log_path, "r", encoding="utf-8") as rlfd:
        for line in rlfd.readlines():
            if "FATAL Error:" in line:
                logging.warning(
                    "The line '%s' found, mark the run as failure", fatal_error
                )
                state = "failure"
                test_results.append(
                    TestResult(
                        "Indexing error",
                        state,
                        stopwatch.duration_seconds,
                        additional_logs,
                    )
                )
                break

    report_url = upload_results(
        s3_helper, pr_info.number, pr_info.sha, test_results, [], NAME
    )

    print(f"::notice ::Report url: {report_url}")

    post_commit_status(commit, state, report_url, "Report built", NAME, pr_info)


if __name__ == "__main__":
    main()
