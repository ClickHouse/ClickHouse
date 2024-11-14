#!/usr/bin/env python3
import argparse
import csv
import logging
import os
import shutil
import subprocess
import sys
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import List, Tuple, Union

import magic

from docker_images_helper import get_docker_image, pull_image
from env_helper import GITHUB_EVENT_PATH, IS_CI, REPO_COPY, TEMP_PATH
from git_helper import GIT_PREFIX, git_runner
from pr_info import PRInfo
from report import (
    ERROR,
    FAIL,
    FAILURE,
    SUCCESS,
    JobReport,
    TestResults,
    read_test_results,
)
from ssh import SSHKey
from stopwatch import Stopwatch


def process_result(
    result_directory: Path,
) -> Tuple[str, str, TestResults, List[Path]]:
    test_results = []  # type: TestResults
    additional_files = []
    # Just upload all files from result_directory.
    # If task provides processed results, then it's responsible
    # for content of result_directory.
    if result_directory.exists():
        additional_files = [p for p in result_directory.iterdir() if p.is_file()]

    status = []
    status_path = result_directory / "check_status.tsv"
    if status_path.exists():
        logging.info("Found check_status.tsv")
        with open(status_path, "r", encoding="utf-8") as status_file:
            status = list(csv.reader(status_file, delimiter="\t"))
    if len(status) != 1 or len(status[0]) != 2:
        logging.info("Files in result folder %s", os.listdir(result_directory))
        return ERROR, "Invalid check_status.tsv", test_results, additional_files
    state, description = status[0][0], status[0][1]

    try:
        results_path = result_directory / "test_results.tsv"
        test_results = read_test_results(results_path)
        if len(test_results) == 0:
            raise ValueError("Empty results")

        return state, description, test_results, additional_files
    except Exception:
        if state == SUCCESS:
            state, description = ERROR, "Failed to read test_results.tsv"
        return state, description, test_results, additional_files


def parse_args():
    parser = argparse.ArgumentParser("Check and report style issues in the repository")
    parser.add_argument("--push", default=True, help=argparse.SUPPRESS)
    parser.add_argument(
        "--no-push",
        action="store_false",
        dest="push",
        help="do not commit and push automatic fixes",
        default=argparse.SUPPRESS,
    )
    return parser.parse_args()


def commit_push_staged(pr_info: PRInfo) -> None:
    # It works ONLY for PRs, and only over ssh, so either
    # ROBOT_CLICKHOUSE_SSH_KEY should be set or ssh-agent should work
    assert pr_info.number
    if not pr_info.head_name == pr_info.base_name:
        # We can't push to forks, sorry folks
        return
    git_staged = git_runner("git diff --cached --name-only")
    if not git_staged:
        logging.info("No fixes are staged")
        return

    def push_fix() -> None:
        """
        Stash staged changes to commit them on the top of the PR's head.
        `pull_request` event runs on top of a temporary merge_commit, we need to avoid
        including it in the autofix
        """
        remote_url = pr_info.event["pull_request"]["base"]["repo"]["ssh_url"]
        head = pr_info.sha
        git_runner(f"{GIT_PREFIX} commit -m 'Automatic style fix'")
        fix_commit = git_runner("git rev-parse HEAD")
        logging.info(
            "Fetching PR's head, check it out and cherry-pick autofix: %s", head
        )
        git_runner(
            f"{GIT_PREFIX} fetch {remote_url} --no-recurse-submodules --depth=1 {head}"
        )
        git_runner(f"git reset --hard {head}")
        git_runner(f"{GIT_PREFIX} cherry-pick {fix_commit}")
        git_runner(f"{GIT_PREFIX} push {remote_url} HEAD:{pr_info.head_ref}")

    if os.getenv("ROBOT_CLICKHOUSE_SSH_KEY", ""):
        with SSHKey("ROBOT_CLICKHOUSE_SSH_KEY"):
            push_fix()
            return

    push_fix()


def _check_mime(file: Union[Path, str], mime: str) -> bool:
    # WARNING: python-magic v2:0.4.24-2 is used in ubuntu 22.04,
    # and `Support os.PathLike values in magic.from_file` is only from 0.4.25
    try:
        return bool(magic.from_file(os.path.join(REPO_COPY, file), mime=True) == mime)
    except (IsADirectoryError, FileNotFoundError) as e:
        # Process submodules and removed files w/o errors
        logging.warning("Captured error on file '%s': %s", file, e)
        return False


def is_python(file: Union[Path, str]) -> bool:
    """returns if the changed file in the repository is python script"""
    return (
        _check_mime(file, "text/x-script.python")
        or str(file).endswith(".py")
        or str(file) == "pyproject.toml"
    )


def is_shell(file: Union[Path, str]) -> bool:
    """returns if the changed file in the repository is shell script"""
    return _check_mime(file, "text/x-shellscript") or str(file).endswith(".sh")


def is_style_image(file: Union[Path, str]) -> bool:
    """returns if the changed file is related to an updated docker image for style check"""
    return str(file).startswith("docker/test/style/")


def main():
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("git_helper").setLevel(logging.DEBUG)
    args = parse_args()

    stopwatch = Stopwatch()

    repo_path = Path(REPO_COPY)
    temp_path = Path(TEMP_PATH)
    if temp_path.is_dir():
        shutil.rmtree(temp_path)
    temp_path.mkdir(parents=True, exist_ok=True)

    pr_info = PRInfo()

    if pr_info.is_merge_queue and args.push:
        print("Auto style fix will be disabled for Merge Queue workflow")
        args.push = False

    run_cpp_check = True
    run_shell_check = True
    run_python_check = True
    if IS_CI and pr_info.number > 0:
        pr_info.fetch_changed_files()
        run_cpp_check = any(
            is_style_image(file) or not (is_python(file) or is_shell(file))
            for file in pr_info.changed_files
        )
        run_shell_check = any(
            is_style_image(file) or is_shell(file) for file in pr_info.changed_files
        )
        run_python_check = any(
            is_style_image(file) or is_python(file) for file in pr_info.changed_files
        )

    IMAGE_NAME = "clickhouse/style-test"
    image = pull_image(get_docker_image(IMAGE_NAME))
    docker_command = (
        f"docker run -u $(id -u ${{USER}}):$(id -g ${{USER}}) --cap-add=SYS_PTRACE "
        f"--volume={repo_path}:/ClickHouse --volume={temp_path}:/test_output "
        f"--entrypoint= -w/ClickHouse/utils/check-style {image}"
    )
    cmd_docs = f"{docker_command} ./check_docs.sh"
    cmd_cpp = f"{docker_command} ./check_cpp.sh"
    cmd_py = f"{docker_command} ./check_py.sh"
    cmd_shell = f"{docker_command} ./check_shell.sh"

    with ProcessPoolExecutor(max_workers=2) as executor:
        logging.info("Run docs files check: %s", cmd_docs)
        future = executor.submit(subprocess.run, cmd_docs, shell=True)
        # Parallelization  does not make it faster - run subsequently
        _ = future.result()

        if run_cpp_check:
            logging.info("Run source files check: %s", cmd_cpp)
            future = executor.submit(subprocess.run, cmd_cpp, shell=True)
            _ = future.result()

        if run_python_check:
            logging.info("Run py files check: %s", cmd_py)
            future = executor.submit(subprocess.run, cmd_py, shell=True)
            _ = future.result()
        if run_shell_check:
            logging.info("Run shellcheck check: %s", cmd_shell)
            future = executor.submit(subprocess.run, cmd_shell, shell=True)
            _ = future.result()

    subprocess.check_call(
        f"python3 ../../utils/check-style/process_style_check_result.py --in-results-dir {temp_path} "
        f"--out-results-file {temp_path}/test_results.tsv --out-status-file {temp_path}/check_status.tsv || "
        f'echo -e "failure\tCannot parse results" > {temp_path}/check_status.tsv',
        shell=True,
    )

    state, description, test_results, additional_files = process_result(temp_path)

    autofix_description = ""
    push_fix = args.push
    for result in test_results:
        if result.status in (FAILURE, FAIL) and push_fix:
            # do not autofix if something besides isort and black is failed
            push_fix = any(result.name.endswith(check) for check in ("isort", "black"))

    if push_fix:
        try:
            commit_push_staged(pr_info)
        except subprocess.SubprocessError:
            # do not fail the whole script if the autofix didn't work out
            logging.error("Unable to push the autofix. Continue.")
            autofix_description = "Failed to push autofix to the PR. "

    JobReport(
        description=f"{autofix_description}{description}",
        test_results=test_results,
        status=state,
        start_time=stopwatch.start_time_str,
        duration=stopwatch.duration_seconds,
        # add GITHUB_EVENT_PATH json file to have it in style check report. sometimes it's needed for debugging.
        additional_files=additional_files + [Path(GITHUB_EVENT_PATH)],
    ).dump()

    if state in [ERROR, FAILURE]:
        print(f"Style check failed: [{description}]")
        sys.exit(1)


if __name__ == "__main__":
    main()
