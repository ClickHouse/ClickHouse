#!/usr/bin/env python3

import argparse
import ast
import csv
import json
import logging
import os
import re
import time
from pathlib import Path
from typing import Dict, List, Optional

from tests.integration.integration_test_images import IMAGES

import ci.jobs.scripts.integration_tests_runner as runner
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils


def get_json_params_dict(
    check_name: str,
    info: Info,
    docker_images,
    run_by_hash_total: int,
    run_by_hash_num: int,
    job_configuration: str,
    need_changed_files: bool,
) -> dict:
    return {
        "context_name": check_name,
        "commit": info.sha,
        "pull_request": info.pr_number,
        "changed_files": info.get_changed_files() if need_changed_files else [],
        "docker_images_with_versions": {d.name: d.version for d in docker_images},
        "shuffle_test_groups": False,
        "use_tmpfs": False,
        "disable_net_host": True,
        "run_by_hash_total": run_by_hash_total,
        "run_by_hash_num": run_by_hash_num,
        "pr_updated_at": info.event_time,
        "job_configuration": job_configuration,
    }


class DockerImage:
    def __init__(self, name: str, version: Optional[str] = None):
        self.name = name
        if version is None:
            self.version = "latest"
        else:
            self.version = version


def get_docker_image(image_name: str) -> DockerImage:
    DOCKER_TAG = os.getenv("DOCKER_TAG", None)
    assert DOCKER_TAG and isinstance(DOCKER_TAG, str), "DOCKER_TAG env must be provided"
    if "{" in DOCKER_TAG:
        tags_map = json.loads(DOCKER_TAG)
        assert (
            image_name in tags_map
        ), f"Image name [{image_name}] does not exist in provided DOCKER_TAG json string"
        arch_suffix = "_arm" if Utils.is_arm() else "_amd"
        return DockerImage(image_name, tags_map[image_name] + arch_suffix)
    assert False


def get_env_for_runner(
    check_name: str,
    binary_path: Path,
    repo_path: Path,
    result_path: Path,
    work_path: Path,
) -> Dict[str, str]:
    my_env = os.environ.copy()
    my_env["CLICKHOUSE_TESTS_SERVER_BIN_PATH"] = binary_path.as_posix()
    my_env["CLICKHOUSE_TESTS_CLIENT_BIN_PATH"] = binary_path.as_posix()
    my_env["CLICKHOUSE_TESTS_REPO_PATH"] = repo_path.as_posix()
    my_env["CLICKHOUSE_TESTS_RESULT_PATH"] = result_path.as_posix()
    my_env["CLICKHOUSE_TESTS_BASE_CONFIG_DIR"] = f"{repo_path}/programs/server"
    my_env["CLICKHOUSE_TESTS_JSON_PARAMS_PATH"] = f"{work_path}/params.json"
    my_env["CLICKHOUSE_TESTS_RUNNER_RESTART_DOCKER"] = "0"

    if "analyzer" in check_name.lower():
        my_env["CLICKHOUSE_USE_OLD_ANALYZER"] = "1"

    if "distributed plan" in check_name.lower():
        my_env["CLICKHOUSE_USE_DISTRIBUTED_PLAN"] = "1"

    return my_env


def read_test_results(results_path: Path, with_raw_logs: bool = True) -> List[Result]:
    results = []
    with open(results_path, "r", encoding="utf-8") as descriptor:
        reader = csv.reader(descriptor, delimiter="\t")
        for line in reader:
            name = line[0]
            status = line[1]
            time = None
            if len(line) >= 3 and line[2] and line[2] != "\\N":
                # The value can be emtpy, but when it's not,
                # it's the time spent on the test
                try:
                    time = float(line[2])
                except ValueError:
                    pass

            result = Result(name=name, status=status, duration=time)
            if len(line) == 4 and line[3]:
                # The value can be emtpy, but when it's not,
                # the 4th value is a pythonic list, e.g. ['file1', 'file2']
                if with_raw_logs:
                    # Python does not support TSV, so we unescape manually
                    result.info = line[3].replace("\\t", "\t").replace("\\n", "\n")
                else:
                    result.files = ast.literal_eval(line[3])

            results.append(result)
    return results


def process_results(
    result_directory: Path,
):
    test_results = []
    additional_files = []
    # Just upload all files from result_directory.
    # If task provides processed results, then it's responsible for content of
    # result_directory.
    if result_directory.exists():
        additional_files = [p for p in result_directory.iterdir() if p.is_file()]

    status = []
    status_path = result_directory / "check_status.tsv"
    if status_path.exists():
        logging.info("Found %s", status_path.name)
        with open(status_path, "r", encoding="utf-8") as status_file:
            status = list(csv.reader(status_file, delimiter="\t"))

    if len(status) != 1 or len(status[0]) != 2:
        logging.info("Files in result folder %s", os.listdir(result_directory))
        return (
            Result.Status.ERROR,
            "Invalid check_status.tsv",
            test_results,
            additional_files,
        )
    state, description = status[0][0], status[0][1]

    try:
        results_path = result_directory / "test_results.tsv"
        test_results = read_test_results(results_path, False)
        if len(test_results) == 0:
            return (
                Result.Status.ERROR,
                "Empty test_results.tsv",
                test_results,
                additional_files,
            )
    except Exception as e:
        return (
            Result.Status.ERROR,
            f"Cannot parse test_results.tsv ({e})",
            test_results,
            additional_files,
        )

    return state, description, test_results, additional_files  # type: ignore


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check-name", default="", required=False)
    parser.add_argument(
        "--run-tests", nargs="*", help="List of tests to run", default=None
    )
    parser.add_argument(
        "--validate-bugfix",
        action="store_true",
        help="Check that added tests failed on latest stable",
    )
    return parser.parse_args()


def main():
    logging.basicConfig(level=logging.INFO)

    stopwatch = Utils.Stopwatch()

    repo_path = Path(Utils.cwd())
    temp_path = Path(f"{repo_path}/ci/tmp")
    info = Info()

    args = parse_args()
    check_name = args.check_name or info.job_name
    assert (
        check_name
    ), "Check name must be provided in --check-name input option or in CHECK_NAME env"
    validate_bugfix_check = args.validate_bugfix

    match = re.search(r"\(.*?\)", check_name)
    options = match.group(0)[1:-1].split(",") if match else []
    run_by_hash_num, run_by_hash_total = 0, 1
    for option in options:
        if "/" in option:
            run_by_hash_num = int(option.split("/")[0]) - 1
            run_by_hash_total = int(option.split("/")[1])
            print(f"batch {run_by_hash_num}/{run_by_hash_total}")
            break

    job_configuration = options[0].strip()

    is_flaky_check = "flaky" in check_name

    images = [get_docker_image(image_) for image_ in IMAGES]

    result_path = temp_path / "output_dir"
    result_path.mkdir(parents=True, exist_ok=True)

    work_path = temp_path / "workdir"
    work_path.mkdir(parents=True, exist_ok=True)

    build_path = temp_path
    build_path.mkdir(parents=True, exist_ok=True)

    if validate_bugfix_check:
        if Utils.is_arm():
            link_to_master_head_binary = "https://clickhouse-builds.s3.us-east-1.amazonaws.com/master/aarch64/clickhouse"
        else:
            link_to_master_head_binary = "https://clickhouse-builds.s3.us-east-1.amazonaws.com/master/amd64/clickhouse"
        if not info.is_local_run or not (temp_path / "clickhouse").exists():
            print(
                f"NOTE: ClickHouse binary will be downloaded to [{temp_path}] from [{link_to_master_head_binary}]"
            )
            if info.is_local_run:
                time.sleep(10)
            Shell.check(
                f"wget -nv -P {temp_path} {link_to_master_head_binary}",
                verbose=True,
                strict=True,
            )

    binary_path = build_path / "clickhouse"

    # Set executable bit and run it for self extraction
    os.chmod(binary_path, 0o755)
    Shell.check(f"{binary_path} local 'SELECT version()'", verbose=True, strict=True)

    my_env = get_env_for_runner(
        check_name, binary_path, repo_path, result_path, work_path
    )

    json_path = work_path / "params.json"
    with open(json_path, "w", encoding="utf-8") as json_params:
        params_text = json.dumps(
            get_json_params_dict(
                check_name,
                info,
                images,
                run_by_hash_total,
                run_by_hash_num,
                job_configuration,
                is_flaky_check or validate_bugfix_check,
            )
        )
        json_params.write(params_text)
        logging.info("Parameters file %s is written: %s", json_path, params_text)

    for k, v in my_env.items():
        os.environ[k] = v
    logging.info(
        "ENV parameters for runner:\n%s",
        "\n".join(
            [f"{k}={v}" for k, v in my_env.items() if k.startswith("CLICKHOUSE_")]
        ),
    )

    try:
        runner.run()
    except Exception as e:
        logging.error("Exception: %s", e)
        state, description, test_results, additional_logs = (
            Result.Status.ERROR,
            "infrastructure error",
            [],
            [],
        )
    else:
        state, description, test_results, additional_logs = process_results(result_path)

    res = Result(
        name=info.job_name,
        info=description,
        results=test_results,
        status=state,
        start_time=stopwatch.start_time,
        duration=stopwatch.duration,
        files=additional_logs,
    )

    if validate_bugfix_check:
        has_failure = False
        for r in res.results:
            if r.status == Result.StatusExtended.FAIL:
                has_failure = True
                break
        if not has_failure:
            print("Failed to reproduce the bug")
            res.set_failed()
            res.set_info("Failed to reproduce the bug")
        else:
            res.set_success()

    res.complete_job()


if __name__ == "__main__":
    main()
