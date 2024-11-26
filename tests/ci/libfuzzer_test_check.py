#!/usr/bin/env python3

import argparse
import logging
import os
import re
import sys
import zipfile
from pathlib import Path
from typing import List

from botocore.exceptions import ClientError

from build_download_helper import download_fuzzers
from clickhouse_helper import CiLogsCredentials
from docker_images_helper import DockerImage, get_docker_image, pull_image
from env_helper import REPO_COPY, REPORT_PATH, S3_BUILDS_BUCKET, TEMP_PATH
from pr_info import PRInfo
from report import FAILURE, SUCCESS, JobReport, TestResult
from s3_helper import S3Helper
from stopwatch import Stopwatch
from tee_popen import TeePopen

TIMEOUT = 60
NO_CHANGES_MSG = "Nothing to run"
s3 = S3Helper()


def zipdir(path, ziph):
    # ziph is zipfile handle
    for root, _, files in os.walk(path):
        for file in files:
            ziph.write(
                os.path.join(root, file),
                os.path.relpath(os.path.join(root, file), os.path.join(path, "..")),
            )


def get_additional_envs(check_name, run_by_hash_num, run_by_hash_total):
    result = []
    if "DatabaseReplicated" in check_name:
        result.append("USE_DATABASE_REPLICATED=1")
    if "DatabaseOrdinary" in check_name:
        result.append("USE_DATABASE_ORDINARY=1")
    if "wide parts enabled" in check_name:
        result.append("USE_POLYMORPHIC_PARTS=1")
    if "ParallelReplicas" in check_name:
        result.append("USE_PARALLEL_REPLICAS=1")
    if "s3 storage" in check_name:
        result.append("USE_S3_STORAGE_FOR_MERGE_TREE=1")
        result.append("RANDOMIZE_OBJECT_KEY_TYPE=1")
    if "analyzer" in check_name:
        result.append("USE_OLD_ANALYZER=1")

    if run_by_hash_total != 0:
        result.append(f"RUN_BY_HASH_NUM={run_by_hash_num}")
        result.append(f"RUN_BY_HASH_TOTAL={run_by_hash_total}")

    return result


def get_run_command(
    fuzzers_path: Path,
    repo_path: Path,
    result_path: Path,
    additional_envs: List[str],
    ci_logs_args: str,
    image: DockerImage,
) -> str:
    additional_options = ["--hung-check"]
    additional_options.append("--print-time")

    additional_options_str = (
        '-e ADDITIONAL_OPTIONS="' + " ".join(additional_options) + '"'
    )

    envs = [
        # a static link, don't use S3_URL or S3_DOWNLOAD
        '-e S3_URL="https://s3.amazonaws.com"',
    ]

    envs += [f"-e {e}" for e in additional_envs]

    env_str = " ".join(envs)
    uid = os.getuid()
    gid = os.getgid()

    return (
        f"docker run "
        f"{ci_logs_args} "
        f"--user {uid}:{gid} "
        f"--workdir=/fuzzers "
        f"--volume={fuzzers_path}:/fuzzers "
        f"--volume={repo_path}/tests:/usr/share/clickhouse-test "
        f"--volume={result_path}:/test_output "
        "--security-opt seccomp=unconfined "  # required to issue io_uring sys-calls
        f"--cap-add=SYS_PTRACE {env_str} {additional_options_str} {image} "
        "python3 /usr/share/clickhouse-test/fuzz/runner.py"
    )


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("check_name")
    return parser.parse_args()


def download_corpus(path: str):
    logging.info("Download corpus...")

    try:
        s3.download_file(
            bucket=S3_BUILDS_BUCKET,
            s3_path="fuzzer/corpus.zip",
            local_file_path=path,
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            logging.debug("No active corpus exists")
        else:
            raise

    with zipfile.ZipFile(f"{path}/corpus.zip", "r") as zipf:
        zipf.extractall(path)
    os.remove(f"{path}/corpus.zip")

    units = 0
    for _, _, files in os.walk(path):
        units += len(files)

    logging.info("...downloaded %d units", units)


def upload_corpus(path: str):
    with zipfile.ZipFile(f"{path}/corpus.zip", "w", zipfile.ZIP_DEFLATED) as zipf:
        zipdir(f"{path}/corpus/", zipf)
    s3.upload_file(
        bucket=S3_BUILDS_BUCKET,
        file_path=f"{path}/corpus.zip",
        s3_path="fuzzer/corpus.zip",
    )


def process_error(path: Path) -> list:
    ERROR = r"^==\d+==\s?ERROR: (\S+): (.*)"
    # error_source = ""
    # error_reason = ""
    # test_unit = ""
    # TEST_UNIT_LINE = r"artifact_prefix='.*\/'; Test unit written to (.*)"
    error_info = []
    is_error = False

    with open(path, "r", encoding="utf-8") as file:
        for line in file:
            line = line.rstrip("\n")
            if is_error:
                error_info.append(line)
                # match = re.search(TEST_UNIT_LINE, line)
                # if match:
                #     test_unit = match.group(1)
                continue

            match = re.search(ERROR, line)
            if match:
                error_info.append(line)
                # error_source = match.group(1)
                # error_reason = match.group(2)
                is_error = True

    return error_info


def read_status(status_path: Path):
    result = []
    with open(status_path, "r", encoding="utf-8") as file:
        for line in file:
            result.append(line.rstrip("\n"))
    return result


def process_results(result_path: Path):
    test_results = []
    oks = 0
    errors = 0
    fails = 0
    for file in result_path.glob("*.status"):
        fuzzer = file.stem
        file_path = file.parent / fuzzer
        file_path_unit = file_path.with_suffix(".unit")
        file_path_out = file_path.with_suffix(".out")
        file_path_stdout = file_path.with_suffix(".stdout")
        status = read_status(file)
        result = TestResult(fuzzer, status[0], float(status[2]))
        if status[0] == "OK":
            oks += 1
        elif status[0] == "ERROR":
            errors += 1
            if file_path_out.exists():
                result.set_log_files(f"['{file_path_out}']")
            elif file_path_stdout.exists():
                result.set_log_files(f"['{file_path_stdout}']")
        else:
            fails += 1
            if file_path_out.exists():
                result.set_raw_logs("\n".join(process_error(file_path_out)))
            if file_path_unit.exists():
                result.set_log_files(f"['{file_path_unit}']")
            elif file_path_out.exists():
                result.set_log_files(f"['{file_path_out}']")
            elif file_path_stdout.exists():
                result.set_log_files(f"['{file_path_stdout}']")
        test_results.append(result)

    return [oks, errors, fails, test_results]


def main():
    logging.basicConfig(level=logging.INFO)

    stopwatch = Stopwatch()

    temp_path = Path(TEMP_PATH)
    reports_path = Path(REPORT_PATH)
    temp_path.mkdir(parents=True, exist_ok=True)
    repo_path = Path(REPO_COPY)

    args = parse_args()
    check_name = args.check_name

    pr_info = PRInfo()

    temp_path.mkdir(parents=True, exist_ok=True)

    if "RUN_BY_HASH_NUM" in os.environ:
        run_by_hash_num = int(os.getenv("RUN_BY_HASH_NUM", "0"))
        run_by_hash_total = int(os.getenv("RUN_BY_HASH_TOTAL", "0"))
    else:
        run_by_hash_num = 0
        run_by_hash_total = 0

    docker_image = pull_image(get_docker_image("clickhouse/libfuzzer"))

    fuzzers_path = temp_path / "fuzzers"
    fuzzers_path.mkdir(parents=True, exist_ok=True)

    download_corpus(fuzzers_path)
    download_fuzzers(check_name, reports_path, fuzzers_path)

    for file in os.listdir(fuzzers_path):
        if file.endswith("_fuzzer"):
            os.chmod(fuzzers_path / file, 0o777)
        elif file.endswith("_seed_corpus.zip"):
            seed_corpus_path = fuzzers_path / (
                file.removesuffix("_seed_corpus.zip") + ".in"
            )
            with zipfile.ZipFile(fuzzers_path / file, "r") as zfd:
                zfd.extractall(seed_corpus_path)

    result_path = temp_path / "result_path"
    result_path.mkdir(parents=True, exist_ok=True)

    run_log_path = result_path / "run.log"

    additional_envs = get_additional_envs(
        check_name, run_by_hash_num, run_by_hash_total
    )

    additional_envs.append(f"TIMEOUT={TIMEOUT}")

    ci_logs_credentials = CiLogsCredentials(Path(temp_path) / "export-logs-config.sh")
    ci_logs_args = ci_logs_credentials.get_docker_arguments(
        pr_info, stopwatch.start_time_str, check_name
    )

    run_command = get_run_command(
        fuzzers_path,
        repo_path,
        result_path,
        additional_envs,
        ci_logs_args,
        docker_image,
    )
    logging.info("Going to run libFuzzer tests: %s", run_command)

    with TeePopen(run_command, run_log_path) as process:
        retcode = process.wait()
        if retcode == 0:
            logging.info("Run successfully")
            upload_corpus(fuzzers_path)
        else:
            logging.info("Run failed")

    results = process_results(result_path)

    success = results[1] == 0 and results[2] == 0

    JobReport(
        description=f"OK: {results[0]}, ERROR: {results[1]}, FAIL: {results[2]}",
        test_results=results[3],
        status=SUCCESS if success else FAILURE,
        start_time=stopwatch.start_time_str,
        duration=stopwatch.duration_seconds,
        additional_files=[],
    ).dump()

    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()
