#!/usr/bin/env python3

# Strategy of fuzzing:
# we want to minimize corpora with preserving coverage, and at the same time
# we want to include whatever additional inputs we can get from either dynamically
# generated inputs or from manual uploads. To this end we are going to firstly run
# fuzzers with -merge=1 flag with first corpus directory empty - which will collect
# only uniquely covering inputs - and with following other corpus directories
# including previously collected and downloaded from S3. This run will produce
# minimized corpus with unique coverage on which we are going to run fuzzing next.
# Also this run may produce some (multiple) failures which we are going to report as regressions.
# After that we are going to run fuzzers normally with produced minimized corpus and
# with enabled coverage collection. After this run we are going to upload fresh corpus
# to S3 for future runs. All discovered failures will be reported along with coverage stats.

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

TIMEOUT = 60 * 60 # 60 minutes
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
    if "distributed plan" in check_name:
        result.append("USE_DISTRIBUTED_PLAN=1")

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


def download_corpus(path):
    logging.info("Download corpus...")

    corpus_path = path / "corpus"
    corpus_path.mkdir(exist_ok=True)

    try:
        s3.download_files(
            bucket=S3_BUILDS_BUCKET,
            s3_path="fuzzer/corpus/",
            file_suffix=".zip",
            local_directory=corpus_path,
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            logging.debug("No active corpus exists")
            return
        else:
            raise

    subprocess.check_call(f"ls -al {corpus_path}", shell=True)
    logging.info("...downloaded %d corpora", len(corpus_path.glob("*.zip")))

    total_units = 0

    for zip_file in corpus_path.glob("*.zip"):
        target_dir = corpus_path / zip_file.stem
        target_dir.mkdir(exist_ok=True)
        with zipfile.ZipFile(zip_file, "r") as zf:
            zf.extractall(target_dir)
        zip_file.unlink()
        units = len(target_dir.glob("*"))
        total_units += units
        logging.info("%s corpus having %d units...", zip_file.stem, units)

    subprocess.check_call(f"ls -al {corpus_path}", shell=True)

    logging.info("...downloaded total %d units", total_units)


def upload_corpus(path):
    with zipfile.ZipFile(f"{path}/corpus.zip", "w", zipfile.ZIP_DEFLATED) as zipf:
        zipdir(f"{path}/corpus/", zipf)
    s3.upload_file(
        bucket=S3_BUILDS_BUCKET,
        file_path=f"{path}/corpus.zip",
        s3_path="fuzzer/corpus.zip",
    )


def process_error(path: Path) -> list:
    ERROR = r"^==\d+==\s?ERROR: (\S+): (.*)"
    ERROR_END = r"^SUMMARY: .*"
    error_source = ""
    error_reason = ""
    test_unit = ""
    stack_trace = []
    TEST_UNIT_LINE = r"artifact_prefix='.*\/'; Test unit written to (.*)"
    error_info = [] # [(error_source, error_reason, test_unit, trace_file), ...]
    is_error = False

    with open(path, "r", encoding="utf-8", errors='replace') as file:
        for line in file:
            line = line.rstrip("\n")
            if is_error:
                match = re.search(ERROR_END, line)
                if match:
                    is_error = False
                    continue
                stack_trace.append(line)                
                continue

            match = re.search(ERROR, line)
            if match:
                stack_trace.append(line)
                error_source = match.group(1)
                error_reason = match.group(2)
                is_error = True
                continue

            match = re.search(TEST_UNIT_LINE, line)
            if match:
                test_unit_path = match.group(1)
                test_unit = os.path.basename(test_unit_path)
                trace_file = f"{test_unit}.trace"
                trace_path = f"{os.path.dirname(test_unit_path)}/{trace_file}"
                with open(trace_path, "w", encoding="utf-8") as tracef:
                    tracef.write("\n".join(stack_trace))
                error_info.append((error_source, error_reason, test_unit, trace_file))
                # reset for next error
                error_source = ""
                error_reason = ""
                test_unit = ""
                stack_trace = []

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
    for fuzzer_result_dir in result_path.glob("*.results"):
        fuzzer = fuzzer_result_dir.stem
        file_path_status = fuzzer_result_dir / "status.txt"
        file_path_out = fuzzer_result_dir / "out.txt"
        file_path_stdout = fuzzer_result_dir / "stdout.txt"

        status = read_status(file_path_status)
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
                errors = process_error(file_path_out)
                result.set_raw_logs("\n".join("\t".join(e) for e in errors))
            # Collect all crash files and trace files
            crash_files = list(fuzzer_result_dir.glob("crash-*"))
            trace_files = list(fuzzer_result_dir.glob("*.trace"))
            all_files = crash_files + trace_files
            if all_files:
                result.set_log_files("[" + ", ".join(f"'{f}'" for f in all_files) + "]")

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

    docker_image = pull_image(get_docker_image("clickhouse/stateless-test"))

    fuzzers_path = temp_path / "fuzzers"
    fuzzers_path.mkdir(parents=True, exist_ok=True)

    download_corpus(fuzzers_path)

    # TESTING TESTING TESTING
    exit(0)

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
            if (
                pr_info.number == 0
                and pr_info.base_ref == "master"
                and pr_info.head_ref == "master"
            ):
                logging.info("Uploading corpus - running in master")
                upload_corpus(fuzzers_path)
            else:
                logging.info("Not uploading corpus - running in PR")
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
