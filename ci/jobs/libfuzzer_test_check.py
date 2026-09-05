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
import json
import logging
import os
import re
import subprocess
import zipfile
from pathlib import Path
from typing import List

from ci.jobs.scripts.docker_image import DockerImage
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.s3 import S3
from ci.praktika.settings import Settings
from ci.praktika.utils import Shell, Utils

TIMEOUT_MASTER = 5 * 60 * 60  # 5 hours of fuzzing for nightly/master runs
TIMEOUT_PR = 30 * 60  # 30 minutes for PR runs
# Corpus minimization is a fixed amount of work proportional to the corpus (the
# slowest target takes about 40 minutes), so its cap does not move with the
# fuzzing budget.
TIMEOUT_MINIMIZATION = 60 * 60
NO_CHANGES_MSG = "Nothing to run"
RUNNER_OUTPUT = "/test_output"


def zipdir(path, ziph):
    # ziph is zipfile handle
    for root, _, files in os.walk(path):
        for file in files:
            ziph.write(
                os.path.join(root, file),
                file,
            )


def get_run_command(
    fuzzers_path: Path,
    repo_path: Path,
    result_path: Path,
    additional_envs: List[str],
    image: DockerImage,
) -> str:
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
        f"--user {uid}:{gid} "
        f"--workdir=/fuzzers "
        f"--volume={fuzzers_path}:/fuzzers "
        f"--volume={repo_path}/tests:/usr/share/clickhouse-test "
        f"--volume={result_path}:{RUNNER_OUTPUT} "
        "--security-opt seccomp=unconfined "  # required to issue io_uring sys-calls
        f"--cap-add=SYS_PTRACE {env_str} {image} "
        "python3 /usr/share/clickhouse-test/fuzz/runner.py"
    )


def count_fuzzers(path: Path) -> int:
    return max(1, len([f for f in os.listdir(path) if f.endswith("_fuzzer")]))


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("check_name")
    parser.add_argument(
        "--minimize-only",
        action="store_true",
        help="Only minimize the corpora and upload the result, do not fuzz.",
    )
    return parser.parse_args()


def download_corpus(path):
    logging.info("Download corpus...")

    corpus_path = path / "corpus"
    corpus_path.mkdir(parents=True, exist_ok=True)

    try:
        S3.copy_file_from_s3(
            s3_path=f"{Settings.S3_ARTIFACT_BUCKET}/fuzzer/corpus",
            local_path=str(corpus_path),
            include_pattern="*.zip",
            recursive=True,
        )
    except Exception as e:
        error_message = str(e).lower()
        if "does not exist" in error_message or "not found" in error_message:
            logging.info("Corpus does not exist in S3, starting with empty corpus")
        else:
            raise

    subprocess.check_call(f"ls -al {corpus_path}", shell=True)
    logging.info("...downloaded %d corpora", len(list(corpus_path.glob("*.zip"))))

    total_units = 0
    orphans = []

    for zip_file in corpus_path.glob("*.zip"):
        if not (path / zip_file.stem).exists():
            # A corpus left behind by a fuzzer that no longer exists. Deploying it
            # would be wasted work, and it would then be uploaded again, keeping
            # the dead corpus alive forever.
            orphans.append(zip_file.stem)
            zip_file.unlink()
            continue

        logging.info("Deploying corpus %s", zip_file.stem)
        target_dir = corpus_path / zip_file.stem
        target_dir.mkdir(exist_ok=True)
        try:
            with zipfile.ZipFile(zip_file, "r") as zf:
                zf.extractall(target_dir)
        except Exception:
            logging.info("Failed to unzip %s", zip_file)
            raise
        zip_file.unlink()
        units = len(list(target_dir.glob("*")))
        total_units += units
        logging.info("%s corpus having %d units...", zip_file.stem, units)

    subprocess.check_call(f"ls -al {corpus_path}", shell=True)

    logging.info("...downloaded total %d units", total_units)

    if orphans:
        logging.warning(
            "Skipped corpora with no matching fuzzer binary; delete them from "
            "s3://%s/fuzzer/corpus/: %s",
            Settings.S3_ARTIFACT_BUCKET,
            ", ".join(f"{name}.zip" for name in sorted(orphans)),
        )


def upload_corpus(path):
    corpus_dir = Path(path) / "corpus"
    for fuzzer_dir in corpus_dir.iterdir():
        if fuzzer_dir.is_dir() and fuzzer_dir.name.endswith("_fuzzer"):
            zip_file_path = corpus_dir / f"{fuzzer_dir.name}.zip"
            with zipfile.ZipFile(zip_file_path, "w", zipfile.ZIP_DEFLATED) as zipf:
                zipdir(fuzzer_dir, zipf)
            S3.copy_file_to_s3(
                s3_path=f"{Settings.S3_ARTIFACT_BUCKET}/fuzzer/corpus/{fuzzer_dir.name}.zip",
                local_path=str(zip_file_path),
            )


# same as upload_corpus but without uploading - for testing purposes
def zip_corpus(path):
    corpus_dir = Path(path) / "corpus"
    for fuzzer_dir in corpus_dir.iterdir():
        if fuzzer_dir.is_dir() and fuzzer_dir.name.endswith("_fuzzer"):
            zip_file_path = corpus_dir / f"{fuzzer_dir.name}.zip"
            with zipfile.ZipFile(zip_file_path, "w", zipfile.ZIP_DEFLATED) as zipf:
                zipdir(fuzzer_dir, zipf)


def process_error(output_log: Path, fuzzer_result_dir: Path) -> list:
    ERROR = r"^==\d+==\s?ERROR: (\S+): (.*)"
    ERROR_END = r"^SUMMARY: .*"
    error_source = ""
    error_reason = ""
    test_unit = ""
    trace_file = ""
    stack_trace = []
    TEST_UNIT_LINE = (
        r"^artifact_prefix='.*\/'; Test unit written to ((?:(?!slow-unit-).)+)$"
    )
    error_info = []  # [(error_source, error_reason, test_unit, trace_file), ...]
    is_error = False

    with open(output_log, "r", encoding="utf-8", errors="replace") as file:
        for line in file:
            line = line.rstrip("\n")

            match = re.search(TEST_UNIT_LINE, line)
            if match:
                test_unit = os.path.basename(match.group(1))
                trace_file = f"{test_unit}.trace"
                trace_path = f"{fuzzer_result_dir}/{trace_file}"

                if not is_error and len(stack_trace) > 0:
                    with open(trace_path, "w", encoding="utf-8") as tracef:
                        tracef.write("\n".join(stack_trace))
                    error_info.append(
                        (error_source, error_reason, test_unit, trace_file)
                    )
                    # reset for next error
                    error_source = ""
                    error_reason = ""
                    test_unit = ""
                    trace_file = ""
                    stack_trace = []
                continue

            if is_error:
                match = re.search(ERROR_END, line)
                if match:
                    is_error = False
                    if test_unit:
                        trace_path = f"{fuzzer_result_dir}/{trace_file}"
                        with open(trace_path, "w", encoding="utf-8") as tracef:
                            tracef.write("\n".join(stack_trace))
                        error_info.append(
                            (error_source, error_reason, test_unit, trace_file)
                        )
                        # reset for next error
                        error_source = ""
                        error_reason = ""
                        test_unit = ""
                        trace_file = ""
                        stack_trace = []
                    continue
                stack_trace.append(line)
                continue

            match = re.search(ERROR, line)
            if match:
                stack_trace.append(line)
                error_source = match.group(1)
                error_reason = match.group(2)
                is_error = True

    return error_info


def read_status(status_path: Path):
    result = []
    with open(status_path, "r", encoding="utf-8") as file:
        for line in file:
            result.append(line.rstrip("\n"))
    return result


def read_stats(stats_path: Path) -> dict:
    if not stats_path.exists():
        return {}
    with open(stats_path, "r", encoding="utf-8") as file:
        return json.load(file)


def format_fuzzing_stats(stats: dict) -> list:
    """Turn the numbers `runner.py` scraped out of the libFuzzer output into report lines.

    Without these the report only says OK or FAIL, so a target that spends its
    whole budget replaying its corpus, or that stopped executing inputs at all,
    is indistinguishable from a healthy one.
    """
    lines = []

    last = stats.get("last", {})
    if last:
        summary = [f"executed {last.get('executed', 0)} inputs"]
        for key in ("exec/s", "cov", "ft", "corp", "rss"):
            if key in last:
                summary.append(f"{key}: {last[key]}")
        lines.append(", ".join(summary))

    input_files = stats.get("input_files", {})
    if input_files:
        lines.append(
            "input directories: "
            + ", ".join(f"{name} ({count} files)" for name, count in input_files.items())
        )

    inited = stats.get("inited", {})
    total = last.get("executed", 0)
    if inited and total:
        replayed = inited.get("executed", 0)
        lines.append(
            f"replaying the initial corpus took {replayed} of {total} executed inputs "
            f"({replayed * 100 // total}%), leaving {total - replayed} mutations"
        )

    before = stats.get("corpus_size_before")
    after = stats.get("corpus_size_after")
    if before is not None and after is not None:
        lines.append(
            f"{stats.get('new_units', 0)} new corpus units ({before} -> {after})"
        )

    return lines


def format_minimization_stats(stats: dict) -> list:
    if not stats:
        return []
    return [
        "corpus {} -> {} units ({}%), processed {}, not processed {}".format(
            stats.get("original_corpus_size"),
            stats.get("minimized_corpus_size"),
            stats.get("reduction_percent"),
            stats.get("processed"),
            stats.get("not_processed"),
        )
    ]


def process_results(result_path: Path):
    test_results = []
    oks = 0
    errors = 0
    fails = 0
    for fuzzer_result_dir in result_path.glob("*.results"):
        fuzzer = fuzzer_result_dir.stem

        # Process corpus minimization results
        file_path_status_mini = fuzzer_result_dir / "status_mini.txt"

        if file_path_status_mini.exists():
            file_path_out_mini = fuzzer_result_dir / "out_mini.txt"
            file_path_stdout_mini = fuzzer_result_dir / "stdout_mini.txt"

            raw_logs = []
            log_files = []

            status_mini = read_status(file_path_status_mini)
            result = Result(
                f"{fuzzer} corpus minimization",
                status_mini[0],
                duration=float(status_mini[2]),
            )

            raw_logs += format_minimization_stats(
                read_stats(fuzzer_result_dir / "stats_mini.txt")
            )

            if file_path_out_mini.exists():
                log_files.append(str(file_path_out_mini))
            if file_path_stdout_mini.exists():
                log_files.append(str(file_path_stdout_mini))

            if status_mini[0] == "ERROR":
                errors += 1
                raw_logs.append("Corpus minimization FAILED.")
            else:
                oks += 1

            if file_path_out_mini.exists():
                err = process_error(file_path_out_mini, fuzzer_result_dir)
                if len(err):
                    raw_logs.append(
                        "Possible regressions:"
                        if status_mini[0] == "ERROR"
                        else "Regressions:"
                    )
                    for line in err:
                        raw_logs.append("\t".join(s for s in line))

            # Collect all crash, timeout and trace files
            for file in list(fuzzer_result_dir.glob("mini-crash-*")):
                log_files.append(str(file))
            for file in list(fuzzer_result_dir.glob("mini-timeout-*")):
                log_files.append(str(file))
            for file in list(fuzzer_result_dir.glob("mini-slow-unit-*")):
                log_files.append(str(file))

            result.set_info("\n".join(raw_logs))
            result.set_files(log_files)
            test_results.append(result)

        # Process fuzzing results
        raw_logs = []
        log_files = []

        file_path_status = fuzzer_result_dir / "status.txt"
        file_path_out = fuzzer_result_dir / "out.txt"
        file_path_stdout = fuzzer_result_dir / "stdout.txt"

        if not file_path_status.exists():
            # A corpus minimization run: there is no fuzzing result to report.
            continue

        status = read_status(file_path_status)
        result = Result(fuzzer, status[0], duration=float(status[2]))

        raw_logs += format_fuzzing_stats(read_stats(fuzzer_result_dir / "stats.txt"))

        if file_path_out.exists():
            log_files.append(str(file_path_out))
        if file_path_stdout.exists():
            log_files.append(str(file_path_stdout))

        if status[0] == "OK":
            oks += 1
        elif status[0] == "ERROR":
            errors += 1
            raw_logs.append("Fuzzing FAILED.")
        else:
            fails += 1
            if file_path_out.exists():
                err = process_error(file_path_out, fuzzer_result_dir)
                if len(err):
                    raw_logs.append("New findings:")
                    for line in err:
                        raw_logs.append("\t".join(s for s in line))
                else:
                    raw_logs.append(
                        "No stack traces found - this is unusual - check output files"
                    )

        # Collect all crash-, timeout-, slow-unit-, oom- and .trace files
        for file in list(fuzzer_result_dir.glob("crash-*")):
            log_files.append(str(file))
        for file in list(fuzzer_result_dir.glob("timeout-*")):
            log_files.append(str(file))
        for file in list(fuzzer_result_dir.glob("slow-unit-*")):
            log_files.append(str(file))
        for file in list(fuzzer_result_dir.glob("oom-*")):
            log_files.append(str(file))

        result.set_info("\n".join(raw_logs))
        result.set_files(log_files)
        test_results.append(result)

    return [oks, errors, fails, test_results]


def main():
    logging.basicConfig(level=logging.INFO)

    stopwatch = Utils.Stopwatch()

    temp_path = Path(Utils.cwd()) / "ci/tmp"
    temp_path.mkdir(parents=True, exist_ok=True)
    repo_path = Path(Utils.cwd())

    # The check name is accepted for consistency with the other job scripts,
    # which all take it as a positional argument; only the flags are used.
    args = parse_args()
    info = Info()

    temp_path.mkdir(parents=True, exist_ok=True)

    docker_image = DockerImage.get_docker_image(
        "clickhouse/stateless-test"
    ).pull_image()

    is_master = info.pr_number == 0 and info.git_branch == "master"

    fuzzers_path = temp_path
    download_corpus(fuzzers_path)

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

    additional_envs = []

    if args.minimize_only:
        timeout = TIMEOUT_MINIMIZATION
    else:
        timeout = TIMEOUT_MASTER if is_master else TIMEOUT_PR
    additional_envs.append(f"TIMEOUT={timeout}")

    # Every fuzzer holds its runner thread for the whole run, so anything above
    # this many targets would not start until another one finishes - at a 5 hour
    # budget that doubles the length of the job instead of queueing for minutes.
    additional_envs.append(f"RUNNERS={count_fuzzers(fuzzers_path)}")

    if args.minimize_only:
        additional_envs.append("MINIMIZE_ONLY=1")
    else:
        # Corpus minimization is a separate scheduled job, so that a fuzzing run
        # always gets its whole budget for fuzzing.
        additional_envs.append("SKIP_MERGE=1")

    run_command = get_run_command(
        fuzzers_path,
        repo_path,
        result_path,
        additional_envs,
        docker_image,
    )
    logging.info("Going to run libFuzzer tests: %s", run_command)

    if Shell.run(run_command) == 0:
        logging.info("Run successfully")
        if is_master:
            logging.info("Uploading corpus - running in master")
            upload_corpus(fuzzers_path)
        else:
            logging.info("Not uploading corpus - running in PR")
            zip_corpus(fuzzers_path)
            subprocess.check_call(f"ls -al {fuzzers_path}/corpus/", shell=True)
    else:
        logging.info("Run failed")

    results = process_results(result_path)

    Result.create_from(
        results=results[3],
        stopwatch=stopwatch,
        info=f"OK: {results[0]}, ERROR: {results[1]}, FAIL: {results[2]}",
    ).complete_job()


if __name__ == "__main__":
    main()
