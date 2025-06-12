import argparse
import math
import multiprocessing
import os
import re
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

from praktika.result import Result
from praktika.utils import Shell, Utils

NPROC = multiprocessing.cpu_count()


def chunk_list(data, n):
    """Split the data list into n nearly equal-sized chunks."""
    chunk_size = math.ceil(len(data) / n)
    for i in range(0, len(data), chunk_size):
        yield data[i : i + chunk_size]


def run_check_concurrent(check_name, check_function, files, nproc=NPROC):
    stop_watch = Utils.Stopwatch()

    if not files:
        print(f"File list is empty [{files}]")
        raise

    file_chunks = list(chunk_list(files, nproc))
    results = []

    # Run check_function concurrently on each chunk
    with ProcessPoolExecutor(max_workers=NPROC) as executor:
        futures = [executor.submit(check_function, chunk) for chunk in file_chunks]
        # Wait for results and process them (optional)
        for future in futures:
            try:
                res = future.result()
                if res and res not in results:
                    results.append(res)
            except Exception as e:
                results.append(f"Exception in {check_name}: {e}")

    result = Result(
        name=check_name,
        status=Result.Status.SUCCESS if not results else Result.Status.FAILED,
        start_time=stop_watch.start_time,
        duration=stop_watch.duration,
        info=f"errors: {results}" if results else "",
    )
    return result


def check_duplicate_includes(file_path):
    includes = []
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            if re.match(r"^#include ", line):
                includes.append(line.strip())

    include_counts = {line: includes.count(line) for line in includes}
    duplicates = {line: count for line, count in include_counts.items() if count > 1}

    if duplicates:
        return f"{file_path}: {duplicates}"
    return ""


def check_whitespaces(file_paths):
    for file in file_paths:
        exit_code, out, err = Shell.get_res_stdout_stderr(
            f'./ci/jobs/scripts/check_style/double_whitespaces.pl "{file}"',
            verbose=False,
        )
        if out or err:
            return out + " err: " + err
    return ""


def check_yamllint(file_paths):
    file_paths = " ".join([f"'{file}'" for file in file_paths])
    exit_code, out, err = Shell.get_res_stdout_stderr(
        f"yamllint --config-file=./.yamllint {file_paths}", verbose=False
    )
    return out or err


def check_xmllint(file_paths):
    if not isinstance(file_paths, list):
        file_paths = [file_paths]
    file_paths = " ".join([f"'{file}'" for file in file_paths])
    exit_code, out, err = Shell.get_res_stdout_stderr(
        f"xmllint --noout --nonet {file_paths}", verbose=False
    )
    return out or err


def check_functional_test_cases(files):
    """
    Queries with event_date should have yesterday() not today()
    NOTE: it is not that accurate, but at least something.
    """

    patterns = [
        re.compile(
            r"(?i)where.*?\bevent_date\s*(=|>=)\s*today\(\)(?!\s*-\s*1)",
            re.IGNORECASE | re.DOTALL,
        )
    ]

    errors = []
    for test_case in files:
        try:
            with open(test_case, "r", encoding="utf-8", errors="replace") as f:
                file_content = " ".join(
                    f.read().splitlines()
                )  # Combine lines into a single string

            # Check if any pattern matches in the concatenated string
            if any(pattern.search(file_content) for pattern in patterns):
                errors.append(
                    f"event_date should be filtered using >=yesterday() in {test_case} (to avoid flakiness)"
                )

        except Exception as e:
            errors.append(f"Error checking {test_case}: {e}")

    for test_case in files:
        if "fail" in test_case:
            errors.append(f"test case {test_case} includes 'fail' in its name")

    return " ".join(errors)


def check_gaps_in_tests_numbers(file_paths, gap_threshold=100):
    test_numbers = set()

    pattern = re.compile(r"(\d+)")

    for file in file_paths:
        file_name = os.path.basename(file)
        match = pattern.search(file_name)
        if match:
            test_numbers.add(int(match.group(1)))

    sorted_numbers = sorted(test_numbers)
    large_gaps = []
    for i in range(1, len(sorted_numbers)):
        prev_num = sorted_numbers[i - 1]
        next_num = sorted_numbers[i]
        diff = next_num - prev_num
        if diff >= gap_threshold:
            large_gaps.append(f"Gap ({prev_num}, {next_num}) > {gap_threshold}")

    return large_gaps


def check_broken_links(path, exclude_paths):
    broken_symlinks = []

    for path in Path(path).rglob("*"):
        if any(exclude_path in str(path) for exclude_path in exclude_paths):
            continue
        if path.is_symlink():
            if not path.exists():
                broken_symlinks.append(str(path))

    if broken_symlinks:
        for symlink in broken_symlinks:
            print(symlink)
        return f"Broken symlinks found: {broken_symlinks}"
    else:
        return ""


def check_cpp_code():
    res, out, err = Shell.get_res_stdout_stderr(
        "./ci/jobs/scripts/check_style/check_cpp.sh"
    )
    if err:
        out += err
    return out


def check_repo_submodules():
    res, out, err = Shell.get_res_stdout_stderr(
        "./ci/jobs/scripts/check_style/check_submodules.sh"
    )
    if err:
        out += err
    return out

def check_nix_submodule_inputs():
    res, out, err = Shell.get_res_stdout_stderr(
        "./ci/jobs/scripts/check_style/check_nix_submodule_inputs.sh"
    )
    if err:
        out += err
    return out


def check_other():
    res, out, err = Shell.get_res_stdout_stderr(
        "./ci/jobs/scripts/check_style/various_checks.sh"
    )
    if err:
        out += err
    return out


def check_codespell():
    res, out, err = Shell.get_res_stdout_stderr(
        "./ci/jobs/scripts/check_style/check_typos.sh"
    )
    if err:
        out += err
    return out


def check_aspell():
    res, out, err = Shell.get_res_stdout_stderr(
        "./ci/jobs/scripts/check_style/check_aspell.sh"
    )
    if err:
        out += err
    return out


def check_mypy():
    res, out, err = Shell.get_res_stdout_stderr(
        "./ci/jobs/scripts/check_style/check-mypy"
    )
    if err:
        out += err
    return out


def check_pylint():
    res, out, err = Shell.get_res_stdout_stderr(
        "./ci/jobs/scripts/check_style/check-pylint"
    )
    if err:
        out += err
    return out


def check_file_names(files):
    files_set = set()
    for file in files:
        file_ = file.lower()
        if file_ in files_set:
            return f"Non-uniq file name in lower case: {file}"
        files_set.add(file_)
    return ""


def parse_args():
    parser = argparse.ArgumentParser(description="ClickHouse Style Check Job")
    parser.add_argument("--test", help="Sub check name", default="")
    return parser.parse_args()


if __name__ == "__main__":
    results = []
    args = parse_args()
    testpattern = args.test

    cpp_files = Utils.traverse_paths(
        include_paths=["./src", "./base", "./programs", "./utils"],
        exclude_paths=[
            "./base/glibc-compatibility",
            "./contrib/consistent-hashing",
            "./base/widechar_width",
        ],
        file_suffixes=[".h", ".cpp"],
    )

    yaml_workflow_files = Utils.traverse_paths(
        include_paths=["./.github"],
        exclude_paths=[],
        file_suffixes=[".yaml", ".yml"],
    )

    xml_files = Utils.traverse_paths(
        include_paths=["./tests", "./programs/"],
        exclude_paths=[],
        file_suffixes=[".xml"],
    )

    functional_test_files = Utils.traverse_paths(
        include_paths=["./tests/queries"],
        exclude_paths=[],
        file_suffixes=[".sql", ".sh", ".py", ".j2"],
    )

    testname = "whitespace_check"
    if testpattern.lower() in testname.lower():
        results.append(
            run_check_concurrent(
                check_name=testname,
                check_function=check_whitespaces,
                files=cpp_files,
            )
        )
    testname = "yamllint"
    if testpattern.lower() in testname.lower():
        results.append(
            run_check_concurrent(
                check_name=testname,
                check_function=check_yamllint,
                files=yaml_workflow_files,
            )
        )
    testname = "xmllint"
    if testpattern.lower() in testname.lower():
        results.append(
            run_check_concurrent(
                check_name=testname,
                check_function=check_xmllint,
                files=xml_files,
            )
        )
    testname = "functional_tests_check"
    if testpattern.lower() in testname.lower():
        results.append(
            run_check_concurrent(
                check_name=testname,
                check_function=check_functional_test_cases,
                files=functional_test_files,
            )
        )
    testname = "test_numbers_check"
    if testpattern.lower() in testname.lower():
        results.append(
            Result.from_commands_run(
                name=testname,
                command=check_gaps_in_tests_numbers,
                command_args=[functional_test_files],
            )
        )
    testname = "symlinks"
    if testpattern.lower() in testname.lower():
        results.append(
            Result.from_commands_run(
                name=testname,
                command=check_broken_links,
                command_kwargs={
                    "path": "./",
                    "exclude_paths": ["contrib/", "metadata/", "programs/server/data"],
                },
            )
        )
    testname = "cpp"
    if testpattern.lower() in testname.lower():
        results.append(
            Result.from_commands_run(
                name=testname,
                command=check_cpp_code,
            )
        )
    testname = "submodules"
    if testpattern.lower() in testname.lower():
        results.append(
            Result.from_commands_run(
                name=testname,
                command=check_repo_submodules,
            )
        )
        results.append(
            Result.from_commands_run(
                name=testname,
                command=check_nix_submodule_inputs,
            )
        )
    testname = "various"
    if testpattern.lower() in testname.lower():
        results.append(
            Result.from_commands_run(
                name=testname,
                command=check_other,
            )
        )
    testname = "codespell"
    if testpattern.lower() in testname.lower():
        results.append(
            Result.from_commands_run(
                name=testname,
                command=check_codespell,
            )
        )
    testname = "aspell"
    if testpattern.lower() in testname.lower():
        results.append(
            Result.from_commands_run(
                name=testname,
                command=check_aspell,
            )
        )

    testname = "mypy"
    if testpattern.lower() in testname.lower():
        results.append(
            Result.from_commands_run(
                name=testname,
                command=check_mypy,
            )
        )

    Result.create_from(results=results).complete_job()
