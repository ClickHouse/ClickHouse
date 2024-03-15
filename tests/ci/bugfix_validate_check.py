#!/usr/bin/env python3

import csv
import logging
import subprocess
import sys
from pathlib import Path
from typing import List, Sequence, Tuple

from ci_config import JobNames
from ci_utils import normalize_string
from env_helper import TEMP_PATH
from functional_test_check import NO_CHANGES_MSG
from report import (
    ERROR,
    FAIL,
    FAILURE,
    OK,
    SKIPPED,
    SUCCESS,
    JobReport,
    TestResult,
    TestResults,
)
from stopwatch import Stopwatch


def post_commit_status_from_file(file_path: Path) -> List[str]:
    with open(file_path, "r", encoding="utf-8") as f:
        res = list(csv.reader(f, delimiter="\t"))
    if len(res) < 1:
        raise IndexError(f'Can\'t read from "{file_path}"')
    if len(res[0]) != 3:
        raise IndexError(f'Can\'t read from "{file_path}"')
    return res[0]


def get_failed_test_cases(file_path: Path) -> List[TestResult]:
    job_report = JobReport.load(from_file=file_path)
    test_results = []  # type: List[TestResult]
    for tr in job_report.test_results:
        if tr.status == FAIL:
            if tr.name == NO_CHANGES_MSG:
                tr.status = SKIPPED
            else:
                tr.name = "[with NOT_OK]   " + tr.name
                tr.status = OK
        elif tr.status == OK:
            tr.name = "[with NOT_OK]   " + tr.name
            tr.status = FAIL
        else:
            # do not invert error status
            pass
        test_results.append(tr)
    return test_results


def process_all_results(
    file_paths: Sequence[Path],
) -> Tuple[str, str, TestResults]:
    all_results = []  # type: TestResults
    has_fail = False
    has_error = False
    has_ok = False
    for job_report_path in file_paths:
        test_results = get_failed_test_cases(job_report_path)
        for tr in test_results:
            if tr.status == FAIL:
                has_fail = True
            elif tr.status == ERROR:
                has_error = True
            elif tr.status == OK:
                has_ok = True
        all_results.extend(test_results)
    if has_error:
        status = ERROR
        description = "Some error(s) occured in tests"
    elif has_ok:
        status = SUCCESS
        description = "New test(s) reproduced a bug"
    elif has_fail:
        status = FAILURE
        description = "New test(s) failed to reproduce a bug"
    else:
        status = ERROR
        description = "Invalid job results"

    return status, description, all_results


def main():
    logging.basicConfig(level=logging.INFO)
    # args = parse_args()
    stopwatch = Stopwatch()
    jobs_to_validate = [JobNames.STATELESS_TEST_RELEASE, JobNames.INTEGRATION_TEST]
    functional_job_report_file = Path(TEMP_PATH) / "functional_test_job_report.json"
    integration_job_report_file = Path(TEMP_PATH) / "integration_test_job_report.json"
    jobs_report_files = {
        JobNames.STATELESS_TEST_RELEASE: functional_job_report_file,
        JobNames.INTEGRATION_TEST: integration_job_report_file,
    }
    jobs_scripts = {
        JobNames.STATELESS_TEST_RELEASE: "functional_test_check.py",
        JobNames.INTEGRATION_TEST: "integration_test_check.py",
    }

    for test_job in jobs_to_validate:
        report_file = jobs_report_files[test_job]
        test_script = jobs_scripts[test_job]
        if report_file.exists():
            report_file.unlink()
        extra_timeout_option = ""
        if test_job == JobNames.STATELESS_TEST_RELEASE:
            extra_timeout_option = str(3600)
        # "bugfix" must be present in checkname, as integration test runner checks this
        check_name = f"Validate bugfix: {test_job}"
        command = f"python3 {test_script} '{check_name}' {extra_timeout_option} --validate-bugfix --report-to-file {report_file}"
        print(f"Going to validate job [{test_job}], command [{command}]")
        _ = subprocess.run(
            command,
            stdout=sys.stdout,
            stderr=sys.stderr,
            text=True,
            check=False,
            shell=True,
        )
        assert (
            report_file.is_file()
        ), f"No job report [{report_file}] found after job execution"

    status, description, test_results = process_all_results(
        list(jobs_report_files.values())
    )

    additional_files = []
    for job_id, report_file in jobs_report_files.items():
        jr = JobReport.load(from_file=report_file)
        additional_files.append(report_file)
        for file in set(jr.additional_files):
            file_ = Path(file)
            file_name = file_.name
            file_name = file_name.replace(".", "__" + normalize_string(job_id) + ".", 1)
            file_ = file_.rename(file_.parent / file_name)
            additional_files.append(file_)

    JobReport(
        description=description,
        test_results=test_results,
        status=status,
        start_time=stopwatch.start_time_str,
        duration=stopwatch.duration_seconds,
        additional_files=additional_files,
    ).dump()


if __name__ == "__main__":
    main()
