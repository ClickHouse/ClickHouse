#!/usr/bin/env python3
from report import create_test_html_report
import logging
import subprocess
import os
import csv


def process_result(result_folder):
    test_results = []
    additional_files = []
    # Just upload all files from result_folder.
    # If task provides processed results, then it's responsible for content of result_folder.
    if os.path.exists(result_folder):
        test_files = [f for f in os.listdir(result_folder) if os.path.isfile(os.path.join(result_folder, f))]
        additional_files = [os.path.join(result_folder, f) for f in test_files]

    status_path = os.path.join(result_folder, "check_status.tsv")
    logging.info("Found test_results.tsv")
    status = list(csv.reader(open(status_path, 'r'), delimiter='\t'))
    if len(status) != 1 or len(status[0]) != 2:
        return "error", "Invalid check_status.tsv", test_results, additional_files
    state, description = status[0][0], status[0][1]

    try:
        results_path = os.path.join(result_folder, "test_results.tsv")
        test_results = list(csv.reader(open(results_path, 'r'), delimiter='\t'))
        if len(test_results) == 0:
            raise Exception("Empty results")

        return state, description, test_results, additional_files
    except Exception:
        if state == "success":
            state, description = "error", "Failed to read test_results.tsv"
        return state, description, test_results, additional_files

def get_pr_url_from_ref(ref):
    try:
        return ref.split("/")[2]
    except:
        return "master"

if __name__ == "__main__":
    repo_path = os.getenv("GITHUB_WORKSPACE", os.path.abspath("../../"))
    temp_path = os.getenv("RUNNER_TEMP", os.path.abspath("./temp"))
    run_id = os.getenv("GITHUB_RUN_ID", 0)
    commit_sha = os.getenv("GITHUB_SHA", 0)
    ref = os.getenv("GITHUB_REF", "")
    docker_image_version = os.getenv("DOCKER_IMAGE_VERSION", "latest")

    if not os.path.exists(temp_path):
        os.makedirs(temp_path)

    subprocess.check_output(f"docker run --cap-add=SYS_PTRACE --volume={repo_path}:/ClickHouse --volume={temp_path}:/test_output clickhouse/style-test:{docker_image_version}", shell=True)
    state, description, test_results, additional_files = process_result(temp_path)
    task_url = f"https://github.com/ClickHouse/ClickHouse/actions/runs/{run_id}"
    branch_url = "https://github.com/ClickHouse/ClickHouse/pull/" + str(get_pr_url_from_ref(ref))
    branch_name = "PR #" + str(get_pr_url_from_ref(ref))
    commit_url = f"https://github.com/ClickHouse/ClickHouse/commit/{commit_sha}"
    raw_log_url = "noop"

    html_report = create_test_html_report("Style Check (actions)", test_results, raw_log_url, task_url, branch_url, branch_name, commit_url)
    with open(os.path.join(temp_path, 'report.html'), 'w') as f:
        f.write(html_report)
