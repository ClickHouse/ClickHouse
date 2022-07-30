#!/usr/bin/env python3

import csv
import os
import time
from typing import Optional

from ci_config import CI_CONFIG
from env_helper import GITHUB_REPOSITORY, GITHUB_RUN_URL
from github import Github
from github.Commit import Commit
from pr_info import SKIP_SIMPLE_CHECK_LABEL

RETRY = 5


def override_status(status, check_name, invert=False):
    if CI_CONFIG["tests_config"].get(check_name, {}).get("force_tests", False):
        return "success"

    if invert:
        if status == "success":
            return "error"
        return "success"

    return status


def get_commit(
    gh: Github, commit_sha: str, retry_count: int = RETRY
) -> Optional[Commit]:
    for i in range(retry_count):
        try:
            repo = gh.get_repo(GITHUB_REPOSITORY)
            commit = repo.get_commit(commit_sha)
            return commit
        except Exception as ex:
            if i == retry_count - 1:
                raise ex
            time.sleep(i)

    # just suppress warning
    return None


def post_commit_status(gh, sha, check_name, description, state, report_url):
    for i in range(RETRY):
        try:
            commit = get_commit(gh, sha, 1)
            commit.create_status(
                context=check_name,
                description=description,
                state=state,
                target_url=report_url,
            )
            break
        except Exception as ex:
            if i == RETRY - 1:
                raise ex
            time.sleep(i)


def post_commit_status_to_file(file_path, description, state, report_url):
    if os.path.exists(file_path):
        raise Exception(f'File "{file_path}" already exists!')
    with open(file_path, "w", encoding="utf-8") as f:
        out = csv.writer(f, delimiter="\t")
        out.writerow([state, report_url, description])


def remove_labels(gh, pr_info, labels_names):
    repo = gh.get_repo(GITHUB_REPOSITORY)
    pull_request = repo.get_pull(pr_info.number)
    for label in labels_names:
        pull_request.remove_from_labels(label)


def post_labels(gh, pr_info, labels_names):
    repo = gh.get_repo(GITHUB_REPOSITORY)
    pull_request = repo.get_pull(pr_info.number)
    for label in labels_names:
        pull_request.add_to_labels(label)


def fail_simple_check(gh, pr_info, description):
    if SKIP_SIMPLE_CHECK_LABEL in pr_info.labels:
        return
    commit = get_commit(gh, pr_info.sha)
    commit.create_status(
        context="Simple Check",
        description=description,
        state="failure",
        target_url=GITHUB_RUN_URL,
    )


def create_simple_check(gh, pr_info):
    commit = get_commit(gh, pr_info.sha)
    for status in commit.get_statuses():
        if "Simple Check" in status.context:
            return
    commit.create_status(
        context="Simple Check",
        description="Skipped",
        state="success",
        target_url=GITHUB_RUN_URL,
    )
