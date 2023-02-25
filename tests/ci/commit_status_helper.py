#!/usr/bin/env python3

import time
from typing import List

from github.Commit import Commit
from github.CommitStatus import CommitStatus

from env_helper import GITHUB_REPOSITORY
from ci_config import CI_CONFIG

RETRY = 5
CommitStatuses = List[CommitStatus]


def override_status(status, check_name):
    if CI_CONFIG["tests_config"][check_name].get("force_tests", False):
        return "success"
    return status


def get_commit(gh, commit_sha, retry_count=RETRY):
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


def get_commit_filtered_statuses(commit: Commit) -> CommitStatuses:
    """
    Squash statuses to latest state
    1. context="first", state="success", update_time=1
    2. context="second", state="success", update_time=2
    3. context="first", stat="failure", update_time=3
    =========>
    1. context="second", state="success"
    2. context="first", stat="failure"
    """
    filtered = {}
    for status in sorted(commit.get_statuses(), key=lambda x: x.updated_at):
        filtered[status.context] = status
    return list(filtered.values())
