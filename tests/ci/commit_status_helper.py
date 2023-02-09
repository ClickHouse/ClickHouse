#!/usr/bin/env python3

import time
from env_helper import GITHUB_REPOSITORY
from ci_config import CI_CONFIG

RETRY = 5


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
