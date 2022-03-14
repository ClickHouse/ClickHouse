#!/usr/bin/env python3

import time
import fnmatch
from env_helper import GITHUB_REPOSITORY
from ci_config import CI_CONFIG

RETRY = 5


def override_status(status, check_name, invert=False):
    if CI_CONFIG["tests_config"].get(check_name, {}).get("force_tests", False):
        return "success"

    if invert:
        if status == 'success':
            return 'error'
        return 'success'

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


def get_post_commit_status(gh, sha, check_name):
    MAX_PAGES_NUM = 100
    for i in range(RETRY):
        try:
            statuses = get_commit(gh, sha, 1).get_statuses()
            for num in range(MAX_PAGES_NUM):
                page = statuses.get_page(num)
                if not page:
                    break
                for status in page:
                    if fnmatch.fnmatch(status.context, check_name):
                        return status
                num += 1
            return None
        except Exception as ex:
            if i == RETRY - 1:
                raise ex
            time.sleep(i)
