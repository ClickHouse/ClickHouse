#!/usr/bin/env python3
import logging
from github import Github

from commit_status_helper import (
    CI_STATUS_NAME,
    NotSet,
    get_commit,
    get_commit_filtered_statuses,
    post_commit_status,
)
from get_robot_token import get_best_robot_token
from pr_info import PRInfo


def main():
    logging.basicConfig(level=logging.INFO)

    pr_info = PRInfo(need_orgs=True)
    gh = Github(get_best_robot_token(), per_page=100)
    commit = get_commit(gh, pr_info.sha)

    statuses = [
        status
        for status in get_commit_filtered_statuses(commit)
        if status.context == CI_STATUS_NAME
    ]
    if not statuses:
        return
    status = statuses[0]
    if status.state == "pending":
        post_commit_status(
            commit,
            "success",
            status.target_url or NotSet,
            "All checks finished",
            CI_STATUS_NAME,
            pr_info,
        )


if __name__ == "__main__":
    main()
