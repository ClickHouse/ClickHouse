#!/usr/bin/env python3
import logging
from github import Github

from env_helper import GITHUB_RUN_URL
from pr_info import PRInfo
from get_robot_token import get_best_robot_token
from commit_status_helper import get_commit, get_commit_filtered_statuses

NAME = "Run Check"


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    pr_info = PRInfo(need_orgs=True)
    gh = Github(get_best_robot_token(), per_page=100)
    commit = get_commit(gh, pr_info.sha)

    url = GITHUB_RUN_URL
    statuses = get_commit_filtered_statuses(commit)
    pending_status = any(  # find NAME status in pending state
        True
        for status in statuses
        if status.context == NAME and status.state == "pending"
    )
    if pending_status:
        commit.create_status(
            context=NAME,
            description="All checks finished",
            state="success",
            target_url=url,
        )
