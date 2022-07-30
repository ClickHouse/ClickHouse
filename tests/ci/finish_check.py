#!/usr/bin/env python3
import logging
from github import Github

from env_helper import GITHUB_RUN_URL
from pr_info import PRInfo
from get_robot_token import get_best_robot_token
from commit_status_helper import get_commit

NAME = "Run Check"


def filter_statuses(statuses):
    """
    Squash statuses to latest state
    1. context="first", state="success", update_time=1
    2. context="second", state="success", update_time=2
    3. context="first", stat="failure", update_time=3
    =========>
    1. context="second", state="success"
    2. context="first", stat="failure"
    """
    filt = {}
    for status in sorted(statuses, key=lambda x: x.updated_at):
        filt[status.context] = status
    return filt


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    pr_info = PRInfo(need_orgs=True)
    gh = Github(get_best_robot_token())
    commit = get_commit(gh, pr_info.sha)

    url = GITHUB_RUN_URL
    statuses = filter_statuses(list(commit.get_statuses()))
    if NAME in statuses and statuses[NAME].state == "pending":
        commit.create_status(
            context=NAME,
            description="All checks finished",
            state="success",
            target_url=url,
        )
