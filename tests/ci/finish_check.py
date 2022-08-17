#!/usr/bin/env python3
import logging
import os
from github import Github
from pr_info import PRInfo, get_event
from get_robot_token import get_best_robot_token
from commit_status_helper import get_commit

NAME = 'Run Check (actions)'

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

    pr_info = PRInfo(get_event(), need_orgs=True)
    gh = Github(get_best_robot_token())
    commit = get_commit(gh, pr_info.sha)

    url = f"{os.getenv('GITHUB_SERVER_URL')}/{os.getenv('GITHUB_REPOSITORY')}/actions/runs/{os.getenv('GITHUB_RUN_ID')}"
    statuses = filter_statuses(list(commit.get_statuses()))
    if NAME in statuses and statuses[NAME].state == "pending":
        commit.create_status(context=NAME, description="All checks finished", state="success", target_url=url)
