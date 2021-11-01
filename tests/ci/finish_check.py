#!/usr/bin/env python3
import logging
import json
import os
from github import Github
from pr_info import PRInfo
from get_robot_token import get_best_robot_token

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


def get_commit(gh, commit_sha):
    repo = gh.get_repo(os.getenv("GITHUB_REPOSITORY", "ClickHouse/ClickHouse"))
    commit = repo.get_commit(commit_sha)
    return commit

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    with open(os.getenv('GITHUB_EVENT_PATH'), 'r') as event_file:
        event = json.load(event_file)

    pr_info = PRInfo(event, need_orgs=True)
    gh = Github(get_best_robot_token())
    commit = get_commit(gh, pr_info.sha)

    url = f"https://github.com/ClickHouse/ClickHouse/actions/runs/{os.getenv('GITHUB_RUN_ID')}"
    statuses = filter_statuses(list(commit.get_statuses()))
    if NAME in statuses and statuses[NAME].state == "pending":
        commit.create_status(context=NAME, description="All checks finished", state="success", target_url=url)
