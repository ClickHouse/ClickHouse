#!/usr/bin/env python3

from env_helper import GITHUB_REPOSITORY


def get_commit(gh, commit_sha):
    repo = gh.get_repo(GITHUB_REPOSITORY)
    commit = repo.get_commit(commit_sha)
    return commit

def post_commit_status(gh, sha, check_name, description, state, report_url):
    commit = get_commit(gh, sha)
    commit.create_status(context=check_name, description=description, state=state, target_url=report_url)
