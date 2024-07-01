#!/usr/bin/env python3

import argparse
import logging
import os

from commit_status_helper import NotSet, get_commit, post_commit_status
from env_helper import GITHUB_JOB_URL
from get_robot_token import get_best_robot_token
from github_helper import GitHub
from pr_info import PRInfo
from release import RELEASE_READY_STATUS
from git_helper import commit as commit_arg


def main():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="mark the commit as ready for release",
    )
    parser.add_argument(
        "--token",
        default=os.getenv("GITHUB_TOKEN", ""),
        required=False,
        help="GitHub token to authorize, required if commit is set. "
        "Can be set as GITHUB_TOKEN env",
    )
    parser.add_argument(
        "commit",
        nargs="?",
        type=commit_arg,
        help="if given, used instead of one from PRInfo",
    )
    args = parser.parse_args()
    url = ""
    description = "the release can be created from the commit, manually set"
    pr_info = None
    if not args.commit:
        pr_info = PRInfo()
        if pr_info.event == pr_info.default_event:
            raise ValueError("neither launched from the CI nor commit is given")
        args.commit = pr_info.sha
        url = GITHUB_JOB_URL()
        description = "the release can be created from the commit"
        args.token = args.token or get_best_robot_token()

    gh = GitHub(args.token, create_cache_dir=False)
    # Get the rate limits for a quick fail
    commit = get_commit(gh, args.commit)
    gh.get_rate_limit()
    post_commit_status(
        commit, "success", url or NotSet, description, RELEASE_READY_STATUS, pr_info
    )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    main()
