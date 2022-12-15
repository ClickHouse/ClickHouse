#!/usr/bin/env python3

from commit_status_helper import get_commit
from env_helper import GITHUB_JOB_URL
from get_robot_token import get_best_robot_token
from github_helper import GitHub
from pr_info import PRInfo
from release import RELEASE_READY_STATUS


def main():
    pr_info = PRInfo()
    gh = GitHub(get_best_robot_token(), create_cache_dir=False, per_page=100)
    commit = get_commit(gh, pr_info.sha)
    commit.create_status(
        context=RELEASE_READY_STATUS,
        description="the release can be created from the commit",
        state="success",
        target_url=GITHUB_JOB_URL(),
    )


if __name__ == "__main__":
    main()
