#!/usr/bin/env python3

import argparse
import logging
import os
import subprocess

from env_helper import GITHUB_WORKSPACE, TEMP_PATH
from get_robot_token import get_best_robot_token
from ssh import SSHKey
from cherry_pick_utils.backport import Backport
from cherry_pick_utils.cherrypick import CherryPick


def parse_args():
    parser = argparse.ArgumentParser("Create cherry-pick and backport PRs")
    parser.add_argument("--token", help="github token, if not set, used from smm")
    parser.add_argument("--dry-run", action="store_true", help="do not create anything")
    return parser.parse_args()


def main():
    args = parse_args()
    token = args.token or get_best_robot_token()

    bp = Backport(
        token,
        os.environ.get("REPO_OWNER"),
        os.environ.get("REPO_NAME"),
        os.environ.get("REPO_TEAM"),
    )

    cherry_pick = CherryPick(
        token,
        os.environ.get("REPO_OWNER"),
        os.environ.get("REPO_NAME"),
        os.environ.get("REPO_TEAM"),
        1,
        "master",
    )
    # Use the same _gh in both objects to have a proper cost
    # pylint: disable=protected-access
    for key in bp._gh.api_costs:
        if key in cherry_pick._gh.api_costs:
            bp._gh.api_costs[key] += cherry_pick._gh.api_costs[key]
    for key in cherry_pick._gh.api_costs:
        if key not in bp._gh.api_costs:
            bp._gh.api_costs[key] = cherry_pick._gh.api_costs[key]
    cherry_pick._gh = bp._gh
    # pylint: enable=protected-access

    def cherrypick_run(pr_data, branch):
        cherry_pick.update_pr_branch(pr_data, branch)
        return cherry_pick.execute(GITHUB_WORKSPACE, args.dry_run)

    try:
        bp.execute(GITHUB_WORKSPACE, "origin", None, cherrypick_run)
    except subprocess.CalledProcessError as e:
        logging.error(e.output)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    if not os.path.exists(TEMP_PATH):
        os.makedirs(TEMP_PATH)

    if os.getenv("ROBOT_CLICKHOUSE_SSH_KEY", ""):
        with SSHKey("ROBOT_CLICKHOUSE_SSH_KEY"):
            main()
    else:
        main()
