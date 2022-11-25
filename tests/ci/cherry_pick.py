#!/usr/bin/env python3

import sys
import logging
import os
import subprocess

from env_helper import GITHUB_WORKSPACE, TEMP_PATH
from get_robot_token import get_parameter_from_ssm
from ssh import SSHKey
from cherry_pick_utils.backport import Backport
from cherry_pick_utils.cherrypick import CherryPick


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    repo_path = GITHUB_WORKSPACE
    temp_path = TEMP_PATH

    if not os.path.exists(temp_path):
        os.makedirs(temp_path)

    sys.path.append(os.path.join(repo_path, "utils/github"))

    with SSHKey("ROBOT_CLICKHOUSE_SSH_KEY"):
        token = get_parameter_from_ssm("github_robot_token_1")

        bp = Backport(
            token,
            os.environ.get("REPO_OWNER"),
            os.environ.get("REPO_NAME"),
            os.environ.get("REPO_TEAM"),
        )

        def cherrypick_run(token, pr, branch):
            return CherryPick(
                token,
                os.environ.get("REPO_OWNER"),
                os.environ.get("REPO_NAME"),
                os.environ.get("REPO_TEAM"),
                pr,
                branch,
            ).execute(repo_path, False)

        try:
            bp.execute(repo_path, "origin", None, cherrypick_run)
        except subprocess.CalledProcessError as e:
            logging.error(e.output)
