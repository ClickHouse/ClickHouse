#!/usr/bin/env python3
from typing import Optional

from commit_status_helper import get_commit, get_commit_filtered_statuses
from github import Github
from github.CommitStatus import CommitStatus
from pr_info import PRInfo


# TODO: move it to commit_status_helper
class RerunHelper:
    def __init__(self, gh: Github, pr_info: PRInfo, check_name: str):
        self.gh = gh
        self.pr_info = pr_info
        self.check_name = check_name
        commit = get_commit(gh, self.pr_info.sha)
        if commit is None:
            raise ValueError(f"unable to receive commit for {pr_info.sha}")
        self.pygh_commit = commit
        self.statuses = get_commit_filtered_statuses(commit)

    def is_already_finished_by_status(self) -> bool:
        # currently we agree even for failed statuses
        for status in self.statuses:
            if self.check_name in status.context and status.state in (
                "success",
                "failure",
            ):
                return True
        return False

    def get_finished_status(self) -> Optional[CommitStatus]:
        for status in self.statuses:
            if self.check_name in status.context:
                return status
        return None
