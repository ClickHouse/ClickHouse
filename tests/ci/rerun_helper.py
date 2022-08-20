#!/usr/bin/env python3
from typing import List, Optional

from commit_status_helper import get_commit
from github import Github
from github.CommitStatus import CommitStatus
from pr_info import PRInfo

CommitStatuses = List[CommitStatus]


class RerunHelper:
    def __init__(self, gh: Github, pr_info: PRInfo, check_name: str):
        self.gh = gh
        self.pr_info = pr_info
        self.check_name = check_name
        commit = get_commit(gh, self.pr_info.sha)
        if commit is None:
            raise ValueError(f"unable to receive commit for {pr_info.sha}")
        self.pygh_commit = commit
        self.statuses = self.ger_filtered_statuses()

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

    def ger_filtered_statuses(self) -> CommitStatuses:
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
        for status in sorted(
            self.pygh_commit.get_statuses(), key=lambda x: x.updated_at
        ):
            filt[status.context] = status
        return list(filt.values())
