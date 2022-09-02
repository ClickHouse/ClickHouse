#!/usr/bin/env python3

from commit_status_helper import get_commit


def _filter_statuses(statuses):
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
    return filt.values()


class RerunHelper:
    def __init__(self, gh, pr_info, check_name):
        self.gh = gh
        self.pr_info = pr_info
        self.check_name = check_name
        self.pygh_commit = get_commit(gh, self.pr_info.sha)
        self.statuses = _filter_statuses(self.pygh_commit.get_statuses())

    def is_already_finished_by_status(self):
        # currently we agree even for failed statuses
        for status in self.statuses:
            if self.check_name in status.context and status.state in (
                "success",
                "failure",
            ):
                return True
        return False

    def get_finished_status(self):
        for status in self.statuses:
            if self.check_name in status.context:
                return status
        return None
