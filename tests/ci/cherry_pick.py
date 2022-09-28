#!/usr/bin/env python3
"""
A plan:
    - TODO: consider receiving GH objects cache from S3, but it's really a few
    of requests to API currently
    - Get all open release PRs (20.10, 21.8, 22.5, etc.)
    - Get all pull-requests between the date of the merge-base for the oldest PR with
    labels pr-must-backport and version-specific v21.8-must-backport, but without
    pr-backported
    - Iterate over gotten PRs:
        - for pr-must-backport:
            - check if all backport-PRs are created. If yes,
            set pr-backported label and finish
            - If not, create either cherrypick PRs or merge cherrypick (in the same
            stage, if mergable) and create backport-PRs
            - If successfull, set pr-backported label on the PR

        - for version-specific labels:
            - the same, check, cherry-pick, backport, pr-backported

Cherry-pick stage:
    - From time to time the cherry-pick fails, if it was done manually. In the
    case we check if it's even needed, and mark the release as done somehow.
"""

import argparse
import logging
import os
from contextlib import contextmanager
from datetime import date, timedelta
from subprocess import CalledProcessError
from typing import List, Optional

from env_helper import TEMP_PATH
from get_robot_token import get_best_robot_token
from git_helper import git_runner, is_shallow
from github_helper import (
    GitHub,
    PullRequest,
    PullRequests,
    Repository,
)
from ssh import SSHKey


class Labels:
    MUST_BACKPORT = "pr-must-backport"
    BACKPORT = "pr-backport"
    BACKPORTS_CREATED = "pr-backports-created"
    CHERRYPICK = "pr-cherrypick"
    DO_NOT_TEST = "do not test"


class ReleaseBranch:
    CHERRYPICK_DESCRIPTION = """This pull-request is a first step of an automated \
    backporting.
It contains changes like after calling a local command `git cherry-pick`.
If you intend to continue backporting this changes, then resolve all conflicts if any.
Otherwise, if you do not want to backport them, then just close this pull-request.

The check results does not matter at this step - you can safely ignore them.
Also this pull-request will be merged automatically as it reaches the mergeable state, \
    but you always can merge it manually.
"""
    BACKPORT_DESCRIPTION = """This pull-request is a last step of an automated \
backporting.
Treat it as a standard pull-request: look at the checks and resolve conflicts.
Merge it only if you intend to backport changes to the target branch, otherwise just \
    close it.
"""
    REMOTE = ""

    def __init__(self, name: str, pr: PullRequest):
        self.name = name
        self.pr = pr
        self.cherrypick_branch = f"cherrypick/{name}/{pr.merge_commit_sha}"
        self.backport_branch = f"backport/{name}/{pr.number}"
        self.cherrypick_pr = None  # type: Optional[PullRequest]
        self.backport_pr = None  # type: Optional[PullRequest]
        self._backported = None  # type: Optional[bool]
        self.git_prefix = (  # All commits to cherrypick are done as robot-clickhouse
            "git -c user.email=robot-clickhouse@clickhouse.com "
            "-c user.name=robot-clickhouse -c commit.gpgsign=false"
        )
        self.pre_check()

    def pre_check(self):
        branch_updated = git_runner(
            f"git branch -a --contains={self.pr.merge_commit_sha} "
            f"{self.REMOTE}/{self.name}"
        )
        if branch_updated:
            self._backported = True

    def pop_prs(self, prs: PullRequests):
        to_pop = []  # type: List[int]
        for i, pr in enumerate(prs):
            if self.name not in pr.head.ref:
                continue
            if pr.head.ref.startswith(f"cherrypick/{self.name}"):
                self.cherrypick_pr = pr
                to_pop.append(i)
            elif pr.head.ref.startswith(f"backport/{self.name}"):
                self.backport_pr = pr
                to_pop.append(i)
            else:
                logging.error(
                    "PR #%s doesn't head ref starting with known suffix",
                    pr.number,
                )
        for i in reversed(to_pop):
            # Going from the tail to keep the order and pop greater index first
            prs.pop(i)

    def process(self, dry_run: bool):
        if self.backported:
            return
        if not self.cherrypick_pr:
            if dry_run:
                logging.info(
                    "DRY RUN: Would create cherrypick PR for #%s", self.pr.number
                )
                return
            self.create_cherrypick()
        if self.backported:
            return
        if self.cherrypick_pr is not None:
            # Try to merge cherrypick instantly
            if self.cherrypick_pr.mergeable and self.cherrypick_pr.state != "closed":
                self.cherrypick_pr.merge()
                # The PR needs update, since PR.merge doesn't update the object
                self.cherrypick_pr.update()
            if self.cherrypick_pr.merged:
                if dry_run:
                    logging.info(
                        "DRY RUN: Would create backport PR for #%s", self.pr.number
                    )
                    return
                self.create_backport()
                return
            elif self.cherrypick_pr.state == "closed":
                logging.info(
                    "The cherrypick PR #%s for PR #%s is discarded",
                    self.cherrypick_pr.number,
                    self.pr.number,
                )
                self._backported = True
                return
            logging.info(
                "Cherrypick PR #%s for PR #%s have conflicts and unable to be merged",
                self.cherrypick_pr.number,
                self.pr.number,
            )

    def create_cherrypick(self):
        # First, create backport branch:
        # Checkout release branch with discarding every change
        git_runner(f"{self.git_prefix} checkout -f {self.name}")
        # Create or reset backport branch
        git_runner(f"{self.git_prefix} checkout -B {self.backport_branch}")
        # Merge all changes from PR's the first parent commit w/o applying anything
        # It will allow to create a merge commit like it would be a cherry-pick
        first_parent = git_runner(f"git rev-parse {self.pr.merge_commit_sha}^1")
        git_runner(f"{self.git_prefix} merge -s ours --no-edit {first_parent}")

        # Second step, create cherrypick branch
        git_runner(
            f"{self.git_prefix} branch -f "
            f"{self.cherrypick_branch} {self.pr.merge_commit_sha}"
        )

        # Check if there actually any changes between branches. If no, then no
        # other actions are required. It's possible when changes are backported
        # manually to the release branch already
        try:
            output = git_runner(
                f"{self.git_prefix} merge --no-commit --no-ff {self.cherrypick_branch}"
            )
            # 'up-to-date', 'up to date', who knows what else (╯°v°)╯ ^┻━┻
            if output.startswith("Already up") and output.endswith("date."):
                # The changes are already in the release branch, we are done here
                logging.info(
                    "Release branch %s already contain changes from %s",
                    self.name,
                    self.pr.number,
                )
                self._backported = True
                return
        except CalledProcessError:
            # There are most probably conflicts, they'll be resolved in PR
            git_runner(f"{self.git_prefix} reset --merge")
        else:
            # There are changes to apply, so continue
            git_runner(f"{self.git_prefix} reset --merge")

        # Push, create the cherrypick PR, lable and assign it
        for branch in [self.cherrypick_branch, self.backport_branch]:
            git_runner(f"{self.git_prefix} push -f {self.REMOTE} {branch}:{branch}")

        self.cherrypick_pr = self.pr.base.repo.create_pull(
            title=f"Cherry pick #{self.pr.number} to {self.name}: {self.pr.title}",
            body=f"Original pull-request #{self.pr.number}\n\n"
            f"{self.CHERRYPICK_DESCRIPTION}",
            base=self.backport_branch,
            head=self.cherrypick_branch,
        )
        self.cherrypick_pr.add_to_labels(Labels.CHERRYPICK)
        self.cherrypick_pr.add_to_labels(Labels.DO_NOT_TEST)
        self._assign_new_pr(self.cherrypick_pr)

    def create_backport(self):
        # Checkout the backport branch from the remote and make all changes to
        # apply like they are only one cherry-pick commit on top of release
        git_runner(f"{self.git_prefix} checkout -f {self.backport_branch}")
        git_runner(
            f"{self.git_prefix} pull --ff-only {self.REMOTE} {self.backport_branch}"
        )
        merge_base = git_runner(
            f"{self.git_prefix} merge-base "
            f"{self.REMOTE}/{self.name} {self.backport_branch}"
        )
        git_runner(f"{self.git_prefix} reset --soft {merge_base}")
        title = f"Backport #{self.pr.number} to {self.name}: {self.pr.title}"
        git_runner(f"{self.git_prefix} commit -a --allow-empty -F -", input=title)

        # Push with force, create the backport PR, lable and assign it
        git_runner(
            f"{self.git_prefix} push -f {self.REMOTE} "
            f"{self.backport_branch}:{self.backport_branch}"
        )
        self.backport_pr = self.pr.base.repo.create_pull(
            title=title,
            body=f"Original pull-request #{self.pr.number}\n"
            f"Cherry-pick pull-request #{self.cherrypick_pr.number}\n\n"
            f"{self.BACKPORT_DESCRIPTION}",
            base=self.name,
            head=self.backport_branch,
        )
        self.backport_pr.add_to_labels(Labels.BACKPORT)
        self._assign_new_pr(self.backport_pr)

    def _assign_new_pr(self, new_pr: PullRequest):
        """Assign `new_pr` to author, merger and assignees of an original PR"""
        # It looks there some race when multiple .add_to_assignees are executed,
        # so we'll add all at once
        assignees = [self.pr.user, self.pr.merged_by]
        if self.pr.assignees:
            assignees.extend(self.pr.assignees)
        logging.info(
            "Assing #%s to author and assignees of the original PR: %s",
            new_pr.number,
            ", ".join(user.login for user in assignees),
        )
        new_pr.add_to_assignees(*assignees)

    @property
    def backported(self) -> bool:
        if self._backported is not None:
            return self._backported
        return self.backport_pr is not None

    def __repr__(self):
        return self.name


class Backport:
    def __init__(self, gh: GitHub, repo: str, dry_run: bool):
        self.gh = gh
        self._repo_name = repo
        self.dry_run = dry_run

        self._query = f"type:pr repo:{repo}"
        self._remote = ""
        self._repo = None  # type: Optional[Repository]
        self.release_prs = []  # type: PullRequests
        self.release_branches = []  # type: List[str]
        self.labels_to_backport = []  # type: List[str]
        self.prs_for_backport = []  # type: PullRequests
        self.error = None  # type: Optional[Exception]

    @property
    def remote(self) -> str:
        if not self._remote:
            # lines of "origin	git@github.com:ClickHouse/ClickHouse.git (fetch)"
            remotes = git_runner("git remote -v").split("\n")
            # We need the first word from the first matching result
            self._remote = tuple(
                remote.split(maxsplit=1)[0]
                for remote in remotes
                if f"github.com/{self._repo_name}" in remote  # https
                or f"github.com:{self._repo_name}" in remote  # ssh
            )[0]
            git_runner(f"git fetch {self._remote}")
            ReleaseBranch.REMOTE = self._remote
        return self._remote

    def receive_release_prs(self):
        logging.info("Getting release PRs")
        self.release_prs = self.gh.get_pulls_from_search(
            query=f"{self._query} is:open",
            sort="created",
            order="asc",
            label="release",
        )
        self.release_branches = [pr.head.ref for pr in self.release_prs]
        self.labels_to_backport = [
            f"v{branch}-must-backport" for branch in self.release_branches
        ]
        logging.info("Active releases: %s", ", ".join(self.release_branches))

    def receive_prs_for_backport(self):
        # The commit is the oldest open release branch's merge-base
        since_commit = git_runner(
            f"git merge-base {self.remote}/{self.release_branches[0]} "
            f"{self.remote}/{self.default_branch}"
        )
        since_date = date.fromisoformat(
            git_runner.run(f"git log -1 --format=format:%cs {since_commit}")
        )
        # To not have a possible TZ issues
        tomorrow = date.today() + timedelta(days=1)
        logging.info("Receive PRs suppose to be backported")
        self.prs_for_backport = self.gh.get_pulls_from_search(
            query=f"{self._query} -label:{Labels.BACKPORTS_CREATED}",
            label=",".join(self.labels_to_backport + [Labels.MUST_BACKPORT]),
            merged=[since_date, tomorrow],
        )
        logging.info(
            "PRs to be backported:\n %s",
            "\n ".join([pr.html_url for pr in self.prs_for_backport]),
        )

    def process_backports(self):
        for pr in self.prs_for_backport:
            try:
                self.process_pr(pr)
            except Exception as e:
                logging.error(
                    "During processing the PR #%s error occured: %s", pr.number, e
                )
                self.error = e

    def process_pr(self, pr: PullRequest):
        pr_labels = [label.name for label in pr.labels]
        if Labels.MUST_BACKPORT in pr_labels:
            branches = [
                ReleaseBranch(br, pr) for br in self.release_branches
            ]  # type: List[ReleaseBranch]
        else:
            branches = [
                ReleaseBranch(br, pr)
                for br in [
                    label.split("-", 1)[0][1:]  # v21.8-must-backport
                    for label in pr_labels
                    if label in self.labels_to_backport
                ]
            ]
        if not branches:
            # This is definitely some error. There must be at least one branch
            # It also make the whole program exit code non-zero
            self.error = Exception(
                f"There are no branches to backport PR #{pr.number}, logical error"
            )
            raise self.error

        logging.info(
            "  PR #%s is suppose to be backported to %s",
            pr.number,
            ", ".join(map(str, branches)),
        )
        # All PRs for cherrypick and backport branches as heads
        query_suffix = " ".join(
            [
                f"head:{branch.backport_branch} head:{branch.cherrypick_branch}"
                for branch in branches
            ]
        )
        bp_cp_prs = self.gh.get_pulls_from_search(
            query=f"{self._query} {query_suffix}",
        )
        for br in branches:
            br.pop_prs(bp_cp_prs)

        if bp_cp_prs:
            # This is definitely some error. All prs must be consumed by
            # branches with ReleaseBranch.pop_prs. It also make the whole
            # program exit code non-zero
            self.error = Exception(
                "The following PRs are not filtered by release branches:\n"
                "\n".join(map(str, bp_cp_prs))
            )
            raise self.error

        if all(br.backported for br in branches):
            # Let's check if the PR is already backported
            self.mark_pr_backported(pr)
            return

        for br in branches:
            br.process(self.dry_run)

        if all(br.backported for br in branches):
            # And check it after the running
            self.mark_pr_backported(pr)

    def mark_pr_backported(self, pr: PullRequest):
        if self.dry_run:
            logging.info("DRY RUN: would mark PR #%s as done", pr.number)
            return
        pr.add_to_labels(Labels.BACKPORTS_CREATED)
        logging.info(
            "PR #%s is successfully labeled with `%s`",
            pr.number,
            Labels.BACKPORTS_CREATED,
        )

    @property
    def repo(self) -> Repository:
        if self._repo is None:
            try:
                self._repo = self.release_prs[0].base.repo
            except IndexError as exc:
                raise Exception(
                    "`repo` is available only after the `receive_release_prs`"
                ) from exc
        return self._repo

    @property
    def default_branch(self) -> str:
        return self.repo.default_branch


def parse_args():
    parser = argparse.ArgumentParser("Create cherry-pick and backport PRs")
    parser.add_argument("--token", help="github token, if not set, used from smm")
    parser.add_argument(
        "--repo", default="ClickHouse/ClickHouse", help="repo owner/name"
    )
    parser.add_argument("--dry-run", action="store_true", help="do not create anything")
    parser.add_argument(
        "--debug-helpers",
        action="store_true",
        help="add debug logging for git_helper and github_helper",
    )
    return parser.parse_args()


@contextmanager
def clear_repo():
    orig_ref = git_runner("git branch --show-current") or git_runner(
        "git rev-parse HEAD"
    )
    try:
        yield
    except (Exception, KeyboardInterrupt):
        git_runner(f"git checkout -f {orig_ref}")
        raise
    else:
        git_runner(f"git checkout -f {orig_ref}")


@contextmanager
def stash():
    need_stash = bool(git_runner("git diff HEAD"))
    if need_stash:
        git_runner("git stash push --no-keep-index -m 'running cherry_pick.py'")
    try:
        with clear_repo():
            yield
    except (Exception, KeyboardInterrupt):
        if need_stash:
            git_runner("git stash pop")
        raise
    else:
        if need_stash:
            git_runner("git stash pop")


def main():
    if not os.path.exists(TEMP_PATH):
        os.makedirs(TEMP_PATH)

    args = parse_args()
    if args.debug_helpers:
        logging.getLogger("github_helper").setLevel(logging.DEBUG)
        logging.getLogger("git_helper").setLevel(logging.DEBUG)
    token = args.token or get_best_robot_token()

    gh = GitHub(token, per_page=100)
    bp = Backport(gh, args.repo, args.dry_run)
    bp.gh.cache_path = str(f"{TEMP_PATH}/gh_cache")
    bp.receive_release_prs()
    bp.receive_prs_for_backport()
    bp.process_backports()
    if bp.error is not None:
        logging.error("Finished successfully, but errors occured!")
        raise bp.error


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    assert not is_shallow()
    with stash():
        if os.getenv("ROBOT_CLICKHOUSE_SSH_KEY", ""):
            with SSHKey("ROBOT_CLICKHOUSE_SSH_KEY"):
                main()
        else:
            main()
