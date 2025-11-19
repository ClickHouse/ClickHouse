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
from datetime import date, datetime, timedelta
from pathlib import Path
from subprocess import CalledProcessError
from typing import Iterable, List, Optional

from cache_utils import GitHubCache
from ci_buddy import CIBuddy
from ci_config import Labels
from ci_utils import Shell
from env_helper import (
    GITHUB_REPOSITORY,
    GITHUB_SERVER_URL,
    GITHUB_UPSTREAM_REPOSITORY,
    IS_CI,
    TEMP_PATH,
)
from get_robot_token import get_best_robot_token
from git_helper import GIT_PREFIX, git_runner, is_shallow, stash
from github_helper import GitHub, PullRequest, PullRequests, Repository
from report import GITHUB_JOB_URL
from s3_helper import S3Helper
from ssh import SSHKey
from synchronizer_utils import SYNC_PR_PREFIX


class ReleaseBranch:
    STALE_THRESHOLD = 24 * 3600
    CHERRYPICK_DESCRIPTION = f"""## Do not merge this PR manually

This pull-request is a first step of an automated backporting.
It contains changes similar to calling `git cherry-pick` locally.
If you intend to continue backporting the changes, then resolve all conflicts if any.
Otherwise, if you do not want to backport them, then just close this pull-request.

The check results does not matter at this step - you can safely ignore them.

### Troubleshooting

#### If the conflicts were resolved in a wrong way

If this cherry-pick PR is completely screwed by a wrong conflicts resolution, and you \
want to recreate it:

- delete the `{Labels.PR_CHERRYPICK}` label from the PR
- delete this branch from the repository

You also need to check the **Original pull-request** for `{Labels.PR_BACKPORTS_CREATED}` \
label, and  delete if it's presented there
"""
    BACKPORT_DESCRIPTION = """This pull-request is a last step of an automated \
backporting.
Treat it as a standard pull-request: look at the checks and resolve conflicts.
Merge it only if you intend to backport changes to the target branch, otherwise just \
close it.
"""
    PR_SOURCE_DESCRIPTION = ""
    REMOTE = ""

    @property
    def pr_source(self) -> str:
        if self.PR_SOURCE_DESCRIPTION:
            return self.PR_SOURCE_DESCRIPTION
        header = "\n\n### The PR source\n"
        if not IS_CI:
            self.PR_SOURCE_DESCRIPTION = (
                f"{header}The PR is created manually outside of the CI"
            )
        else:
            self.PR_SOURCE_DESCRIPTION = (
                f"{header}The PR is created in the [CI job]({GITHUB_JOB_URL()})"
            )

        return self.PR_SOURCE_DESCRIPTION

    def __init__(
        self,
        name: str,
        pr: PullRequest,
        repo: Repository,
    ):
        self.name = name
        self.pr = pr
        self.repo = repo

        self.cherrypick_branch = self.cp_branch(name, pr.number)
        self.backport_branch = self.bp_branch(name, pr.number)
        self.cherrypick_pr = None  # type: Optional[PullRequest]
        self.backport_pr = None  # type: Optional[PullRequest]
        self._backported = False

        self.pre_check()

    @staticmethod
    def cp_branch(name: str, pr_number: int) -> str:
        """
        Returns the name of the cherry-pick branch for the given release branch and PR
        number.
        """
        return f"cherrypick/{name}/{pr_number}"

    @staticmethod
    def bp_branch(name: str, pr_number: int) -> str:
        """
        Returns the name of the backport branch for the given release branch and PR
        number.
        """
        return f"backport/{name}/{pr_number}"

    def pre_check(self):
        self._backported = Shell.check(
            f"git merge-base --is-ancestor {self.pr.merge_commit_sha} {self.REMOTE}/{self.name}",
            verbose=True,
        )
        if self._backported:
            print(
                f"WARNING: Backport for PR [{self.pr}] is already present on {self.name}"
            )

    def pop_prs(self, prs: PullRequests) -> PullRequests:
        """the method processes all prs and pops the ReleaseBranch related prs"""
        to_pop = []  # type: List[int]
        for i, pr in enumerate(prs):
            if self.name not in pr.head.ref:
                # this pr is not for the current branch
                continue
            if pr.head.ref.startswith(f"cherrypick/{self.name}"):
                self.cherrypick_pr = pr
                to_pop.append(i)
            elif pr.head.ref.startswith(f"backport/{self.name}"):
                self.backport_pr = pr
                self._backported = True
                to_pop.append(i)
            else:
                assert False, f"BUG! Invalid PR's branch [{pr.head.ref}]"

        for i in reversed(to_pop):
            # Going from the tail to keep the order and pop greater index first
            prs.pop(i)
        return prs

    def process(  # pylint: disable=too-many-return-statements
        self, dry_run: bool
    ) -> None:
        if self.backported:
            return

        if not self.cherrypick_pr:
            if dry_run:
                logging.info(
                    "DRY RUN: Would create cherrypick PR for #%s", self.pr.number
                )
                return
            self.create_cherrypick()
        assert self.cherrypick_pr, "BUG!"

        if self.cherrypick_pr.mergeable and self.cherrypick_pr.state != "closed":
            if dry_run:
                logging.info(
                    "DRY RUN: Would merge cherry-pick PR for #%s", self.pr.number
                )
                return
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
        if self.cherrypick_pr.state == "closed":
            logging.info(
                "The cherry-pick PR #%s for PR #%s is discarded",
                self.cherrypick_pr.number,
                self.pr.number,
            )
            self._backported = True
            return
        logging.info(
            "Cherry-pick PR #%s for PR #%s has conflicts and unable to be merged",
            self.cherrypick_pr.number,
            self.pr.number,
        )
        self.ping_cherry_pick_assignees(dry_run)

    def create_cherrypick(self):
        # First, create backport branch:
        # Checkout release branch with discarding every change
        git_runner(f"{GIT_PREFIX} checkout -f {self.name}")
        # Create or reset backport branch
        git_runner(f"{GIT_PREFIX} checkout -B {self.backport_branch}")
        # Merge all changes from PR's the first parent commit w/o applying anything
        # It will allow to create a merge commit like it would be a cherry-pick
        first_parent = git_runner(f"git rev-parse {self.pr.merge_commit_sha}^1")
        git_runner(f"{GIT_PREFIX} merge -s ours --no-edit {first_parent}")

        # Second step, create cherrypick branch
        git_runner(
            f"{GIT_PREFIX} branch -f "
            f"{self.cherrypick_branch} {self.pr.merge_commit_sha}"
        )

        # Check if there are actually any changes between branches. If no, then no
        # other actions are required. It's possible when changes are backported
        # manually to the release branch already
        try:
            output = git_runner(
                f"{GIT_PREFIX} merge --no-commit --no-ff {self.cherrypick_branch}"
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
            git_runner(f"{GIT_PREFIX} reset --merge")
        else:
            # There are changes to apply, so continue
            git_runner(f"{GIT_PREFIX} reset --merge")

        # Push, create the cherry-pick PR, label and assign it
        for branch in [self.cherrypick_branch, self.backport_branch]:
            git_runner(f"{GIT_PREFIX} push -f {self.REMOTE} {branch}:{branch}")

        title = f"Cherry pick #{self.pr.number} to {self.name}: {self.pr.title}"
        self.cherrypick_pr = self.repo.create_pull(
            title=title,
            body=self.body_header() + self.CHERRYPICK_DESCRIPTION + self.pr_source,
            base=self.backport_branch,
            head=self.cherrypick_branch,
        )
        self.cherrypick_pr.add_to_labels(Labels.PR_CHERRYPICK)
        self.cherrypick_pr.add_to_labels(Labels.DO_NOT_TEST)
        if Labels.PR_CRITICAL_BUGFIX in [label.name for label in self.pr.labels]:
            self.cherrypick_pr.add_to_labels(Labels.PR_CRITICAL_BUGFIX)
        elif Labels.PR_BUGFIX in [label.name for label in self.pr.labels]:
            self.cherrypick_pr.add_to_labels(Labels.PR_BUGFIX)
        self._assign_new_pr(self.cherrypick_pr)
        # update cherrypick PR to get the state for PR.mergable
        self.cherrypick_pr.update()

    def create_backport(self):
        assert self.cherrypick_pr is not None
        # Checkout the backport branch from the remote and make all changes to
        # apply like they are only one cherry-pick commit on top of release
        logging.info("Creating backport for PR #%s", self.pr.number)
        git_runner(f"{GIT_PREFIX} checkout -f {self.backport_branch}")
        git_runner(f"{GIT_PREFIX} pull --ff-only {self.REMOTE} {self.backport_branch}")
        merge_base = git_runner(
            f"{GIT_PREFIX} merge-base "
            f"{self.REMOTE}/{self.name} {self.backport_branch}"
        )
        git_runner(f"{GIT_PREFIX} reset --soft {merge_base}")
        title = f"Backport #{self.pr.number} to {self.name}: {self.pr.title}"
        git_runner(f"{GIT_PREFIX} commit --allow-empty -F -", input=title)

        # Push with force, create the backport PR, lable and assign it
        git_runner(
            f"{GIT_PREFIX} push -f {self.REMOTE} "
            f"{self.backport_branch}:{self.backport_branch}"
        )
        self.backport_pr = self.repo.create_pull(
            title=title,
            body=self.body_header() + self.BACKPORT_DESCRIPTION + self.pr_source,
            base=self.name,
            head=self.backport_branch,
        )
        self.backport_pr.add_to_labels(Labels.PR_BACKPORT)
        if Labels.PR_CRITICAL_BUGFIX in [label.name for label in self.pr.labels]:
            self.backport_pr.add_to_labels(Labels.PR_CRITICAL_BUGFIX)
        elif Labels.PR_BUGFIX in [label.name for label in self.pr.labels]:
            self.backport_pr.add_to_labels(Labels.PR_BUGFIX)
        self._assign_new_pr(self.backport_pr)

    def body_header(self) -> str:
        """
        Returns the description of the original PR, which is used in the
        cherry-pick and backport PRs.
        """
        upstream_pr = ""
        if self.pr.head.ref.startswith(SYNC_PR_PREFIX):
            try:
                upstream_pr_number = int(self.pr.head.ref.rsplit("/", maxsplit=1)[-1])
                upstream_pr = (
                    f"Upstream pull-request {GITHUB_SERVER_URL}/"
                    f"{GITHUB_UPSTREAM_REPOSITORY}/pull/{upstream_pr_number}\n"
                )
            except ValueError:
                logging.error(
                    "Sync PR #%s has an invalid head ref: %s",
                    self.pr.number,
                    self.pr.head.ref,
                )
        original_pr = f"Original pull-request {self.pr.html_url}\n"
        cherrypick_pr = ""
        if self.cherrypick_pr is not None:
            cherrypick_pr = f"Cherry-pick pull-request {self.cherrypick_pr.html_url}\n"
        return f"{upstream_pr}{original_pr}{cherrypick_pr}\n"

    def ping_cherry_pick_assignees(self, dry_run: bool) -> None:
        assert self.cherrypick_pr is not None
        logging.info(
            "Checking if cherry-pick PR #%s needs to be pinged",
            self.cherrypick_pr.number,
        )
        # The `updated_at` is Optional[datetime]
        cherrypick_updated_ts = (
            self.cherrypick_pr.updated_at or datetime.now()
        ).timestamp()
        since_updated = int(datetime.now().timestamp() - cherrypick_updated_ts)
        since_updated_str = (
            f"{since_updated // 86400}d{since_updated // 3600 % 24}h"
            f"{since_updated // 60 % 60}m{since_updated % 60}s"
        )
        if since_updated < self.STALE_THRESHOLD:
            logging.info(
                "The cherry-pick PR was updated %s ago, "
                "waiting for the next running",
                since_updated_str,
            )
            return
        assignees = ", ".join(f"@{user.login}" for user in self.cherrypick_pr.assignees)
        comment_body = (
            f"Dear {assignees}, the PR is not updated for {since_updated_str}. "
            "Please, either resolve the conflicts, or close it to finish "
            f"the backport process of #{self.pr.number}"
        )
        if dry_run:
            logging.info(
                "DRY RUN: would comment the cherry-pick PR #%s:\n",
                self.cherrypick_pr.number,
            )
            return

        self.cherrypick_pr.create_issue_comment(comment_body)

    def _assign_new_pr(self, new_pr: PullRequest) -> None:
        """Assign `new_pr` to author, merger and assignees of an original PR"""
        # It looks there some race when multiple .add_to_assignees are executed,
        # so we'll add all at once
        assignees = [self.pr.user, self.pr.merged_by]
        if self.pr.assignees:
            assignees.extend(self.pr.assignees)
        assignees = [
            a
            for a in assignees
            if "robot-clickhouse" not in str(a) and "clickhouse-gh" not in str(a)
        ]
        logging.info(
            "Assing #%s to author and assignees of the original PR: %s",
            new_pr.number,
            ", ".join(user.login for user in assignees),
        )
        new_pr.add_to_assignees(*assignees)

    @property
    def backported(self) -> bool:
        return self._backported

    def __repr__(self):
        return self.name


class BackportPRs:
    def __init__(
        self,
        gh: GitHub,
        repo: str,
        dry_run: bool,
    ):
        self.gh = gh
        self._repo_name = repo
        self.dry_run = dry_run

        self.must_create_backport_labels = [Labels.MUST_BACKPORT]

        self._remote = ""
        self._remote_line = ""

        self._repo = None  # type: Optional[Repository]
        self.release_prs = []  # type: PullRequests
        self.release_branches = []  # type: List[str]
        self.labels_to_backport = []  # type: List[str]
        self.prs_for_backport = []  # type: PullRequests
        self.error = None  # type: Optional[Exception]

    @property
    def remote_line(self) -> str:
        if not self._remote_line:
            # lines of "origin	git@github.com:ClickHouse/ClickHouse.git (fetch)"
            remotes = git_runner("git remote -v").split("\n")
            # We need the first word from the first matching result
            self._remote_line = next(
                iter(
                    remote
                    for remote in remotes
                    if f"github.com/{self._repo_name}" in remote  # https
                    or f"github.com:{self._repo_name}" in remote  # ssh
                )
            )

        return self._remote_line

    @property
    def remote(self) -> str:
        if not self._remote:
            self._remote = self.remote_line.split(maxsplit=1)[0]
            git_runner(f"git fetch {self._remote}")
            ReleaseBranch.REMOTE = self._remote
        return self._remote

    @property
    def is_remote_ssh(self) -> bool:
        return "github.com:" in self.remote_line

    def receive_release_prs(self):
        logging.info("Getting release PRs")
        self.release_prs = self.gh.get_release_pulls(self._repo_name)
        self.release_branches = [pr.head.ref for pr in self.release_prs]

        self.labels_to_backport = [
            # compatibility labels for the cloud and public release branches
            f"v{branch.replace('release/', '')}-must-backport"
            for branch in self.release_branches
        ]

        logging.info("Active releases: %s", ", ".join(self.release_branches))

    def update_local_release_branches(self):
        logging.info("Update local release branches")
        branches = git_runner("git branch").split()
        for branch in self.release_branches:
            if branch not in branches:
                # the local branch is not exist, so continue
                continue
            local_ref = git_runner(f"git rev-parse {branch}")
            remote_ref = git_runner(f"git rev-parse {self.remote}/{branch}")
            if local_ref == remote_ref:
                # Do not need to update, continue
                continue
            logging.info("Resetting %s to %s/%s", branch, self.remote, branch)
            git_runner(f"git branch -f {branch} {self.remote}/{branch}")

    def oldest_commit_date(self) -> date:
        # The dates of every commit in each release branche
        commit_dates = [
            commit
            for branch in self.release_branches
            for commit in git_runner(
                "git log --no-merges --format=format:%cs --reverse "
                f"{self.remote}/{self.default_branch}..{self.remote}/{branch}"
            ).split("\n")
        ]
        return min(date.fromisoformat(c_date) for c_date in commit_dates)

    def receive_prs_for_backport(
        self,
        since_date: Optional[date] = None,
        # The following arguments are used for a cross-repo labels synchronization
        labels_to_backport: Optional[Iterable[str]] = None,
        backport_created_label: str = Labels.PR_BACKPORTS_CREATED,
        repo_name: str = "",
    ) -> None:

        since_date = since_date or self.oldest_commit_date()
        labels_to_backport = (
            labels_to_backport
            or self.labels_to_backport + self.must_create_backport_labels
        )
        repo_name = repo_name or self.repo.full_name
        # To not have a possible TZ issues
        tomorrow = date.today() + timedelta(days=1)

        # The search API struggles to serve the heavy queries, so we limit the
        # updated date to 90 days ago. It improves the response quality by an order of
        # magnitude
        updated = (date.today() - timedelta(days=90)).isoformat() + "..*"

        query_args = {
            "query": f"type:pr repo:{repo_name} -label:{backport_created_label}",
            "label": ",".join(labels_to_backport),
            "updated": updated,
            "merged": [since_date, tomorrow],
        }
        logging.info("Query to find the backport PRs:\n %s", query_args)

        self.prs_for_backport = self.gh.get_pulls_from_search(**query_args)
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
                    "During processing the PR #%s error occurred: %s", pr.number, e
                )
                self.error = e

    def process_pr(self, pr: PullRequest) -> None:
        pr_labels = [label.name for label in pr.labels]

        if any(label in pr_labels for label in self.must_create_backport_labels):
            branches = [
                ReleaseBranch(br, pr, self.repo) for br in self.release_branches
            ]  # type: List[ReleaseBranch]
        else:
            branches = [
                ReleaseBranch(
                    (
                        br
                        if self._repo_name == "ClickHouse/ClickHouse"
                        else f"release/{br}"
                    ),
                    pr,
                    self.repo,
                )
                for br in [
                    label.split("-", 1)[0][1:]  # v21.8-must-backport
                    for label in pr_labels
                    if label in self.labels_to_backport
                ]
            ]
        assert branches, "BUG!"

        logging.info(
            "  PR #%s is supposed to be backported to %s",
            pr.number,
            ", ".join(map(str, branches)),
        )
        # All PRs for cherry-pick and backport branches as heads
        query_suffix = " ".join(
            [
                f"head:{branch.backport_branch} head:{branch.cherrypick_branch}"
                for branch in branches
            ]
        )
        bp_cp_prs = self.gh.get_pulls_from_search(
            query=f"type:pr repo:{self._repo_name} {query_suffix}",
            label=f"{Labels.PR_BACKPORT},{Labels.PR_CHERRYPICK}",
        )
        for br in branches:
            bp_cp_prs = br.pop_prs(bp_cp_prs)
        assert not bp_cp_prs, "BUG!"

        for br in branches:
            br.process(self.dry_run)

        if all(br.backported for br in branches):
            # And check it after the running
            self.mark_pr_backported(pr)

    def mark_pr_backported(self, pr: PullRequest) -> None:
        if self.dry_run:
            logging.info("DRY RUN: would mark PR #%s as done", pr.number)
            return
        pr.add_to_labels(Labels.PR_BACKPORTS_CREATED)
        logging.info(
            "PR #%s is successfully labeled with `%s`",
            pr.number,
            Labels.PR_BACKPORTS_CREATED,
        )

    @property
    def repo(self) -> Repository:
        if self._repo is None:
            self._repo = self.gh.get_repo(self._repo_name)
        return self._repo

    @property
    def default_branch(self) -> str:
        return self.repo.default_branch


class CherryPickPRs:
    def __init__(self, gh: GitHub, repo: str, dry_run: bool):
        self.gh = gh
        self.repo = gh.get_repo(repo)
        self.dry_run = dry_run
        self.error = None  # type: Optional[Exception]

        self.release_prs = gh.get_release_pulls(repo)
        logging.info(f"Release PRs: {self.release_prs}")

    def get_open_cherry_pick_prs(self) -> PullRequests:
        """
        Get all open cherry-pick PRs in the repository.
        """
        query_args = {
            "query": f"type:pr repo:{self.repo.full_name} label:{Labels.PR_CHERRYPICK}",
            "state": "open",
        }
        logging.info("Query to find the cherry-pick PRs:\n %s", query_args)
        return self.gh.get_pulls_from_search(**query_args)

    def check_open_prs(self) -> None:
        """
        After the cherry-pick PRs are closed, the original PRs are marked as
        `pr-backports-created`. If the cherry-pick PR is reopened, we remove this label
        """
        try:
            prs = self.get_open_cherry_pick_prs()
            logging.info("Found %d open cherry-pick PRs", len(prs))
        except Exception as e:
            logging.error("Error while getting open cherry-pick PRs: %s", e)
            self.error = e
            return

        # We need to check if there is an open release branch for each cherry-pick PR
        for pr in prs.copy():
            # We need to copy the list, since we will modify it
            #
            try:
                if not self._check_opened_release(pr):
                    # The cherry-pick PR is not opened in any of the release branches,
                    # so we can skip it
                    prs.remove(pr)
                    continue
            except Exception as e:
                logging.error(
                    "Error while checking opened release branch for cherry-pick PR #%s: %s",
                    pr.number,
                    e,
                )
                self.error = e
                continue

        # And then, we need to check if the original PR is marked as backported for any
        # open cherry-pick PR
        for pr in prs:
            try:
                self._remove_backported_label(pr)
            except Exception as e:
                logging.error(
                    "Error while pinging stale cherry-pick PR #%s: %s", pr.number, e
                )
                self.error = e
                continue

    def _check_opened_release(self, cpp: PullRequest) -> bool:
        """
        Check if the original PR is opened in any of the release branches.
        """
        # The cherry-pick's head ref is like cherrypick/{release_name}/12345,
        # so we can extract the release name from it, and then try to find it in the
        # self.release_prs
        original_pr_number = int(cpp.head.ref.rsplit("/", maxsplit=1)[-1])
        if cpp.head.ref in [
            ReleaseBranch.cp_branch(r.head.ref, original_pr_number)
            for r in self.release_prs
        ]:
            # The release branch is opened, so we can continue
            return True
        release_name = cpp.head.ref.split("/", maxsplit=1)[1].rsplit("/", maxsplit=1)[0]
        logging.info(
            "An opened release PR `%s` for cherry-pick PR %s is not found, going to close it",
            release_name,
            cpp.html_url,
        )
        if self.dry_run:
            logging.info(
                "DRY RUN: would close and leave a comment in the cherry-pick PR #%s",
                cpp.number,
            )
            return False
        cpp.create_issue_comment(
            f"The release branch `{release_name}` for the cherry-pick doesn't have "
            "an opened PR, closing this PR."
        )
        cpp.edit(state="closed")
        return False

    def _remove_backported_label(self, pr: PullRequest) -> None:
        # The `updated_at` is Optional[datetime]
        try:
            original_pr_number = int(pr.head.ref.rsplit("/")[-1])
        except ValueError:
            logging.error(
                "Cherry-pick PR #%s has an invalid head ref: %s",
                pr.number,
                pr.head.ref,
            )
            raise

        original_pr = self.gh.get_pull_cached(self.repo, original_pr_number)
        if not any(l.name == Labels.PR_BACKPORTS_CREATED for l in original_pr.labels):
            # The original PR is not marked as backported, so nothing to do
            return

        assignees = ", ".join(f"@{user.login}" for user in pr.assignees)
        comment_body = (
            f"Dear {assignees}, this PR is opened while #{original_pr.number} was "
            f"marked as backported. The `{Labels.PR_BACKPORTS_CREATED}` is removed, so "
            "the original PR can be processed again.\n\n"
            "If the cherry-pick is not needed anymore, then just close this PR."
        )
        logging.info(
            "Label %s should be removed from from #%s due opened cherry-pick PR #%s",
            Labels.PR_BACKPORTS_CREATED,
            original_pr.number,
            pr.number,
        )
        if self.dry_run:
            logging.info(
                "DRY RUN: would remove label and comment the cherry-pick PR #%s:\n%s",
                pr.number,
                comment_body,
            )
            return

        original_pr.remove_from_labels(Labels.PR_BACKPORTS_CREATED)
        pr.create_issue_comment(comment_body)


def parse_args():
    parser = argparse.ArgumentParser(
        "Create cherry-pick and backport PRs",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--token", help="github token, if not set, used from smm")
    parser.add_argument("--repo", default=GITHUB_REPOSITORY, help="repo owner/name")
    parser.add_argument("--dry-run", action="store_true", help="do not create anything")

    parser.add_argument(
        "--debug-helpers",
        action="store_true",
        help="add debug logging for git_helper and github_helper",
    )
    return parser.parse_args()


def main():
    temp_path = Path(TEMP_PATH)
    temp_path.mkdir(parents=True, exist_ok=True)

    args = parse_args()
    if args.debug_helpers:
        logging.getLogger("github_helper").setLevel(logging.DEBUG)
        logging.getLogger("git_helper").setLevel(logging.DEBUG)
    token = args.token or get_best_robot_token()

    gh = GitHub(token)
    temp_path = Path(TEMP_PATH)
    gh_cache = GitHubCache(gh.cache_path, temp_path, S3Helper())
    gh_cache.download()

    # First, check if some cherry-pick PRs are reopened and original PRs are mared as
    # done
    cpp = CherryPickPRs(gh, args.repo, args.dry_run)
    cpp.check_open_prs()

    bpp = BackportPRs(gh, args.repo, args.dry_run)

    bpp.gh.cache_path = temp_path / "gh_cache"
    bpp.receive_release_prs()
    bpp.update_local_release_branches()
    bpp.receive_prs_for_backport()
    bpp.process_backports()
    gh_cache.upload()

    errors = [e for e in (bpp.error, cpp.error) if e is not None]
    if any(errors):
        logging.error("Finished successfully, but errors occurred!")
        raise errors[0]


if __name__ == "__main__":
    logging.getLogger().setLevel(level=logging.INFO)

    assert not is_shallow()
    try:
        with stash():
            if os.getenv("ROBOT_CLICKHOUSE_SSH_KEY", ""):
                with SSHKey("ROBOT_CLICKHOUSE_SSH_KEY"):
                    main()
            else:
                main()

    except Exception as e:
        if IS_CI:
            ci_buddy = CIBuddy()
            ci_buddy.post_job_error(
                f"The backport process finished with errors: {e}",
                with_instance_info=True,
                with_wf_link=True,
                critical=True,
            )
