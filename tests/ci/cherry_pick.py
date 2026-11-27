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
            - If successful, set pr-backported label on the PR

        - for version-specific labels (e.g. v25.12-must-backport):
            - the label marks the OLDEST release the PR must reach. Backport to
            that release AND to every newer active release branch, then the same
            check, cherry-pick, backport, pr-backported

Cherry-pick stage:
    - From time to time the cherry-pick fails, if it was done manually. In the
    case we check if it's even needed, and mark the release as done somehow.
    - A cherry-pick PR that conflicts is retried on every run against the
    current release branch (`ReleaseBranch._retry_cherrypick`). Conflicts are
    very often caused by a prerequisite backport that has not landed yet, and
    both branches of the cherry-pick PR are frozen at the moment the conflict
    was found, so once the prerequisite arrives the PR heals itself instead of
    waiting for someone to close it and let the bot start over.

The cross-repo synchronization is described in the KB article:
https://github.com/ClickHouse/internal-knowledge-base/issues/452
"""

import argparse
import logging
import os
import shlex
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from subprocess import CalledProcessError
from typing import Dict, Iterable, List, Optional, Tuple

from github.GithubException import GithubException

from cache_utils import GitHubCache
from cherry_pick_branches import (
    branch_version,
    label_version,
    select_backport_branches,
)
from ci_buddy import CIBuddy
from ci_utils import Shell
from env_helper import (
    GITHUB_REPOSITORY,
    GITHUB_SERVER_URL,
    GITHUB_UPSTREAM_REPOSITORY,
    IS_CI,
    TEMP_PATH,
)
from get_robot_token import get_best_robot_token
from git_helper import GIT_PREFIX, git_runner, is_shallow, removeprefix, stash
from github_helper import (
    GitHub,
    PullRequest,
    PullRequestInfo,
    PullRequests,
    Repository,
)
from pr_info import Labels
from report import GITHUB_JOB_URL
from s3_helper import S3Helper
from ssh import SSHKey
from synchronizer_utils import SYNC_PR_PREFIX


class BackportException(Exception):
    pass


def recover_git_state() -> None:
    """
    Best-effort recovery of the working tree after a git command crashed
    (e.g. an internal assertion in `merge-ort`) and left `.git/index.lock`
    behind. In that state subsequent commands -- including
    `git merge --abort` -- fail with "Unable to create '.git/index.lock'",
    which would otherwise poison every later PR processed in the same run.
    """
    try:
        # `--absolute-git-dir` -- avoid a relative `.git` resolved against
        # Python's cwd (which is `tests/ci/`, not the repo root).
        git_dir = git_runner("git rev-parse --absolute-git-dir")
    except CalledProcessError:
        return
    lock = Path(git_dir) / "index.lock"
    if lock.exists():
        logging.warning("Removing stale %s left by a crashed git process", lock)
        try:
            lock.unlink()
        except OSError as e:
            logging.error("Failed to remove %s: %s", lock, e)
            return
    # Best-effort cleanup of any in-progress merge / cherry-pick and the
    # working tree. None of these are required to succeed -- they only run
    # to bring the tree back to a usable state for the next PR.
    for cmd in (
        f"{GIT_PREFIX} merge --abort",
        f"{GIT_PREFIX} cherry-pick --abort",
        f"{GIT_PREFIX} reset --hard HEAD",
    ):
        try:
            git_runner(cmd)
        except CalledProcessError as e:
            logging.info("recover_git_state: %s -> %s (ignored)", cmd, e)


class ReleaseBranch:
    CHERRYPICK_DESCRIPTION = f"""## Do not merge this PR manually

This pull-request is a first step of an automated backporting.
It contains changes similar to calling `git cherry-pick` locally.
If you intend to continue backporting the changes, then resolve all conflicts if any.
Otherwise, if you do not want to backport them, then just close this pull-request.

The check results does not matter at this step - you can safely ignore them.

### Before you resolve anything

Conflicts are often caused by a prerequisite change that has not been backported \
yet, rather than by a real divergence. The bot re-tries this cherry-pick against \
the release branch on every run, so if that is the case here it will merge itself \
as soon as the prerequisite lands, and you will see a comment saying so. Manual \
resolution is only needed while the conflict persists.

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
    # GitHub recomputes a PR's `mergeable` asynchronously after a push and
    # reports None meanwhile. `_retry_cherrypick` waits this long for it so the
    # same run can finish the healed cherry-pick instead of the next one.
    MERGEABLE_POLL_ATTEMPTS = 3
    MERGEABLE_POLL_SECONDS = 5

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
        to_pop: List[int] = []
        for i, pr in enumerate(prs):
            if self.name not in pr.head.ref:
                # this pr is not for the current branch
                continue
            if pr.head.ref.startswith(f"cherrypick/{self.name}"):
                to_pop.append(i)
                if not any(label.name == Labels.PR_CHERRYPICK for label in pr.labels):
                    logging.warning(
                        "The cherry-pick PR #%s is found but doesn't have %s label. The "
                        "GitHub search index is stuck",
                        pr.number,
                        Labels.PR_CHERRYPICK,
                    )
                    continue
                self.cherrypick_pr = pr
            elif pr.head.ref.startswith(f"backport/{self.name}"):
                to_pop.append(i)
                if not any(label.name == Labels.PR_BACKPORT for label in pr.labels):
                    logging.warning(
                        "The backport PR #%s is found but doesn't have %s label. The "
                        "GitHub search index is stuck",
                        pr.number,
                        Labels.PR_BACKPORT,
                    )
                    continue
                self.backport_pr = pr
                self._backported = True
            else:
                assert False, f"BUG! Invalid PR's branch [{pr.head.ref}]"

        for i in reversed(to_pop):
            # Going from the tail to keep the order and pop greater index first
            prs.pop(i)
        return prs

    def process(  # pylint: disable=too-many-return-statements
        self, dry_run: bool, retried: bool = False
    ) -> None:
        if self.backported:
            return

        if not self.cherrypick_pr:
            if dry_run:
                logging.info(
                    "DRY RUN: Would create cherry-pick or backport PR for #%s",
                    self.pr.number,
                )
                return
            self.create_cherrypick()

        if self.backported:
            # The `backported` can be set to True if the changes are already applied
            return
        assert self.cherrypick_pr, "Unable to create cherry-pick PR"

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
        if not retried and self._retry_cherrypick(dry_run):
            # The branches were rebuilt against the current release branch and
            # the merge is clean now, so re-enter to take the ordinary merge and
            # `create_backport` path above. `retried` bounds this to one extra
            # pass, for the case where GitHub has not recomputed `mergeable` yet.
            return self.process(dry_run, retried=True)
        if self.backported:
            # The retry found the release branch already carries the changes
            return
        # Assign to engineer if not already assigned (only for PRs with conflicts)
        if not self.cherrypick_pr.assignees:
            if dry_run:
                logging.info(
                    "DRY RUN: Would assign cherry-pick PR #%s to engineers",
                    self.cherrypick_pr.number,
                )
            else:
                self._assign_new_pr(self.cherrypick_pr)
                self.cherrypick_pr.update()
        self.ping_cherry_pick_assignees(dry_run)

    def _prepare_backport_branch(self, base: str = "") -> str:
        """
        Reset `backport_branch` to `base` (the release branch by default) plus an
        empty merge of the original PR's first parent.

        The `-s ours` merge applies nothing; it only makes that first parent an
        ancestor, so that merging the PR's merge commit into this branch reduces
        to a cherry-pick of the PR's own diff.
        """
        # Pin the release used for both the resolved tree and its final parent.
        # A retry can refresh the remote ref while the local release stays stale.
        release_head = git_runner(
            f"git rev-parse {base or f'{self.REMOTE}/{self.name}'}"
        )
        git_runner(f"{GIT_PREFIX} checkout -f {release_head}")
        # Create or reset backport branch
        git_runner(f"{GIT_PREFIX} checkout -B {self.backport_branch}")
        # Merge all changes from PR's the first parent commit w/o applying anything
        # It will allow to create a merge commit like it would be a cherry-pick
        first_parent = git_runner(f"git rev-parse {self.pr.merge_commit_sha}^1")
        git_runner(f"{GIT_PREFIX} merge -s ours --no-edit {first_parent}")
        return release_head

    def _try_merge_backport_into_cherrypick(self) -> bool:
        """
        Reset `cherrypick_branch` to the PR's merge commit, merge
        `backport_branch` into it and report whether it applied cleanly.

        The rename limit is raised so git does not silently disable rename
        detection on large diffs (files renamed between the release branch and
        master would otherwise show up as spurious conflicts).

        Clean: `cherrypick_branch` now holds the fully resolved tree. Conflict:
        `merge --abort` restores `cherrypick_branch` to the PR's merge commit.
        """
        git_runner(
            f"{GIT_PREFIX} checkout --no-track -B "
            f"{self.cherrypick_branch} {self.pr.merge_commit_sha}"
        )
        try:
            git_runner(
                f"{GIT_PREFIX} -c merge.renameLimit=999999 "
                f"merge --no-ff --no-edit {self.backport_branch}"
            )
            return True
        except CalledProcessError:
            # Read the unmerged paths before aborting: they name what a human
            # would have to resolve, and they are the input for spotting a
            # missing prerequisite backport
            logging.info(
                "Cherry-pick of #%s to %s conflicts on: %s",
                self.pr.number,
                self.name,
                ", ".join(git_runner("git diff --name-only --diff-filter=U").split())
                or "unknown paths",
            )
            try:
                git_runner(f"{GIT_PREFIX} merge --abort")
            except CalledProcessError:
                # `merge --abort` itself can fail when the merge process
                # crashed (e.g. merge-ort assertion) and left
                # `.git/index.lock` behind -- the lock blocks any further
                # git command in this checkout. Clean it up so subsequent
                # PRs in the same run are not poisoned.
                recover_git_state()
            return False

    def _cherrypick_is_empty(self) -> bool:
        """
        Whether the resolved cherry-pick changes nothing versus the backport
        branch, meaning the release branch already carries these changes: either
        "Already up to date" (no merge commit at all) or an empty merge commit
        whose resolution collapsed onto the backport branch's tree (e.g. the PR
        was applied by hand with equivalent content).
        """
        return not git_runner(
            f"{GIT_PREFIX} diff --name-only "
            f"{self.backport_branch} {self.cherrypick_branch}"
        )

    def create_cherrypick(self):
        release_head = self._prepare_backport_branch()

        if self._try_merge_backport_into_cherrypick():
            # Nothing to open a PR for
            if self._cherrypick_is_empty():
                logging.info(
                    "Release branch %s already contain changes from %s",
                    self.name,
                    self.pr.number,
                )
                self._backported = True
                return
            # A clean cherry-pick needs no manual conflict resolution, so the
            # intermediate cherry-pick PR carries no value. Create the backport
            # PR directly from the already resolved tree and skip it entirely.
            self.create_backport_from_resolved_tree(release_head)
            return

        # There are conflicts: cherrypick_branch stays at pr.merge_commit_sha
        # (merge --abort restored HEAD). Push both branches and open the
        # cherry-pick PR so the conflicts are surfaced on GitHub for manual
        # resolution by the assigned engineer.
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
        # Do not assign yet - will assign only if there are conflicts
        # update cherrypick PR to get the state for PR.mergable
        self.cherrypick_pr.update()

    def _retry_cherrypick(self, dry_run: bool) -> bool:
        """
        Re-try a conflicting cherry-pick against the current release branch, and
        report whether it now applies cleanly.

        Both branches of a cherry-pick PR are frozen at the moment the conflict
        was found: the base is the release branch head from back then. A conflict
        is very often caused by a prerequisite backport that had not landed yet,
        and when it does land the base still lacks it, so GitHub keeps reporting
        `CONFLICTING` indefinitely. Recovering meant a human closing the PR,
        deleting its branch and unlabelling the original just to make the bot
        start over.

        Rebuild both branches against the current release branch head instead. If
        the merge is clean now, force-push them so the PR becomes mergeable and
        the caller's existing merge and `create_backport` path takes over.
        """
        assert self.cherrypick_pr is not None
        remote_release = f"{self.REMOTE}/{self.name}"
        remote_backport = f"{self.REMOTE}/{self.backport_branch}"
        remote_cherrypick = f"{self.REMOTE}/{self.cherrypick_branch}"
        # Forced refspecs: the bot force-pushes both of these branches, so a
        # non-forced fetch can leave stale remote-tracking refs behind
        git_runner(
            f"{GIT_PREFIX} fetch --no-tags --no-recurse-submodules {self.REMOTE} "
            + " ".join(
                f"+refs/heads/{branch}:refs/remotes/{self.REMOTE}/{branch}"
                for branch in (self.name, self.backport_branch, self.cherrypick_branch)
            )
        )

        # The retry force-pushes the cherry-pick branch, so it must never
        # overwrite a partial resolution somebody is working on. An untouched
        # head is the only condition strictly required for that.
        head = git_runner(f"git rev-parse {remote_cherrypick}")
        if head != self.pr.merge_commit_sha:
            logging.info(
                "Retry of cherry-pick PR #%s skipped: its head is %s, not the "
                "original merge commit %s, so it was resolved by hand",
                self.cherrypick_pr.number,
                head,
                self.pr.merge_commit_sha,
            )
            return False

        # Belt and braces: the base must still be the merge commit this script
        # generates, i.e. a release branch commit merged with the original PR's
        # first parent. Anything else was hand-edited.
        first_parent = git_runner(f"git rev-parse {self.pr.merge_commit_sha}^1")
        base = git_runner(f"git rev-parse {remote_backport}")
        base_parents = git_runner(f"git rev-parse {base}^@").split()
        if len(base_parents) != 2 or base_parents[1] != first_parent:
            logging.info(
                "Retry of cherry-pick PR #%s skipped: its base %s is not a "
                "generated merge of the release branch with %s",
                self.cherrypick_pr.number,
                remote_backport,
                first_parent,
            )
            return False
        if git_runner(f"git rev-parse {base}^{{tree}}") != git_runner(
            f"git rev-parse {base_parents[0]}^{{tree}}"
        ):
            logging.info(
                "Retry of cherry-pick PR #%s skipped: its base tree was edited",
                self.cherrypick_pr.number,
            )
            return False
        if not Shell.check(
            f"git merge-base --is-ancestor {base_parents[0]} {remote_release}",
            verbose=True,
        ):
            logging.info(
                "Retry of cherry-pick PR #%s skipped: its base is not built on %s",
                self.cherrypick_pr.number,
                self.name,
            )
            return False

        # Nothing new to merge against, so the outcome cannot have changed. This
        # is the common case and it costs no merge attempt and no API call.
        release_head = git_runner(f"git rev-parse {remote_release}")
        if release_head == base_parents[0]:
            logging.info(
                "Retry of cherry-pick PR #%s skipped: %s is unchanged at %s since "
                "the conflict was found",
                self.cherrypick_pr.number,
                self.name,
                release_head,
            )
            return False

        # Rebuild the empty merge and resolve entirely in the object database.
        # Literal path intersections miss renames between master and the release;
        # `merge-tree` uses the real merge machinery without rewriting the checkout.
        release_tree = git_runner(f"git rev-parse {release_head}^{{tree}}")
        refreshed_base = git_runner(
            f"{GIT_PREFIX} commit-tree {release_tree} "
            f"-p {release_head} -p {first_parent} -F -",
            input=f"Refresh cherry-pick base for #{self.pr.number} on {self.name}",
        )
        try:
            result = git_runner(
                f"{GIT_PREFIX} -c merge.renameLimit=999999 merge-tree "
                f"--write-tree --name-only -z --no-messages {head} {refreshed_base}"
            )
        except CalledProcessError as e:
            # Exit status 1 means conflicts; other failures must abort the retry.
            if e.returncode != 1:
                raise
            logging.info(
                "Cherry-pick PR #%s still conflicts against %s at %s on: %s",
                self.cherrypick_pr.number,
                self.name,
                release_head,
                ", ".join(path for path in e.output.split("\0")[1:] if path),
            )
            return False
        resolved_tree = result.split("\0", maxsplit=1)[0]

        if resolved_tree == release_tree:
            # The prerequisite that landed was the change itself, applied by
            # some other route. Same conclusion as in `create_cherrypick`, and
            # the PR has nothing left to do.
            logging.info(
                "Release branch %s already contain changes from %s",
                self.name,
                self.pr.number,
            )
            self._backported = True
            if dry_run:
                logging.info(
                    "DRY RUN: would close cherry-pick PR #%s, %s already has the "
                    "changes",
                    self.cherrypick_pr.number,
                    self.name,
                )
                return False
            self.cherrypick_pr.create_issue_comment(
                f"`{self.name}` already contains these changes, closing."
            )
            self.cherrypick_pr.edit(state="closed")
            return False

        if dry_run:
            logging.info(
                "DRY RUN: would refresh the branches of cherry-pick PR #%s "
                "against %s and let the bot merge it",
                self.cherrypick_pr.number,
                release_head,
            )
            return False

        resolved_head = git_runner(
            f"{GIT_PREFIX} commit-tree {resolved_tree} "
            f"-p {head} -p {refreshed_base} -F -",
            input=f"Retry cherry-pick #{self.pr.number} against {self.name}",
        )
        # Protect the exact refs inspected above, including against a human push
        # during the merge. Publish both refs together or leave both untouched.
        git_runner(
            f"{GIT_PREFIX} push --atomic "
            f"--force-with-lease=refs/heads/{self.cherrypick_branch}:{head} "
            f"--force-with-lease=refs/heads/{self.backport_branch}:{base} "
            f"{self.REMOTE} "
            f"{resolved_head}:refs/heads/{self.cherrypick_branch} "
            f"{refreshed_base}:refs/heads/{self.backport_branch}"
        )
        self.cherrypick_pr.create_issue_comment(
            f"The `{self.name}` branch moved since this cherry-pick was created "
            f"(now at {release_head[:12]}). Re-tried the merge against it and it "
            "applies cleanly, so the bot is proceeding without manual resolution."
        )
        logging.info(
            "Cherry-pick PR #%s merges cleanly now, branches pushed",
            self.cherrypick_pr.number,
        )

        # GitHub recomputes `mergeable` asynchronously and reports None while it
        # does. Give it a few chances so this run can finish the job; if it is
        # still None, the next run merges the PR instead.
        for attempt in range(1, self.MERGEABLE_POLL_ATTEMPTS + 1):
            self.cherrypick_pr.update()
            if self.cherrypick_pr.mergeable is not None:
                break
            logging.info(
                "GitHub has not recomputed `mergeable` for #%s yet (attempt %s/%s)",
                self.cherrypick_pr.number,
                attempt,
                self.MERGEABLE_POLL_ATTEMPTS,
            )
            time.sleep(self.MERGEABLE_POLL_SECONDS)
        return True

    def create_backport(self):
        assert self.cherrypick_pr is not None
        # Checkout the backport branch from the remote and make all changes to
        # apply like they are only one cherry-pick commit on top of release
        logging.info("Creating backport for PR #%s", self.pr.number)
        git_runner(
            f"{GIT_PREFIX} fetch --no-tags --no-recurse-submodules {self.REMOTE} "
            f"+refs/heads/{self.backport_branch}:"
            f"refs/remotes/{self.REMOTE}/{self.backport_branch}"
        )
        backport_head = git_runner(
            f"git rev-parse {self.REMOTE}/{self.backport_branch}"
        )
        # A retry may have replaced the remote base in this same iteration.
        git_runner(
            f"{GIT_PREFIX} checkout -f -B {self.backport_branch} {backport_head}"
        )
        merge_base = git_runner(
            f"{GIT_PREFIX} merge-base "
            f"{self.REMOTE}/{self.name} {self.backport_branch}"
        )
        git_runner(f"{GIT_PREFIX} reset --soft {merge_base}")
        title = f"Backport #{self.pr.number} to {self.name}: {self.pr.title}"
        git_runner(f"{GIT_PREFIX} commit --allow-empty -F -", input=title)

        # Do not overwrite a resolution pushed after the fetch above.
        git_runner(
            f"{GIT_PREFIX} push "
            f"--force-with-lease=refs/heads/{self.backport_branch}:{backport_head} "
            f"{self.REMOTE} {self.backport_branch}:refs/heads/{self.backport_branch}"
        )
        self._finalize_backport_pr(title)

    def create_backport_from_resolved_tree(self, release_head: str):
        # Fast path for a conflict-free cherry-pick: the merge done in
        # create_cherrypick already produced the fully resolved tree in
        # cherrypick_branch. Materialize it as a single commit on top of the
        # release branch tip - exactly what create_backport produces after a
        # cherry-pick PR is merged - and open the backport PR directly, without
        # the intermediate cherry-pick PR and the extra run it needs (the
        # cherry-pick PR is created in one run and only merged in a later one,
        # once GitHub has computed its mergeable state).
        logging.info(
            "Creating backport directly for PR #%s (no conflicts)", self.pr.number
        )
        resolved_tree = git_runner(f"git rev-parse {self.cherrypick_branch}^{{tree}}")
        title = f"Backport #{self.pr.number} to {self.name}: {self.pr.title}"
        commit = git_runner(
            f"{GIT_PREFIX} commit-tree {resolved_tree} " f"-p {release_head} -F -",
            input=title,
        )
        git_runner(f"{GIT_PREFIX} branch -f {self.backport_branch} {commit}")
        git_runner(
            f"{GIT_PREFIX} push -f {self.REMOTE} "
            f"{self.backport_branch}:{self.backport_branch}"
        )
        self._finalize_backport_pr(title)
        # A backport PR now exists, so the original PR is fully processed.
        self._backported = True

    def _finalize_backport_pr(self, title: str) -> None:
        try:
            self.backport_pr = self.repo.create_pull(
                title=title,
                body=self.body_header() + self.BACKPORT_DESCRIPTION + self.pr_source,
                base=self.name,
                head=self.backport_branch,
            )
        except GithubException as e:
            if e.status != 422 or "already exists" not in str(e):
                raise
            # The backport PR was created in a previous run but left without the
            # `pr-backport` label (e.g. the run was interrupted after `create_pull`
            # but before `add_to_labels`). Find and reuse it.
            existing = list(
                self.repo.get_pulls(
                    head=f"{self.repo.owner.login}:{self.backport_branch}",
                    base=self.name,
                    state="open",
                )
            )
            if not existing:
                raise
            self.backport_pr = existing[0]
            logging.warning(
                "Backport PR #%s for PR #%s already exists without label, reusing it",
                self.backport_pr.number,
                self.pr.number,
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
            "Checking if cherry-pick PR #%s needs to be pinged or closed",
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

        PING_THRESHOLD = 3 * 24 * 3600  # 3 days
        CLOSE_THRESHOLD = 7 * 24 * 3600  # 7 days

        if since_updated < PING_THRESHOLD:
            logging.info(
                "The cherry-pick PR was updated %s ago, waiting for the next run",
                since_updated_str,
            )
            return

        if since_updated >= CLOSE_THRESHOLD:
            # Close the PR after 7 days
            if self.cherrypick_pr.assignees:
                assignees = ", ".join(
                    f"@{user.login}" for user in self.cherrypick_pr.assignees
                )
                comment_body = (
                    f"Dear {assignees}, this cherry-pick PR has not been updated for {since_updated_str}. "
                    f"Closing automatically. If you still want to backport #{self.pr.number}, "
                    "please resolve the conflicts and reopen this PR."
                )
            else:
                logging.warning(
                    "Cherry-pick PR #%s has no assignees when closing",
                    self.cherrypick_pr.number,
                )
                comment_body = (
                    f"This cherry-pick PR has not been updated for {since_updated_str}. "
                    f"Closing automatically. If you still want to backport #{self.pr.number}, "
                    "please resolve the conflicts and reopen this PR."
                )
            if dry_run:
                logging.info(
                    "DRY RUN: would close cherry-pick PR #%s with comment:\n%s",
                    self.cherrypick_pr.number,
                    comment_body,
                )
                return
            self.cherrypick_pr.create_issue_comment(comment_body)
            logging.info(
                "Posted closing comment to cherry-pick PR #%s",
                self.cherrypick_pr.number,
            )
            self.cherrypick_pr.edit(state="closed")
            logging.info(
                "Closed cherry-pick PR #%s after %s of inactivity",
                self.cherrypick_pr.number,
                since_updated_str,
            )
            return

        # Ping after 3 days
        # Check if we've already pinged to avoid spamming
        comments = self.cherrypick_pr.get_issue_comments()
        for comment in comments:
            if (
                "has not been updated for" in comment.body
                and "resolve the conflicts" in comment.body
            ):
                # We've already pinged, don't ping again
                logging.info(
                    "Already pinged cherry-pick PR #%s, waiting for update or closure threshold",
                    self.cherrypick_pr.number,
                )
                return

        if self.cherrypick_pr.assignees:
            assignees = ", ".join(
                f"@{user.login}" for user in self.cherrypick_pr.assignees
            )
            comment_body = (
                f"Dear {assignees}, this cherry-pick PR has not been updated for {since_updated_str}. "
                f"Please resolve the conflicts to backport #{self.pr.number}, "
                "or close this PR if the backport is no longer needed. "
                f"This PR will be automatically closed after {CLOSE_THRESHOLD // 86400} days of inactivity."
            )
        else:
            logging.warning(
                "Cherry-pick PR #%s has no assignees when pinging",
                self.cherrypick_pr.number,
            )
            comment_body = (
                f"This cherry-pick PR has not been updated for {since_updated_str}. "
                f"Please resolve the conflicts to backport #{self.pr.number}, "
                "or close this PR if the backport is no longer needed. "
                f"This PR will be automatically closed after {CLOSE_THRESHOLD // 86400} days of inactivity."
            )
        if dry_run:
            logging.info(
                "DRY RUN: would comment on cherry-pick PR #%s:\n%s",
                self.cherrypick_pr.number,
                comment_body,
            )
            return

        self.cherrypick_pr.create_issue_comment(comment_body)
        logging.info(
            "Posted ping comment to cherry-pick PR #%s after %s of inactivity",
            self.cherrypick_pr.number,
            since_updated_str,
        )

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

        self._remote = ""
        self._remote_line = ""

        self._repo = None  # type: Optional[Repository]
        self.release_prs = []  # type: PullRequests
        self.release_branches: List[str] = []
        self.labels_to_backport: List[str] = []
        self.prs_for_backport = []  # type: PullRequests
        # Original PRs that `reconcile_backport_branches` found stranded. They are
        # unreachable through `receive_prs_for_backport`, so they are processed in
        # addition to its search result.
        self.prs_to_reprocess = []  # type: PullRequests
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
            remote = self.remote_line.split(maxsplit=1)[0]
            self.fetch_branches(remote)
            self._remote = remote
            ReleaseBranch.REMOTE = remote
        return self._remote

    def fetch_branches(self, remote: str) -> None:
        """Refresh complete history for the active backport work only."""
        if not self.release_branches:
            raise BackportException("Discover active releases before fetching branches")
        branches = [self.default_branch]
        for release in self.release_branches:
            branches.extend(
                (release, f"backport/{release}/*", f"cherrypick/{release}/*")
            )
        refspecs = [
            f"+refs/heads/{branch}:refs/remotes/{remote}/{branch}"
            for branch in branches
        ]
        # Explicit refspecs avoid fetching unrelated heads through the remote's
        # usual wildcard configuration. Wildcards keep recovery independent of
        # GitHub's PR search and also discover branches created between passes.
        # Prune deleted automation refs before reconciliation inspects them.
        unshallow = "--unshallow " if is_shallow() else ""
        logging.info("Fetching backport branches: %s", ", ".join(branches))
        git_runner(
            f"{GIT_PREFIX} fetch --prune --no-tags --no-recurse-submodules "
            f"{unshallow}{shlex.quote(remote)} "
            + " ".join(shlex.quote(refspec) for refspec in refspecs)
        )
        if is_shallow():
            raise BackportException("Backport automation requires complete Git history")

    @property
    def is_remote_ssh(self) -> bool:
        return "github.com:" in self.remote_line

    def receive_release_prs(self):
        logging.info("Getting release PRs")
        self.release_prs = self.gh.get_release_pulls(self._repo_name)
        self.release_branches = [pr.head.ref for pr in self.release_prs]

        # A version-specific label `vX.Y-must-backport` is backported to X.Y and
        # to every newer active release (see `select_backport_branches`). The
        # named release need not be active itself, so the search must also pick
        # up PRs labelled for an end-of-life release as long as a newer release
        # is still active. Include every version-specific label that exists in
        # the repo whose version is not newer than the newest active release --
        # a newer label could not expand to any active branch.
        newest_active = max(branch_version(branch) for branch in self.release_branches)
        self.labels_to_backport = sorted(
            label.name
            for label in self.repo.get_labels()
            if label_version(label.name) is not None
            and label_version(label.name) <= newest_active
        )

        logging.info("Active releases: %s", ", ".join(self.release_branches))
        logging.info("Labels to backport: %s", ", ".join(self.labels_to_backport))

    def update_local_release_branches(self):
        logging.info("Update local release branches")
        # Fetch even when the bootstrap checkout has no local release branches.
        remote = self.remote
        branches = git_runner("git branch").split()
        for branch in self.release_branches:
            if branch not in branches:
                # the local branch is not exist, so continue
                continue
            local_ref = git_runner(f"git rev-parse {branch}")
            remote_ref = git_runner(f"git rev-parse {remote}/{branch}")
            if local_ref == remote_ref:
                # Do not need to update, continue
                continue
            logging.info("Resetting %s to %s/%s", branch, remote, branch)
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
        labels_to_backport = labels_to_backport or (
            self.labels_to_backport + [Labels.MUST_BACKPORT, Labels.MUST_BACKPORT_FORCE]
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

    def backport_branch_carries_changes(self, release: str, branch: str) -> bool:
        """
        Whether a backport branch has anything to open a backport PR for.

        A freshly created backport branch is the release tip plus a
        `merge -s ours` of the original PR's first parent, so it holds the
        release branch's tree unchanged until a cherry-pick is merged into it.
        The comparison is against the merge base -- the same commit
        `create_backport` squashes onto -- and not against the release tip, so
        the answer does not flip as the release branch moves on.
        """
        merge_base = git_runner(
            f"git merge-base {self.remote}/{release} {self.remote}/{branch}"
        )
        return bool(
            git_runner(f"git diff --name-only {merge_base} {self.remote}/{branch}")
        )

    def reconcile_backport_branches(self) -> None:
        """
        Recover backports whose work is finished but which have no backport PR.

        A backport PR is only ever created while processing the ORIGINAL PR, and
        the original is dropped from `receive_prs_for_backport` for good once it
        carries `pr-backports-created`. A cherry-pick PR merged after that label
        was set therefore leaves its resolution stranded on the `backport/*`
        branch with nothing pointing at it. `check_open_prs` is the only existing
        recovery path and it looks exclusively at cherry-pick PRs that are open at
        the moment a run starts, so a reopen and a merge that both fall between
        two runs are lost with no trace.

        Every backport reaches a release branch through a
        `backport/<release>/<PR>` branch, which makes the set of those branches a
        complete work list. Anything that carries changes and has no backport PR
        is unstuck here, independently of when, how fast or by whom the
        cherry-pick was merged.
        """
        # branch -> (release branch, original PR number)
        branches: Dict[str, Tuple[str, int]] = {}
        for release in self.release_branches:
            refs = git_runner(
                "git for-each-ref --format='%(refname:short)' "
                f"'refs/remotes/{self.remote}/backport/{release}/*'"
            ).split("\n")
            for ref in refs:
                branch = removeprefix(ref, f"{self.remote}/")
                number = branch.rsplit("/", maxsplit=1)[-1]
                if not number.isdigit():
                    # Not an automation branch, e.g. `backport/release/26.2/wip`
                    continue
                branches[branch] = (release, int(number))

        if not branches:
            return

        # One GraphQL request per 50 branches, instead of a REST request each
        backport_prs = self.gh.get_pull_states_by_head_refs(
            self._repo_name,
            {branch: branches[branch][0] for branch in sorted(branches)},
        )
        stranded = [branch for branch in sorted(branches) if not backport_prs[branch]]
        logging.info(
            "Reconciliation: %s backport branches for active releases, "
            "%s without a backport PR",
            len(branches),
            len(stranded),
        )
        if not stranded:
            return

        # A stranded branch is only actionable when the cherry-pick work on it is
        # done: an open cherry-pick PR is still waiting for a human, and a closed
        # one is a decision not to backport. A branch with no cherry-pick PR at
        # all was pushed by the conflict-free path, which means `create_pull` did
        # not run or did not survive.
        cherrypick_prs = self.gh.get_pull_states_by_head_refs(
            self._repo_name,
            {ReleaseBranch.cp_branch(*branches[branch]): branch for branch in stranded},
        )
        # original PR number -> release branches it is stranded on
        to_recover: Dict[int, List[str]] = {}
        for branch in stranded:
            release, number = branches[branch]
            states = [
                state
                for _, state in cherrypick_prs[ReleaseBranch.cp_branch(release, number)]
            ]
            if states and "MERGED" not in states:
                continue
            try:
                carries_changes = self.backport_branch_carries_changes(release, branch)
            except CalledProcessError as e:
                # This is a safety net for the normal path that runs after it, so
                # one unreadable branch must not stop the rest of the run
                logging.error("Cannot inspect the branch %s: %s", branch, e)
                self.error = e
                continue
            if not carries_changes:
                # Nothing was applied to the branch yet, so there is no backport
                # to open a PR for. Creating one would produce an empty PR.
                continue
            logging.warning(
                "Backport branch %s carries changes but has no backport PR", branch
            )
            to_recover.setdefault(number, []).append(release)

        if not to_recover:
            return

        general_backport_labels = {
            Labels.MUST_BACKPORT,
            Labels.MUST_BACKPORT_FORCE,
        } | Labels.AUTO_BACKPORT
        infos = self.gh.get_pulls_lightweight_by_numbers(
            self._repo_name, sorted(to_recover)
        )
        for number, releases in sorted(to_recover.items()):
            try:
                self._recover_stranded_pr(
                    number, releases, infos.get(number), general_backport_labels
                )
            except Exception as e:
                logging.error("Cannot recover the PR #%s: %s", number, e)
                self.error = e

    def _recover_stranded_pr(
        self,
        number: int,
        releases: List[str],
        info: Optional[PullRequestInfo],
        general_backport_labels: Iterable[str],
    ) -> None:
        """Put one stranded original PR back into the backport process."""
        if info is None:
            logging.error("Cannot fetch the original PR #%s, skipping", number)
            return
        if not info.merged:
            # `create_cherrypick` needs the merge commit. A backport branch for an
            # unmerged PR was not created by this automation.
            logging.info("PR #%s is not merged, skipping", number)
            return

        # The labels may have been removed deliberately since the branch was
        # created. Recreating a backport nobody asked for would be worse than
        # leaving the branch alone, so re-check the targets. Unlike in
        # `process_pr`, the labels are not guaranteed by the search here, so the
        # no-label case has to be excluded before `select_backport_branches`
        # asserts on it.
        labels = set(info.label_names)
        if not labels & set(general_backport_labels) and not any(
            label_version(label) is not None for label in labels
        ):
            logging.info(
                "PR #%s carries no backport label anymore, leaving the branch(es) alone",
                number,
            )
            return
        targets = select_backport_branches(
            info.label_names,
            self.release_branches,
            general_backport_labels=set(general_backport_labels),
            force_backport_label=Labels.MUST_BACKPORT_FORCE,
        )
        stale = [release for release in releases if release not in targets]
        if stale:
            logging.info(
                "PR #%s no longer targets %s, leaving the branch(es) alone",
                number,
                ", ".join(stale),
            )
        wanted = [release for release in releases if release in targets]
        if not wanted:
            return

        logging.info(
            "PR #%s is stranded on %s, putting it back into the process",
            number,
            ", ".join(wanted),
        )
        if self.dry_run:
            logging.info("DRY RUN: would reprocess PR #%s", number)
            return

        # Not `get_pull_cached`: the label set must be current, and it is about to
        # be edited
        pr = self.repo.get_pull(number)
        if Labels.PR_BACKPORTS_CREATED in [label.name for label in pr.labels]:
            # Dropping the label also makes the normal search pick the PR up on
            # the next run, so the recovery is retried if this run dies before the
            # backport PR is created
            pr.remove_from_labels(Labels.PR_BACKPORTS_CREATED)
            logging.info(
                "Removed label %s from PR #%s", Labels.PR_BACKPORTS_CREATED, number
            )
        self.prs_to_reprocess.append(pr)

    def process_backports(self):
        prs = list(self.prs_for_backport)
        numbers = {pr.number for pr in prs}
        for pr in self.prs_to_reprocess:
            # The search may already return a stranded PR, e.g. when its label was
            # dropped by an earlier run
            if pr.number not in numbers:
                prs.append(pr)
                numbers.add(pr.number)

        for pr in prs:
            try:
                self.process_pr(pr)
            except Exception as e:
                logging.error(
                    "During processing the PR #%s error occurred: %s", pr.number, e
                )
                self.error = e
                # Whatever went wrong, make sure the next PR starts from a
                # clean working tree -- a leftover `.git/index.lock` from a
                # crashed git process would otherwise break every later PR.
                recover_git_state()

    def process_pr(self, pr: PullRequest) -> None:
        pr_labels = [label.name for label in pr.labels]

        # Decide the target release branches (pure logic in
        # `cherry_pick_branches.py`). A version-specific label
        # (`vX.Y-must-backport`) marks the OLDEST release the PR must reach, so
        # the PR is backported to that release and every newer active release
        # branch; the lowest such label wins.
        branch_names = select_backport_branches(
            pr_labels,
            self.release_branches,
            general_backport_labels={Labels.MUST_BACKPORT, Labels.MUST_BACKPORT_FORCE}
            | Labels.AUTO_BACKPORT,
            force_backport_label=Labels.MUST_BACKPORT_FORCE,
        )

        if not branch_names:
            logging.info(
                "PR #%s: no candidate release branches found, skipping backport",
                pr.number,
            )
            return

        branches: List[ReleaseBranch] = [
            ReleaseBranch(br, pr, self.repo) for br in branch_names
        ]

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

        # Backport and cherry-pick PRs
        bp_cp_prs = self.gh.get_pulls_from_search(
            query=f"type:pr repo:{self._repo_name} {query_suffix}",
            label=f"{Labels.PR_BACKPORT},{Labels.PR_CHERRYPICK}",
        )
        # Check that all
        for br in branches:
            bp_cp_prs = br.pop_prs(bp_cp_prs)
        assert not bp_cp_prs, f"Some PRs are not processed by backporting: {bp_cp_prs}"

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

        if pr.assignees:
            assignees = ", ".join(f"@{user.login}" for user in pr.assignees)
            comment_body = (
                f"Dear {assignees}, this PR is opened while #{original_pr.number} was "
                f"marked as backported. The `{Labels.PR_BACKPORTS_CREATED}` is removed, so "
                "the original PR can be processed again.\n\n"
                "If the cherry-pick is not needed anymore, then just close this PR."
            )
        else:
            logging.warning(
                "Cherry-pick PR #%s has no assignees when removing backported label",
                pr.number,
            )
            comment_body = (
                f"This PR is opened while #{original_pr.number} was "
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

        try:
            original_pr.remove_from_labels(Labels.PR_BACKPORTS_CREATED)
        except GithubException as e:
            if e.status == 404:
                logging.info(
                    "Label %s is already removed from PR #%s",
                    Labels.PR_BACKPORTS_CREATED,
                    original_pr.number,
                )
            else:
                raise
        pr.create_issue_comment(comment_body)
        logging.info(
            "Removed label %s from PR #%s and posted comment to cherry-pick PR #%s",
            Labels.PR_BACKPORTS_CREATED,
            original_pr.number,
            pr.number,
        )


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
    parser.add_argument(
        "--loop", action="store_true", help="repeat within one checkout"
    )
    parser.add_argument(
        "--loop-duration",
        type=int,
        default=170 * 60,
        help="stop starting iterations after this many seconds",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=10 * 60,
        help="seconds between iteration starts",
    )
    parser.add_argument(
        "--max-consecutive-failures",
        type=int,
        default=3,
        help="stop the loop after this many consecutive failures",
    )
    args = parser.parse_args()
    for option in ("loop_duration", "interval", "max_consecutive_failures"):
        if getattr(args, option) <= 0:
            parser.error(f"--{option.replace('_', '-')} must be positive")
    return args


def run_once(args):
    temp_path = Path(TEMP_PATH)
    temp_path.mkdir(parents=True, exist_ok=True)

    token = args.token or get_best_robot_token(refresh=True)

    gh = GitHub(token)
    temp_path = Path(TEMP_PATH)
    gh_cache = GitHubCache(gh.cache_path, temp_path, S3Helper())
    gh_cache.download()

    bpp = BackportPRs(gh, args.repo, args.dry_run)

    bpp.gh.cache_path = temp_path / "gh_cache"
    bpp.receive_release_prs()
    bpp.update_local_release_branches()
    # Finish the targeted fetch before changing any PR labels or state. Then
    # requeue reopened cherry-picks before searching for original PRs to process.
    cpp = CherryPickPRs(gh, args.repo, args.dry_run)
    cpp.check_open_prs()
    # Before the search, so a recovered PR is processed in this very run: it is
    # invisible to `receive_prs_for_backport` until GitHub reindexes the label
    bpp.reconcile_backport_branches()
    bpp.receive_prs_for_backport()
    bpp.process_backports()
    gh_cache.upload()

    errors = [e for e in (bpp.error, cpp.error) if e is not None]
    if any(errors):
        logging.error("Finished successfully, but %s errors occurred!", len(errors))
        raise BackportException(
            "Errors occurred during backport process: "
            + "; ".join(str(e) for e in errors)
        )


def check_git_version() -> None:
    version = git_runner("git --version").split()[2]
    if tuple(int(part) for part in version.split(".")[:2]) < (2, 38):
        raise BackportException(
            f"Git 2.38 or newer is required for cherry-pick retries; found {version}. "
            "Upgrade the runner image before running backport automation."
        )


def local_branches() -> set:
    return set(
        git_runner(
            "git for-each-ref --format='%(refname:short)' refs/heads"
        ).splitlines()
    )


def restore_checkout(initial_sha: str, initial_branches: set) -> None:
    # Keep the selected code for every iteration, including manual branch runs.
    git_runner(f"{GIT_PREFIX} checkout --detach -f {initial_sha}")
    for branch in sorted(local_branches() - initial_branches):
        if branch.startswith(("backport/", "cherrypick/")):
            logging.info("Deleting temporary local branch %s", branch)
            git_runner(f"git branch -D -- {shlex.quote(branch)}")


def run_loop(args) -> None:
    initial_sha = git_runner("git rev-parse HEAD")
    initial_branches = local_branches()
    deadline = time.monotonic() + args.loop_duration
    iterations = 0
    failures = 0
    consecutive_failures = 0
    last_error = None  # type: Optional[Exception]

    while True:
        started = time.monotonic()
        if args.loop and started >= deadline:
            break
        iterations += 1
        logging.info("Starting backport iteration %s at %s", iterations, initial_sha)
        try:
            # Recreate clients and refresh credentials and repository state each pass.
            run_once(args)
        except Exception as e:
            failures += 1
            consecutive_failures += 1
            last_error = e
            logging.exception("Backport iteration %s failed", iterations)
            recover_git_state()
            if IS_CI and not args.dry_run:
                CIBuddy().post_job_error(
                    f"The backport process finished with errors: {e}",
                    with_instance_info=True,
                    with_wf_link=True,
                    critical=True,
                )
        else:
            consecutive_failures = 0
        finally:
            # Failure to restore a usable checkout aborts the loop as well.
            restore_checkout(initial_sha, initial_branches)

        if not args.loop or consecutive_failures >= args.max_consecutive_failures:
            break
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        delay = min(max(0, args.interval - (time.monotonic() - started)), remaining)
        if delay > 0:
            time.sleep(delay)

    summary = f"Ran {iterations} iteration(s), {failures} failed"
    logging.info(summary)
    if IS_CI and os.getenv("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as output:
            output.write(summary + "\n")
    if failures:
        raise BackportException(summary) from last_error


def main():
    args = parse_args()
    if args.debug_helpers:
        logging.getLogger("github_helper").setLevel(logging.DEBUG)
        logging.getLogger("git_helper").setLevel(logging.DEBUG)
    # Check prerequisites before fetching credentials or making API mutations.
    check_git_version()
    with stash():
        if os.getenv("ROBOT_CLICKHOUSE_SSH_KEY", ""):
            with SSHKey("ROBOT_CLICKHOUSE_SSH_KEY"):
                run_loop(args)
        else:
            run_loop(args)


if __name__ == "__main__":
    logging.getLogger().setLevel(level=logging.INFO)
    main()
