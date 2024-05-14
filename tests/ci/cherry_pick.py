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
from datetime import date, datetime, timedelta
from pathlib import Path
from subprocess import CalledProcessError
from typing import List, Optional

import __main__
from env_helper import TEMP_PATH
from get_robot_token import get_best_robot_token
from git_helper import git_runner, is_shallow
from github_helper import GitHub, PullRequest, PullRequests, Repository
from lambda_shared_package.lambda_shared.pr import Labels
from ssh import SSHKey


class ReleaseBranch:
    CHERRYPICK_DESCRIPTION = """Original pull-request #{pr_number}

This pull-request is a first step of an automated backporting.
It contains changes similar to calling `git cherry-pick` locally.
If you intend to continue backporting the changes, then resolve all conflicts if any.
Otherwise, if you do not want to backport them, then just close this pull-request.

The check results does not matter at this step - you can safely ignore them.

### Note

This pull-request will be merged automatically. Please, **do not merge it manually** \
(but if you accidentally did, nothing bad will happen).

### Troubleshooting

#### If the PR was manually reopened after being closed

If this PR is stuck (i.e. not automatically merged after one day), check {pr_url} for \
`{backport_created_label}` *label* and delete it.

Manually merging will do nothing. The `{backport_created_label}` *label* prevents the \
original PR {pr_url} from being processed.

#### If the conflicts were resolved in a wrong way

If this cherry-pick PR is completely screwed by a wrong conflicts resolution, and you \
want to recreate it:

- delete the `{label_cherrypick}` label from the PR
- delete this branch from the repository

You also need to check the original PR {pr_url} for `{backport_created_label}`, and \
delete if it's presented there
"""
    BACKPORT_DESCRIPTION = """This pull-request is a last step of an automated \
backporting.
Treat it as a standard pull-request: look at the checks and resolve conflicts.
Merge it only if you intend to backport changes to the target branch, otherwise just \
close it.
"""
    REMOTE = ""

    def __init__(
        self,
        name: str,
        pr: PullRequest,
        repo: Repository,
        backport_created_label: str = Labels.PR_BACKPORTS_CREATED,
    ):
        self.name = name
        self.pr = pr
        self.repo = repo

        self.cherrypick_branch = f"cherrypick/{name}/{pr.merge_commit_sha}"
        self.backport_branch = f"backport/{name}/{pr.number}"
        self.cherrypick_pr = None  # type: Optional[PullRequest]
        self.backport_pr = None  # type: Optional[PullRequest]
        self._backported = False

        self.backport_created_label = backport_created_label

        self.git_prefix = (  # All commits to cherrypick are done as robot-clickhouse
            "git -c user.email=robot-clickhouse@users.noreply.github.com "
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

    def pop_prs(self, prs: PullRequests) -> None:
        """the method processes all prs and pops the ReleaseBranch related prs"""
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
                    "head ref of PR #%s isn't starting with known suffix",
                    pr.number,
                )
        for i in reversed(to_pop):
            # Going from the tail to keep the order and pop greater index first
            prs.pop(i)

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
        if self.backported:
            return
        if self.cherrypick_pr is not None:
            # Try to merge cherrypick instantly
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
            self.ping_cherry_pick_assignees(dry_run)

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

        # Check if there are actually any changes between branches. If no, then no
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

        self.cherrypick_pr = self.repo.create_pull(
            title=f"Cherry pick #{self.pr.number} to {self.name}: {self.pr.title}",
            body=self.CHERRYPICK_DESCRIPTION.format(
                pr_number=self.pr.number,
                pr_url=self.pr.html_url,
                backport_created_label=self.backport_created_label,
                label_cherrypick=Labels.PR_CHERRYPICK,
            ),
            base=self.backport_branch,
            head=self.cherrypick_branch,
        )
        self.cherrypick_pr.add_to_labels(Labels.PR_CHERRYPICK)
        self.cherrypick_pr.add_to_labels(Labels.DO_NOT_TEST)
        self._assign_new_pr(self.cherrypick_pr)
        # update cherrypick PR to get the state for PR.mergable
        self.cherrypick_pr.update()

    def create_backport(self):
        assert self.cherrypick_pr is not None
        # Checkout the backport branch from the remote and make all changes to
        # apply like they are only one cherry-pick commit on top of release
        logging.info("Creating backport for PR #%s", self.pr.number)
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
        git_runner(f"{self.git_prefix} commit --allow-empty -F -", input=title)

        # Push with force, create the backport PR, lable and assign it
        git_runner(
            f"{self.git_prefix} push -f {self.REMOTE} "
            f"{self.backport_branch}:{self.backport_branch}"
        )
        self.backport_pr = self.repo.create_pull(
            title=title,
            body=f"Original pull-request {self.pr.html_url}\n"
            f"Cherry-pick pull-request #{self.cherrypick_pr.number}\n\n"
            f"{self.BACKPORT_DESCRIPTION}",
            base=self.name,
            head=self.backport_branch,
        )
        self.backport_pr.add_to_labels(Labels.PR_BACKPORT)
        self._assign_new_pr(self.backport_pr)

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
            f"{since_updated // 86400}d{since_updated // 3600}"
            f"h{since_updated // 60 % 60}m{since_updated % 60}s"
        )
        if since_updated < 86400:
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
        logging.info(
            "Assing #%s to author and assignees of the original PR: %s",
            new_pr.number,
            ", ".join(user.login for user in assignees),
        )
        new_pr.add_to_assignees(*assignees)

    @property
    def backported(self) -> bool:
        return self._backported or self.backport_pr is not None

    def __repr__(self):
        return self.name


class Backport:
    def __init__(
        self,
        gh: GitHub,
        repo: str,
        fetch_from: Optional[str],
        dry_run: bool,
        must_create_backport_label: str,
        backport_created_label: str,
    ):
        self.gh = gh
        self._repo_name = repo
        self._fetch_from = fetch_from
        self.dry_run = dry_run

        self.must_create_backport_label = must_create_backport_label
        self.backport_created_label = backport_created_label

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
            f"v{branch}-must-backport" for branch in self.release_branches
        ]

        if self._fetch_from:
            logging.info("Fetching from %s", self._fetch_from)
            fetch_from_repo = self.gh.get_repo(self._fetch_from)
            git_runner(
                "git fetch "
                f"{fetch_from_repo.ssh_url if self.is_remote_ssh else fetch_from_repo.clone_url} "
                f"{fetch_from_repo.default_branch} --no-tags"
            )

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

    def receive_prs_for_backport(self, reserve_search_days: int) -> None:
        # The commits in the oldest open release branch
        oldest_branch_commits = git_runner(
            "git log --no-merges --format=%H --reverse "
            f"{self.remote}/{self.default_branch}..{self.remote}/{self.release_branches[0]}"
        )
        # The first commit is the one we are looking for
        since_commit = oldest_branch_commits.split("\n", 1)[0]
        since_date = date.fromisoformat(
            git_runner.run(f"git log -1 --format=format:%cs {since_commit}")
        ) - timedelta(days=reserve_search_days)
        # To not have a possible TZ issues
        tomorrow = date.today() + timedelta(days=1)
        logging.info("Receive PRs suppose to be backported")

        query_args = {
            "query": f"type:pr repo:{self._fetch_from} -label:{self.backport_created_label}",
            "label": ",".join(
                self.labels_to_backport + [self.must_create_backport_label]
            ),
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
                    "During processing the PR #%s error occured: %s", pr.number, e
                )
                self.error = e

    def process_pr(self, pr: PullRequest) -> None:
        pr_labels = [label.name for label in pr.labels]
        if self.must_create_backport_label in pr_labels:
            branches = [
                ReleaseBranch(br, pr, self.repo, self.backport_created_label)
                for br in self.release_branches
            ]  # type: List[ReleaseBranch]
        else:
            branches = [
                ReleaseBranch(br, pr, self.repo, self.backport_created_label)
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
            query=f"type:pr repo:{self._repo_name} {query_suffix}",
            label=f"{Labels.PR_BACKPORT},{Labels.PR_CHERRYPICK}",
        )
        for br in branches:
            br.pop_prs(bp_cp_prs)

        if bp_cp_prs:
            # This is definitely some error. All prs must be consumed by
            # branches with ReleaseBranch.pop_prs. It also makes the whole
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

    def mark_pr_backported(self, pr: PullRequest) -> None:
        if self.dry_run:
            logging.info("DRY RUN: would mark PR #%s as done", pr.number)
            return
        pr.add_to_labels(self.backport_created_label)
        logging.info(
            "PR #%s is successfully labeled with `%s`",
            pr.number,
            self.backport_created_label,
        )

    @property
    def repo(self) -> Repository:
        if self._repo is None:
            self._repo = self.gh.get_repo(self._repo_name)
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
    parser.add_argument(
        "--from-repo",
        default="ClickHouse/ClickHouse",
        help="if set, the commits will be taken from this repo, but PRs will be created in the main repo",
    )
    parser.add_argument("--dry-run", action="store_true", help="do not create anything")

    parser.add_argument(
        "--must-create-backport-label",
        default=Labels.MUST_BACKPORT,
        choices=(Labels.MUST_BACKPORT, Labels.MUST_BACKPORT_CLOUD),
        help="label to filter PRs to backport",
    )
    parser.add_argument(
        "--backport-created-label",
        default=Labels.PR_BACKPORTS_CREATED,
        choices=(Labels.PR_BACKPORTS_CREATED, Labels.PR_BACKPORTS_CREATED_CLOUD),
        help="label to mark PRs as backported",
    )
    parser.add_argument(
        "--reserve-search-days",
        default=0,
        type=int,
        help="safity reserve for the PRs search days, necessary for cloud",
    )

    parser.add_argument(
        "--debug-helpers",
        action="store_true",
        help="add debug logging for git_helper and github_helper",
    )
    return parser.parse_args()


@contextmanager
def clear_repo():
    def ref():
        return git_runner("git branch --show-current") or git_runner(
            "git rev-parse HEAD"
        )

    orig_ref = ref()
    try:
        yield
    finally:
        current_ref = ref()
        if orig_ref != current_ref:
            git_runner(f"git checkout -f {orig_ref}")


@contextmanager
def stash():
    # diff.ignoreSubmodules=all don't show changed submodules
    need_stash = bool(git_runner("git -c diff.ignoreSubmodules=all diff HEAD"))
    if need_stash:
        script = (
            __main__.__file__ if hasattr(__main__, "__file__") else "unknown script"
        )
        git_runner(f"git stash push --no-keep-index -m 'running {script}'")
    try:
        with clear_repo():
            yield
    finally:
        if need_stash:
            git_runner("git stash pop")


def main():
    temp_path = Path(TEMP_PATH)
    temp_path.mkdir(parents=True, exist_ok=True)

    args = parse_args()
    if args.debug_helpers:
        logging.getLogger("github_helper").setLevel(logging.DEBUG)
        logging.getLogger("git_helper").setLevel(logging.DEBUG)
    token = args.token or get_best_robot_token()

    gh = GitHub(token, create_cache_dir=False)
    bp = Backport(
        gh,
        args.repo,
        args.from_repo,
        args.dry_run,
        args.must_create_backport_label,
        args.backport_created_label,
    )
    # https://github.com/python/mypy/issues/3004
    bp.gh.cache_path = temp_path / "gh_cache"
    bp.receive_release_prs()
    bp.update_local_release_branches()
    bp.receive_prs_for_backport(args.reserve_search_days)
    bp.process_backports()
    if bp.error is not None:
        logging.error("Finished successfully, but errors occured!")
        raise bp.error


if __name__ == "__main__":
    logging.getLogger().setLevel(level=logging.INFO)

    assert not is_shallow()
    with stash():
        if os.getenv("ROBOT_CLICKHOUSE_SSH_KEY", ""):
            with SSHKey("ROBOT_CLICKHOUSE_SSH_KEY"):
                main()
        else:
            main()
