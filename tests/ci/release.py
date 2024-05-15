#!/usr/bin/env python3

"""
script to create releases for ClickHouse

The `gh` CLI preferred over the PyGithub to have an easy way to rollback bad
release in command line by simple execution giving rollback commands

On another hand, PyGithub is used for convenient getting commit's status from API

To run this script on a freshly installed Ubuntu 22.04 system, it is enough to do the following commands:

sudo apt install pip
pip install requests boto3 github PyGithub
sudo snap install gh
gh auth login
"""


import argparse
import json
import logging
import subprocess
from contextlib import contextmanager
from typing import Any, Final, Iterator, List, Optional, Tuple

from git_helper import Git, commit, release_branch
from lambda_shared_package.lambda_shared.pr import Labels
from report import SUCCESS
from version_helper import (
    FILE_WITH_VERSION_PATH,
    GENERATED_CONTRIBUTORS,
    ClickHouseVersion,
    VersionType,
    get_abs_path,
    get_version_from_repo,
    update_cmake_version,
    update_contributors,
)

RELEASE_READY_STATUS = "Ready for release"


class Repo:
    VALID = ("ssh", "https", "origin")

    def __init__(self, repo: str, protocol: str):
        self._repo = repo
        self._url = ""
        self.url = protocol

    @property
    def url(self) -> str:
        return self._url

    @url.setter
    def url(self, protocol: str) -> None:
        if protocol == "ssh":
            self._url = f"git@github.com:{self}.git"
        elif protocol == "https":
            self._url = f"https://github.com/{self}.git"
        elif protocol == "origin":
            self._url = protocol
        else:
            raise ValueError(f"protocol must be in {self.VALID}")

    def __str__(self):
        return self._repo


class Release:
    NEW = "new"  # type: Final
    PATCH = "patch"  # type: Final
    VALID_TYPE = (NEW, PATCH)  # type: Final[Tuple[str, str]]
    CMAKE_PATH = get_abs_path(FILE_WITH_VERSION_PATH)
    CONTRIBUTORS_PATH = get_abs_path(GENERATED_CONTRIBUTORS)

    def __init__(
        self,
        repo: Repo,
        release_commit: str,
        release_type: str,
        dry_run: bool,
        with_stderr: bool,
    ):
        self.repo = repo
        self._release_commit = ""
        self.release_commit = release_commit
        self.dry_run = dry_run
        self.with_stderr = with_stderr
        assert release_type in self.VALID_TYPE
        self.release_type = release_type
        self._git = Git()
        self._version = get_version_from_repo(git=self._git)
        self.release_version = self.version
        self._release_branch = ""
        self._rollback_stack = []  # type: List[str]

    def run(
        self, cmd: str, cwd: Optional[str] = None, dry_run: bool = False, **kwargs: Any
    ) -> str:
        cwd_text = ""
        if cwd:
            cwd_text = f" (CWD='{cwd}')"
        if dry_run:
            logging.info("Would run command%s:\n    %s", cwd_text, cmd)
            return ""
        if not self.with_stderr:
            kwargs["stderr"] = subprocess.DEVNULL

        logging.info("Running command%s:\n    %s", cwd_text, cmd)
        return self._git.run(cmd, cwd, **kwargs)

    def set_release_info(self):
        # Fetch release commit and tags in case they don't exist locally
        self.run(
            f"git fetch {self.repo.url} {self.release_commit} --no-recurse-submodules"
        )
        self.run(f"git fetch {self.repo.url} --tags --no-recurse-submodules")

        # Get the actual version for the commit before check
        with self._checkout(self.release_commit, True):
            self.release_branch = f"{self.version.major}.{self.version.minor}"
            self.release_version = get_version_from_repo(git=self._git)
            self.release_version.with_description(self.get_stable_release_type())

        self.read_version()

    def read_version(self):
        self._git.update()
        self.version = get_version_from_repo(git=self._git)

    def get_stable_release_type(self) -> str:
        if self.version.is_lts:
            return VersionType.LTS
        return VersionType.STABLE

    def check_commit_release_ready(self):
        per_page = 100
        page = 1
        while True:
            statuses = json.loads(
                self.run(
                    f"gh api 'repos/{self.repo}/commits/{self.release_commit}"
                    f"/statuses?per_page={per_page}&page={page}'"
                )
            )

            if not statuses:
                break

            for status in statuses:
                if status["context"] == RELEASE_READY_STATUS:
                    if not status["state"] == SUCCESS:
                        raise ValueError(
                            f"the status {RELEASE_READY_STATUS} is {status['state']}"
                            ", not success"
                        )

                    return

            page += 1

        raise KeyError(
            f"the status {RELEASE_READY_STATUS} "
            f"is not found for commit {self.release_commit}"
        )

    def check_prerequisites(self):
        """
        Check tooling installed in the system, `git` is checked by Git() init
        """
        try:
            self.run("gh auth status")
        except subprocess.SubprocessError:
            logging.error(
                "The github-cli either not installed or not setup, please follow "
                "the instructions on https://github.com/cli/cli#installation and "
                "https://cli.github.com/manual/"
            )
            raise

        self.check_commit_release_ready()

    def do(
        self, check_dirty: bool, check_run_from_master: bool, check_branch: bool
    ) -> None:
        self.check_prerequisites()

        if check_dirty:
            logging.info("Checking if repo is clean")
            try:
                self.run("git diff HEAD --exit-code")
            except subprocess.CalledProcessError:
                logging.fatal("Repo contains uncommitted changes")
                raise

        if check_run_from_master and self._git.branch != "master":
            raise RuntimeError("the script must be launched only from master")

        self.set_release_info()

        if check_branch:
            self.check_branch()

        if self.release_type == self.NEW:
            with self._checkout(self.release_commit, True):
                # Checkout to the commit, it will provide the correct current version
                with self.new_release():
                    with self.create_release_branch():
                        logging.info(
                            "Publishing release %s from commit %s is done",
                            self.release_version.describe,
                            self.release_commit,
                        )

        elif self.release_type == self.PATCH:
            with self._checkout(self.release_commit, True):
                with self.patch_release():
                    logging.info(
                        "Publishing release %s from commit %s is done",
                        self.release_version.describe,
                        self.release_commit,
                    )

        if self.dry_run:
            logging.info("Dry running, clean out possible changes")
            rollback = self._rollback_stack.copy()
            rollback.reverse()
            for cmd in rollback:
                self.run(cmd)
            return

        self.log_post_workflows()
        self.log_rollback()

    def check_no_tags_after(self):
        tags_after_commit = self.run(f"git tag --contains={self.release_commit}")
        if tags_after_commit:
            raise RuntimeError(
                f"Commit {self.release_commit} belongs to following tags:\n"
                f"{tags_after_commit}\nChoose another commit"
            )

    def check_branch(self):
        branch = self.release_branch
        if self.release_type == self.NEW:
            # Commit to spin up the release must belong to a main branch
            branch = "master"
        elif self.release_type != self.PATCH:
            raise (
                ValueError(f"release_type {self.release_type} not in {self.VALID_TYPE}")
            )

        # Prefetch the branch to have it updated
        if self._git.branch == branch:
            self.run("git pull --no-recurse-submodules")
        else:
            self.run(
                f"git fetch {self.repo.url} {branch}:{branch} --no-recurse-submodules"
            )
        output = self.run(f"git branch --contains={self.release_commit} {branch}")
        if branch not in output:
            raise RuntimeError(
                f"commit {self.release_commit} must belong to {branch} "
                f"for {self.release_type} release"
            )

    def _update_cmake_contributors(
        self, version: ClickHouseVersion, reset_tweak: bool = True
    ) -> None:
        if reset_tweak:
            desc = version.description
            version = version.reset_tweak()
            version.with_description(desc)
        update_cmake_version(version)
        update_contributors(raise_error=True)
        if self.dry_run:
            logging.info(
                "Dry running, resetting the following changes in the repo:\n%s",
                self.run(f"git diff '{self.CMAKE_PATH}' '{self.CONTRIBUTORS_PATH}'"),
            )
            self.run(f"git checkout '{self.CMAKE_PATH}' '{self.CONTRIBUTORS_PATH}'")

    def _commit_cmake_contributors(
        self, version: ClickHouseVersion, reset_tweak: bool = True
    ) -> None:
        if reset_tweak:
            version = version.reset_tweak()
        self.run(
            f"git commit '{self.CMAKE_PATH}' '{self.CONTRIBUTORS_PATH}' "
            f"-m 'Update autogenerated version to {version.string} and contributors'",
            dry_run=self.dry_run,
        )

    @property
    def bump_part(self) -> ClickHouseVersion.PART_TYPE:
        if self.release_type == Release.NEW:
            if self._version.minor >= 12:
                return "major"
            return "minor"
        return "patch"

    @property
    def has_rollback(self) -> bool:
        return bool(self._rollback_stack)

    def log_rollback(self):
        if self.has_rollback:
            rollback = self._rollback_stack.copy()
            rollback.reverse()
            logging.info(
                "To rollback the action run the following commands:\n  %s",
                "\n  ".join(rollback),
            )

    def log_post_workflows(self):
        logging.info(
            "To verify all actions are running good visit the following links:\n  %s",
            "\n  ".join(
                f"https://github.com/{self.repo}/actions/workflows/{action}.yml"
                for action in ("release", "tags_stable")
            ),
        )

    @contextmanager
    def create_release_branch(self):
        self.check_no_tags_after()
        # Create release branch
        self.read_version()
        with self._create_branch(self.release_branch, self.release_commit):
            with self._checkout(self.release_branch, True):
                with self._bump_release_branch():
                    yield

    @contextmanager
    def patch_release(self):
        self.check_no_tags_after()
        self.read_version()
        version_type = self.get_stable_release_type()
        self.version.with_description(version_type)
        with self._create_gh_release(False):
            self.version = self.version.update(self.bump_part)
            self.version.with_description(version_type)
            self._update_cmake_contributors(self.version)
            # Checking out the commit of the branch and not the branch itself,
            # then we are able to skip rollback
            with self._checkout(f"{self.release_branch}^0", False):
                current_commit = self.run("git rev-parse HEAD")
                self._commit_cmake_contributors(self.version)
                with self._push(
                    "HEAD", with_rollback_on_fail=False, remote_ref=self.release_branch
                ):
                    # DO NOT PUT ANYTHING ELSE HERE
                    # The push must be the last action and mean the successful release
                    self._rollback_stack.append(
                        f"{self.dry_run_prefix}git push {self.repo.url} "
                        f"+{current_commit}:{self.release_branch}"
                    )
                    yield

    @contextmanager
    def new_release(self):
        # Create branch for a version bump
        self.read_version()
        self.version = self.version.update(self.bump_part)
        helper_branch = f"{self.version.major}.{self.version.minor}-prepare"
        with self._create_branch(helper_branch, self.release_commit):
            with self._checkout(helper_branch, True):
                with self._bump_version_in_master(helper_branch):
                    yield

    @property
    def version(self) -> ClickHouseVersion:
        return self._version

    @version.setter
    def version(self, version: ClickHouseVersion) -> None:
        if not isinstance(version, ClickHouseVersion):
            raise ValueError(f"version must be ClickHouseVersion, not {type(version)}")
        self._version = version

    @property
    def release_branch(self) -> str:
        return self._release_branch

    @release_branch.setter
    def release_branch(self, branch: str) -> None:
        self._release_branch = release_branch(branch)

    @property
    def release_commit(self) -> str:
        return self._release_commit

    @release_commit.setter
    def release_commit(self, release_commit: str) -> None:
        self._release_commit = commit(release_commit)

    @property
    def dry_run_prefix(self) -> str:
        if self.dry_run:
            return "# "
        return ""

    @contextmanager
    def _bump_release_branch(self):
        # Update only git, original version stays the same
        self._git.update()
        new_version = self.version.patch_update()
        version_type = self.get_stable_release_type()
        pr_labels = f"--label {Labels.RELEASE}"
        if version_type == VersionType.LTS:
            pr_labels += f" --label {Labels.RELEASE_LTS}"
        new_version.with_description(version_type)
        self._update_cmake_contributors(new_version)
        self._commit_cmake_contributors(new_version)
        with self._push(self.release_branch):
            with self._create_gh_label(
                f"v{self.release_branch}-must-backport", "10dbed"
            ):
                with self._create_gh_label(
                    f"v{self.release_branch}-affected", "c2bfff"
                ):
                    # The following command is rolled back by deleting branch
                    # in self._push
                    self.run(
                        f"gh pr create --repo {self.repo} --title "
                        f"'Release pull request for branch {self.release_branch}' "
                        f"--head {self.release_branch} {pr_labels} "
                        "--body 'This PullRequest is a part of ClickHouse release "
                        "cycle. It is used by CI system only. Do not perform any "
                        "changes with it.'",
                        dry_run=self.dry_run,
                    )
                    with self._create_gh_release(False):
                        # Here the release branch part is done
                        yield

    @contextmanager
    def _bump_version_in_master(self, helper_branch: str) -> Iterator[None]:
        self.read_version()
        self.version = self.version.update(self.bump_part)
        self.version.with_description(VersionType.TESTING)
        self._update_cmake_contributors(self.version)
        self._commit_cmake_contributors(self.version)
        with self._push(helper_branch):
            body_file = get_abs_path(".github/PULL_REQUEST_TEMPLATE.md")
            # The following command is rolled back by deleting branch in self._push
            self.run(
                f"gh pr create --repo {self.repo} --title 'Update version after "
                f"release' --head {helper_branch} --body-file '{body_file}' "
                "--label 'do not test' --assignee @me",
                dry_run=self.dry_run,
            )
            # Here the new release part is done
            yield

    @contextmanager
    def _checkout(self, ref: str, with_checkout_back: bool = False) -> Iterator[None]:
        orig_ref = self._git.branch or self._git.sha
        need_rollback = False
        if ref not in (self._git.branch, self._git.sha):
            need_rollback = True
            self.run(f"git checkout {ref}")
            # checkout is not put into rollback_stack intentionally
            rollback_cmd = f"git checkout {orig_ref}"
            # always update version and git after checked out ref
            self.read_version()
        try:
            yield
        except (Exception, KeyboardInterrupt):
            logging.warning("Rolling back checked out %s for %s", ref, orig_ref)
            self.run(f"git reset --hard; git checkout -f {orig_ref}")
            raise
        # Normal flow when we need to checkout back
        if with_checkout_back and need_rollback:
            self.run(rollback_cmd)

    @contextmanager
    def _create_branch(self, name: str, start_point: str = "") -> Iterator[None]:
        self.run(f"git branch {name} {start_point}")

        rollback_cmd = f"git branch -D {name}"
        self._rollback_stack.append(rollback_cmd)
        try:
            yield
        except (Exception, KeyboardInterrupt):
            logging.warning("Rolling back created branch %s", name)
            self.run(rollback_cmd)
            raise

    @contextmanager
    def _create_gh_label(self, label: str, color_hex: str) -> Iterator[None]:
        # API call, https://docs.github.com/en/rest/reference/issues#create-a-label
        self.run(
            f"gh api repos/{self.repo}/labels -f name={label} -f color={color_hex}",
            dry_run=self.dry_run,
        )
        rollback_cmd = (
            f"{self.dry_run_prefix}gh api repos/{self.repo}/labels/{label} -X DELETE"
        )
        self._rollback_stack.append(rollback_cmd)
        try:
            yield
        except (Exception, KeyboardInterrupt):
            logging.warning("Rolling back label %s", label)
            self.run(rollback_cmd)
            raise

    @contextmanager
    def _create_gh_release(self, as_prerelease: bool) -> Iterator[None]:
        with self._create_tag():
            # Preserve tag if version is changed
            tag = self.release_version.describe
            prerelease = ""
            if as_prerelease:
                prerelease = "--prerelease"
            self.run(
                f"gh release create {prerelease} --repo {self.repo} "
                f"--title 'Release {tag}' '{tag}'",
                dry_run=self.dry_run,
            )
            rollback_cmd = (
                f"{self.dry_run_prefix}gh release delete --yes "
                f"--repo {self.repo} '{tag}'"
            )
            self._rollback_stack.append(rollback_cmd)
            try:
                yield
            except (Exception, KeyboardInterrupt):
                logging.warning("Rolling back release publishing")
                self.run(rollback_cmd)
                raise

    @contextmanager
    def _create_tag(self):
        tag = self.release_version.describe
        self.run(
            f"git tag -a -m 'Release {tag}' '{tag}' {self.release_commit}",
            dry_run=self.dry_run,
        )
        rollback_cmd = f"{self.dry_run_prefix}git tag -d '{tag}'"
        self._rollback_stack.append(rollback_cmd)
        try:
            with self._push(tag):
                yield
        except (Exception, KeyboardInterrupt):
            logging.warning("Rolling back tag %s", tag)
            self.run(rollback_cmd)
            raise

    @contextmanager
    def _push(
        self, ref: str, with_rollback_on_fail: bool = True, remote_ref: str = ""
    ) -> Iterator[None]:
        if remote_ref == "":
            remote_ref = ref

        self.run(f"git push {self.repo.url} {ref}:{remote_ref}", dry_run=self.dry_run)
        if with_rollback_on_fail:
            rollback_cmd = (
                f"{self.dry_run_prefix}git push -d {self.repo.url} {remote_ref}"
            )
            self._rollback_stack.append(rollback_cmd)

        try:
            yield
        except (Exception, KeyboardInterrupt):
            if with_rollback_on_fail:
                logging.warning("Rolling back pushed ref %s", ref)
                self.run(rollback_cmd)

            raise


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="Script to release a new ClickHouse version, requires `git` and "
        "`gh` (github-cli) commands "
        "!!! LAUNCH IT ONLY FROM THE MASTER BRANCH !!!",
    )

    parser.add_argument(
        "--commit",
        required=True,
        type=commit,
        help="commit create a release",
    )
    parser.add_argument(
        "--repo",
        default="ClickHouse/ClickHouse",
        help="repository to create the release",
    )
    parser.add_argument(
        "--remote-protocol",
        "-p",
        default="ssh",
        choices=Repo.VALID,
        help="repo protocol for git commands remote, 'origin' is a special case and "
        "uses 'origin' as a remote",
    )
    parser.add_argument(
        "--type",
        required=True,
        choices=Release.VALID_TYPE,
        dest="release_type",
        help="a release type to bump the major.minor.patch version part, "
        "new branch is created only for the value 'new'",
    )
    parser.add_argument("--with-release-branch", default=True, help=argparse.SUPPRESS)
    parser.add_argument("--check-dirty", default=True, help=argparse.SUPPRESS)
    parser.add_argument(
        "--no-check-dirty",
        dest="check_dirty",
        action="store_false",
        default=argparse.SUPPRESS,
        help="(dangerous) if set, skip check repository for uncommitted changes",
    )
    parser.add_argument("--check-run-from-master", default=True, help=argparse.SUPPRESS)
    parser.add_argument(
        "--no-run-from-master",
        dest="check_run_from_master",
        action="store_false",
        default=argparse.SUPPRESS,
        help="(for development) if set, the script could run from non-master branch",
    )
    parser.add_argument("--check-branch", default=True, help=argparse.SUPPRESS)
    parser.add_argument(
        "--no-check-branch",
        dest="check_branch",
        action="store_false",
        default=argparse.SUPPRESS,
        help="(debug or development only, dangerous) if set, skip the branch check for "
        "a run. By default, 'new' type work only for master, and 'patch' "
        "works only for a release branches, that name "
        "should be the same as '$MAJOR.$MINOR' version, e.g. 22.2",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="do not make any actual changes in the repo, just show what will be done",
    )
    parser.add_argument(
        "--with-stderr",
        action="store_true",
        help="if set, the stderr of all subprocess commands will be printed as well",
    )

    return parser.parse_args()


def main():
    logging.basicConfig(level=logging.INFO)
    args = parse_args()
    repo = Repo(args.repo, args.remote_protocol)
    release = Release(
        repo, args.commit, args.release_type, args.dry_run, args.with_stderr
    )

    try:
        release.do(args.check_dirty, args.check_run_from_master, args.check_branch)
    except:
        if release.has_rollback:
            logging.error(
                "!!The release process finished with error, read the output carefully!!"
            )
            logging.error(
                "Probably, rollback finished with error. "
                "If you don't see any of the following commands in the output, "
                "execute them manually:"
            )
            release.log_rollback()
        raise


if __name__ == "__main__":
    main()
