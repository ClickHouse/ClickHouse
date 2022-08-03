#!/usr/bin/env python


from contextlib import contextmanager
from typing import List, Optional
import argparse
import logging
import subprocess

from git_helper import commit, release_branch
from version_helper import (
    FILE_WITH_VERSION_PATH,
    GENERATED_CONTRIBUTORS,
    ClickHouseVersion,
    Git,
    VersionType,
    get_abs_path,
    get_version_from_repo,
    update_cmake_version,
    update_contributors,
)


git = Git()


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
    def url(self, protocol: str):
        if protocol == "ssh":
            self._url = f"git@github.com:{self}.git"
        elif protocol == "https":
            self._url = f"https://github.com/{self}.git"
        elif protocol == "origin":
            self._url = protocol
        else:
            raise Exception(f"protocol must be in {self.VALID}")

    def __str__(self):
        return self._repo


class Release:
    BIG = ("major", "minor")
    SMALL = ("patch",)
    CMAKE_PATH = get_abs_path(FILE_WITH_VERSION_PATH)
    CONTRIBUTORS_PATH = get_abs_path(GENERATED_CONTRIBUTORS)

    def __init__(self, repo: Repo, release_commit: str, release_type: str):
        self.repo = repo
        self._release_commit = ""
        self.release_commit = release_commit
        self.release_type = release_type
        self._git = git
        self._version = get_version_from_repo(git=self._git)
        self._release_branch = ""
        self._rollback_stack = []  # type: List[str]

    def run(self, cmd: str, cwd: Optional[str] = None) -> str:
        cwd_text = ""
        if cwd:
            cwd_text = f" (CWD='{cwd}')"
        logging.info("Running command%s:\n    %s", cwd_text, cmd)
        return self._git.run(cmd, cwd)

    def set_release_branch(self):
        # Fetch release commit in case it does not exist locally
        self.run(f"git fetch {self.repo.url} {self.release_commit}")

        # Get the actual version for the commit before check
        with self._checkout(self.release_commit, True):
            self.read_version()
            self.release_branch = f"{self.version.major}.{self.version.minor}"

        self.read_version()

    def read_version(self):
        self._git.update()
        self.version = get_version_from_repo(git=self._git)

    def get_stable_release_type(self) -> str:
        if self.version.minor % 5 == 3:  # our 3 and 8 are LTS
            return VersionType.LTS
        return VersionType.STABLE

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

    def do(self, check_dirty: bool, check_branch: bool, with_release_branch: bool):
        self.check_prerequisites()

        if check_dirty:
            logging.info("Checking if repo is clean")
            try:
                self.run("git diff HEAD --exit-code")
            except subprocess.CalledProcessError:
                logging.fatal("Repo contains uncommitted changes")
                raise

        self.set_release_branch()

        if check_branch:
            self.check_branch()

        with self._checkout(self.release_commit, True):
            if self.release_type in self.BIG:
                # Checkout to the commit, it will provide the correct current version
                if with_release_branch:
                    with self.create_release_branch():
                        logging.info("Prestable part of the releasing is done")
                else:
                    logging.info("Skipping creating release branch stage")

                rollback = self._rollback_stack.copy()
                try:
                    with self.testing():
                        logging.info("Testing part of the releasing is done")
                except (Exception, KeyboardInterrupt):
                    logging.fatal("Testing part failed, rollback previous steps")
                    rollback.reverse()
                    for cmd in rollback:
                        self.run(cmd)
                    raise

            elif self.release_type in self.SMALL:
                with self.stable():
                    logging.info("Stable part of the releasing is done")

        self.log_rollback()

    def check_no_tags_after(self):
        tags_after_commit = self.run(f"git tag --contains={self.release_commit}")
        if tags_after_commit:
            raise Exception(
                f"Commit {self.release_commit} belongs to following tags:\n"
                f"{tags_after_commit}\nChoose another commit"
            )

    def check_branch(self):
        branch = self.release_branch
        if self.release_type in self.BIG:
            # Commit to spin up the release must belong to a main branch
            branch = "master"
        elif self.release_type not in self.SMALL:
            raise (
                ValueError(
                    f"release_type {self.release_type} neiter in {self.BIG} nor "
                    f"in {self.SMALL}"
                )
            )

        # Prefetch the branch to have it updated
        if self._git.branch == branch:
            self.run("git pull")
        else:
            self.run(f"git fetch {self.repo.url} {branch}:{branch}")
        output = self.run(f"git branch --contains={self.release_commit} {branch}")
        if branch not in output:
            raise Exception(
                f"commit {self.release_commit} must belong to {branch} "
                f"for {self.release_type} release"
            )

    def log_rollback(self):
        if self._rollback_stack:
            rollback = self._rollback_stack.copy()
            rollback.reverse()
            logging.info(
                "To rollback the action run the following commands:\n  %s",
                "\n  ".join(rollback),
            )

    @contextmanager
    def create_release_branch(self):
        self.check_no_tags_after()
        # Create release branch
        self.read_version()
        with self._create_branch(self.release_branch, self.release_commit):
            with self._checkout(self.release_branch, True):
                self.read_version()
                self.version.with_description(self.get_stable_release_type())
                with self._create_gh_release(False):
                    with self._bump_release_branch():
                        # At this point everything will rollback automatically
                        yield

    @contextmanager
    def stable(self):
        self.check_no_tags_after()
        self.read_version()
        version_type = self.get_stable_release_type()
        self.version.with_description(version_type)
        with self._create_gh_release(False):
            self.version = self.version.update(self.release_type)
            self.version.with_description(version_type)
            update_cmake_version(self.version)
            update_contributors(raise_error=True)
            # Checkouting the commit of the branch and not the branch itself,
            # then we are able to skip rollback
            with self._checkout(f"{self.release_branch}^0", False):
                current_commit = self.run("git rev-parse HEAD")
                self.run(
                    f"git commit -m "
                    f"'Update version to {self.version.string}' "
                    f"'{self.CMAKE_PATH}' '{self.CONTRIBUTORS_PATH}'"
                )
                with self._push(
                    "HEAD", with_rollback_on_fail=False, remote_ref=self.release_branch
                ):
                    # DO NOT PUT ANYTHING ELSE HERE
                    # The push must be the last action and mean the successful release
                    self._rollback_stack.append(
                        f"git push {self.repo.url} "
                        f"+{current_commit}:{self.release_branch}"
                    )
                    yield

    @contextmanager
    def testing(self):
        # Create branch for a version bump
        self.read_version()
        self.version = self.version.update(self.release_type)
        helper_branch = f"{self.version.major}.{self.version.minor}-prepare"
        with self._create_branch(helper_branch, self.release_commit):
            with self._checkout(helper_branch, True):
                with self._bump_testing_version(helper_branch):
                    yield

    @property
    def version(self) -> ClickHouseVersion:
        return self._version

    @version.setter
    def version(self, version: ClickHouseVersion):
        if not isinstance(version, ClickHouseVersion):
            raise ValueError(f"version must be ClickHouseVersion, not {type(version)}")
        self._version = version

    @property
    def release_branch(self) -> str:
        return self._release_branch

    @release_branch.setter
    def release_branch(self, branch: str):
        self._release_branch = release_branch(branch)

    @property
    def release_commit(self) -> str:
        return self._release_commit

    @release_commit.setter
    def release_commit(self, release_commit: str):
        self._release_commit = commit(release_commit)

    @contextmanager
    def _bump_release_branch(self):
        # Update only git, origal version stays the same
        self._git.update()
        new_version = self.version.patch_update()
        version_type = self.get_stable_release_type()
        pr_labels = "--label release"
        if version_type == VersionType.LTS:
            pr_labels += " --label release-lts"
        new_version.with_description(version_type)
        update_cmake_version(new_version)
        update_contributors(raise_error=True)
        self.run(
            f"git commit -m 'Update version to {new_version.string}' "
            f"'{self.CMAKE_PATH}' '{self.CONTRIBUTORS_PATH}'"
        )
        with self._push(self.release_branch):
            with self._create_gh_label(
                f"v{self.release_branch}-must-backport", "10dbed"
            ):
                with self._create_gh_label(
                    f"v{self.release_branch}-affected", "c2bfff"
                ):
                    # The following command is rolled back by self._push
                    self.run(
                        f"gh pr create --repo {self.repo} --title "
                        f"'Release pull request for branch {self.release_branch}' "
                        f"--head {self.release_branch} {pr_labels} "
                        "--body 'This PullRequest is a part of ClickHouse release "
                        "cycle. It is used by CI system only. Do not perform any "
                        "changes with it.'"
                    )
                    # Here the release branch part is done
                    yield

    @contextmanager
    def _bump_testing_version(self, helper_branch: str):
        self.read_version()
        self.version = self.version.update(self.release_type)
        self.version.with_description(VersionType.TESTING)
        update_cmake_version(self.version)
        update_contributors(raise_error=True)
        self.run(
            f"git commit -m 'Update version to {self.version.string}' "
            f"'{self.CMAKE_PATH}' '{self.CONTRIBUTORS_PATH}'"
        )
        with self._push(helper_branch):
            body_file = get_abs_path(".github/PULL_REQUEST_TEMPLATE.md")
            self.run(
                f"gh pr create --repo {self.repo} --title 'Update version after "
                f"release' --head {helper_branch} --body-file '{body_file}'"
            )
            # Here the testing part is done
            yield

    @contextmanager
    def _checkout(self, ref: str, with_checkout_back: bool = False):
        orig_ref = self._git.branch or self._git.sha
        need_rollback = False
        if ref not in (self._git.branch, self._git.sha):
            need_rollback = True
            self.run(f"git checkout {ref}")
            # checkout is not put into rollback_stack intentionally
            rollback_cmd = f"git checkout {orig_ref}"
        try:
            yield
        except (Exception, KeyboardInterrupt):
            logging.warning("Rolling back checked out %s for %s", ref, orig_ref)
            self.run(f"git reset --hard; git checkout {orig_ref}")
            raise
        else:
            if with_checkout_back and need_rollback:
                self.run(rollback_cmd)

    @contextmanager
    def _create_branch(self, name: str, start_point: str = ""):
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
    def _create_gh_label(self, label: str, color_hex: str):
        # API call, https://docs.github.com/en/rest/reference/issues#create-a-label
        self.run(
            f"gh api repos/{self.repo}/labels -f name={label} -f color={color_hex}"
        )
        rollback_cmd = f"gh api repos/{self.repo}/labels/{label} -X DELETE"
        self._rollback_stack.append(rollback_cmd)
        try:
            yield
        except (Exception, KeyboardInterrupt):
            logging.warning("Rolling back label %s", label)
            self.run(rollback_cmd)
            raise

    @contextmanager
    def _create_gh_release(self, as_prerelease: bool):
        with self._create_tag():
            # Preserve tag if version is changed
            tag = self.version.describe
            prerelease = ""
            if as_prerelease:
                prerelease = "--prerelease"
            self.run(
                f"gh release create {prerelease} --repo {self.repo} "
                f"--title 'Release {tag}' '{tag}'"
            )
            rollback_cmd = f"gh release delete --yes --repo {self.repo} '{tag}'"
            self._rollback_stack.append(rollback_cmd)
            try:
                yield
            except (Exception, KeyboardInterrupt):
                logging.warning("Rolling back release publishing")
                self.run(rollback_cmd)
                raise

    @contextmanager
    def _create_tag(self):
        tag = self.version.describe
        self.run(f"git tag -a -m 'Release {tag}' '{tag}'")
        rollback_cmd = f"git tag -d '{tag}'"
        self._rollback_stack.append(rollback_cmd)
        try:
            with self._push(f"'{tag}'"):
                yield
        except (Exception, KeyboardInterrupt):
            logging.warning("Rolling back tag %s", tag)
            self.run(rollback_cmd)
            raise

    @contextmanager
    def _push(self, ref: str, with_rollback_on_fail: bool = True, remote_ref: str = ""):
        if remote_ref == "":
            remote_ref = ref

        self.run(f"git push {self.repo.url} {ref}:{remote_ref}")
        if with_rollback_on_fail:
            rollback_cmd = f"git push -d {self.repo.url} {remote_ref}"
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
        "`gh` (github-cli) commands",
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
        default="minor",
        choices=Release.BIG + Release.SMALL,
        dest="release_type",
        help="a release type, new branch is created only for 'major' and 'minor'",
    )
    parser.add_argument("--with-release-branch", default=True, help=argparse.SUPPRESS)
    parser.add_argument(
        "--no-release-branch",
        dest="with_release_branch",
        action="store_false",
        default=argparse.SUPPRESS,
        help=f"if set, for release types in {Release.BIG} skip creating release branch",
    )
    parser.add_argument("--check-dirty", default=True, help=argparse.SUPPRESS)
    parser.add_argument(
        "--no-check-dirty",
        dest="check_dirty",
        action="store_false",
        default=argparse.SUPPRESS,
        help="(dangerous) if set, skip check repository for uncommited changes",
    )
    parser.add_argument("--check-branch", default=True, help=argparse.SUPPRESS)
    parser.add_argument(
        "--no-check-branch",
        dest="check_branch",
        action="store_false",
        default=argparse.SUPPRESS,
        help="(debug or development only, dangerous) if set, skip the branch check for "
        "a run. By default, 'major' and 'minor' types workonly for master, and 'patch' "
        "works only for a release branches, that name "
        "should be the same as '$MAJOR.$MINOR' version, e.g. 22.2",
    )

    return parser.parse_args()


def main():
    logging.basicConfig(level=logging.INFO)
    args = parse_args()
    repo = Repo(args.repo, args.remote_protocol)
    release = Release(repo, args.commit, args.release_type)

    release.do(args.check_dirty, args.check_branch, args.with_release_branch)


if __name__ == "__main__":
    main()
