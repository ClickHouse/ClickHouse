#!/usr/bin/env python


from contextlib import contextmanager
from typing import List, Optional
import argparse
import logging

from git_helper import commit
from version_helper import (
    FILE_WITH_VERSION_PATH,
    ClickHouseVersion,
    VersionType,
    git,
    get_abs_path,
    get_version_from_repo,
    update_cmake_version,
)


class Release:
    BIG = ("major", "minor")
    SMALL = ("patch",)

    def __init__(self, version: ClickHouseVersion):
        self._version = version
        self._git = version._git
        self._release_commit = ""
        self._rollback_stack = []  # type: List[str]

    def run(self, cmd: str, cwd: Optional[str] = None) -> str:
        cwd_text = ""
        if cwd:
            cwd_text = f" (CWD='{cwd}')"
        logging.info("Running command%s:\n    %s", cwd_text, cmd)
        return self._git.run(cmd, cwd)

    def update(self):
        self._git.update()
        self.version = get_version_from_repo()

    def do(self, args: argparse.Namespace):
        self.release_commit = args.commit

        if not args.no_check_dirty:
            logging.info("Checking if repo is clean")
            self.run("git diff HEAD --exit-code")

        if not args.no_check_branch:
            self.check_branch(args.release_type)

        if args.release_type in self.BIG:
            # Checkout to the commit, it will provide the correct current version
            with self._checkout(self.release_commit, True):
                if args.no_prestable:
                    logging.info("Skipping prestable stage")
                else:
                    with self.prestable(args):
                        logging.info("Prestable part of the releasing is done")

                with self.testing(args):
                    logging.info("Testing part of the releasing is done")

        self.log_rollback()

    def check_no_tags_after(self):
        tags_after_commit = self.run(f"git tag --contains={self.release_commit}")
        if tags_after_commit:
            raise Exception(
                f"Commit {self.release_commit} belongs to following tags:\n"
                f"{tags_after_commit}\nChoose another commit"
            )

    def check_branch(self, release_type: str):
        if release_type in self.BIG:
            # Commit to spin up the release must belong to a main branch
            output = self.run(f"git branch --contains={self.release_commit} master")
            if "master" not in output:
                raise Exception(
                    f"commit {self.release_commit} must belong to 'master' for "
                    f"{release_type} release"
                )
        if release_type in self.SMALL:
            branch = f"{self.version.major}.{self.version.minor}"
            if self._git.branch != branch:
                raise Exception(f"branch must be '{branch}' for {release_type} release")

    def log_rollback(self):
        if self._rollback_stack:
            rollback = self._rollback_stack
            rollback.reverse()
            logging.info(
                "To rollback the action run the following commands:\n  %s",
                "\n  ".join(rollback),
            )

    @contextmanager
    def prestable(self, args: argparse.Namespace):
        self.check_no_tags_after()
        # Create release branch
        self.update()
        release_branch = f"{self.version.major}.{self.version.minor}"
        with self._create_branch(release_branch, self.release_commit):
            with self._checkout(release_branch, True):
                self.update()
                self.version.with_description(VersionType.PRESTABLE)
                with self._create_gh_release(args):
                    with self._bump_prestable_version(release_branch, args):
                        # At this point everything will rollback automatically
                        yield

    @contextmanager
    def testing(self, args: argparse.Namespace):
        # Create branch for a version bump
        self.update()
        self.version = self.version.update(args.release_type)
        helper_branch = f"{self.version.major}.{self.version.minor}-prepare"
        with self._create_branch(helper_branch, self.release_commit):
            with self._checkout(helper_branch, True):
                self.update()
                self.version = self.version.update(args.release_type)
                with self._bump_testing_version(helper_branch, args):
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
    def release_commit(self) -> str:
        return self._release_commit

    @release_commit.setter
    def release_commit(self, release_commit: str):
        self._release_commit = commit(release_commit)

    @contextmanager
    def _bump_prestable_version(self, release_branch: str, args: argparse.Namespace):
        self._git.update()
        new_version = self.version.patch_update()
        new_version.with_description("prestable")
        update_cmake_version(new_version)
        cmake_path = get_abs_path(FILE_WITH_VERSION_PATH)
        self.run(
            f"git commit -m 'Update version to {new_version.string}' '{cmake_path}'"
        )
        with self._push(release_branch, args):
            with self._create_gh_label(
                f"v{release_branch}-must-backport", "10dbed", args
            ):
                with self._create_gh_label(
                    f"v{release_branch}-affected", "c2bfff", args
                ):
                    self.run(
                        f"gh pr create --repo {args.repo} --title 'Release pull "
                        f"request for branch {release_branch}' --head {release_branch} "
                        "--body 'This PullRequest is a part of ClickHouse release "
                        "cycle. It is used by CI system only. Do not perform any "
                        "changes with it.' --label release"
                    )
                    # Here the prestable part is done
                    yield

    @contextmanager
    def _bump_testing_version(self, helper_branch: str, args: argparse.Namespace):
        self.version.with_description("testing")
        update_cmake_version(self.version)
        cmake_path = get_abs_path(FILE_WITH_VERSION_PATH)
        self.run(
            f"git commit -m 'Update version to {self.version.string}' '{cmake_path}'"
        )
        with self._push(helper_branch, args):
            body_file = get_abs_path(".github/PULL_REQUEST_TEMPLATE.md")
            self.run(
                f"gh pr create --repo {args.repo} --title 'Update version after "
                f"release' --head {helper_branch} --body-file '{body_file}'"
            )
            # Here the prestable part is done
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
        except BaseException:
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
        except BaseException:
            logging.warning("Rolling back created branch %s", name)
            self.run(rollback_cmd)
            raise

    @contextmanager
    def _create_gh_label(self, label: str, color: str, args: argparse.Namespace):
        self.run(f"gh api repos/{args.repo}/labels -f name={label} -f color={color}")
        rollback_cmd = f"gh api repos/{args.repo}/labels/{label} -X DELETE"
        self._rollback_stack.append(rollback_cmd)
        try:
            yield
        except BaseException:
            logging.warning("Rolling back label %s", label)
            self.run(rollback_cmd)
            raise

    @contextmanager
    def _create_gh_release(self, args: argparse.Namespace):
        with self._create_tag(args):
            # Preserve tag if version is changed
            tag = self.version.describe
            self.run(
                f"gh release create --prerelease --draft --repo {args.repo} "
                f"--title 'Release {tag}' '{tag}'"
            )
            rollback_cmd = f"gh release delete --yes --repo {args.repo} '{tag}'"
            self._rollback_stack.append(rollback_cmd)
            try:
                yield
            except BaseException:
                logging.warning("Rolling back release publishing")
                self.run(rollback_cmd)
                raise

    @contextmanager
    def _create_tag(self, args: argparse.Namespace):
        tag = self.version.describe
        self.run(f"git tag -a -m 'Release {tag}' '{tag}'")
        rollback_cmd = f"git tag -d '{tag}'"
        self._rollback_stack.append(rollback_cmd)
        try:
            with self._push(f"'{tag}'", args):
                yield
        except BaseException:
            logging.warning("Rolling back tag %s", tag)
            self.run(rollback_cmd)
            raise

    @contextmanager
    def _push(self, ref: str, args: argparse.Namespace):
        self.run(f"git push git@github.com:{args.repo}.git {ref}")
        rollback_cmd = f"git push -d git@github.com:{args.repo}.git {ref}"
        self._rollback_stack.append(rollback_cmd)
        try:
            yield
        except BaseException:
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
        "--repo",
        default="ClickHouse/ClickHouse",
        help="repository to create the release",
    )
    parser.add_argument(
        "--type",
        default="minor",
        # choices=Release.BIG+Release.SMALL, # add support later
        choices=Release.BIG + Release.SMALL,
        dest="release_type",
        help="a release type, new branch is created only for 'major' and 'minor'",
    )
    parser.add_argument(
        "--no-prestable",
        action="store_true",
        help=f"for release types in {Release.BIG} skip creating prestable release and "
        "release branch",
    )
    parser.add_argument(
        "--commit",
        default=git.sha,
        type=commit,
        help="commit create a release, default to HEAD",
    )
    parser.add_argument(
        "--no-check-dirty",
        action="store_true",
        help="skip check repository for uncommited changes",
    )
    parser.add_argument(
        "--no-check-branch",
        action="store_true",
        help="by default, 'major' and 'minor' types work only for master, and 'patch' "
        "works only for a release branches, that name should be the same as "
        "'$MAJOR.$MINOR' version, e.g. 22.2",
    )

    return parser.parse_args()


def prestable():
    pass


def main():
    logging.basicConfig(level=logging.INFO)
    args = parse_args()
    release = Release(get_version_from_repo())

    release.do(args)


if __name__ == "__main__":
    main()
