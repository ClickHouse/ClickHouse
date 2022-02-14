#!/usr/bin/env python


from contextlib import contextmanager
from typing import Optional
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

    def run(self, cmd: str, cwd: Optional[str] = None) -> str:
        logging.info("Running in directory %s, command:\n    %s", cwd or "$CWD", cmd)
        return self._git.run(cmd, cwd)

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

    def update(self):
        self._git.update()
        self.version = get_version_from_repo()

    @contextmanager
    def _new_branch(self, name: str, start_point: str = ""):
        self.run(f"git branch {name} {start_point}")
        try:
            yield
        except BaseException:
            logging.warning("Rolling back created branch %s", name)
            self.run(f"git branch -D {name}")
            raise

    @contextmanager
    def _checkout(self, ref: str, with_rollback: bool = False):
        orig_ref = self._git.branch or self._git.sha
        need_rollback = False
        if ref not in (self._git.branch, self._git.sha):
            need_rollback = True
            self.run(f"git checkout {ref}")
        try:
            yield
        except BaseException:
            logging.warning("Rolling back checked out %s for %s", ref, orig_ref)
            self.run(f"git reset --hard; git checkout {orig_ref}")
            raise
        else:
            if with_rollback and need_rollback:
                self.run(f"git checkout {orig_ref}")

    @contextmanager
    def prestable(self, args: argparse.Namespace):
        self.check_no_tags_after()
        # Create release branch
        # TODO: this place is wrong. If we are in stale branch, it will produce
        # a wrong version
        self.update()
        release_branch = f"{self.version.major}.{self.version.minor}"
        with self._new_branch(release_branch, self.release_commit):
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
        # TODO: this place is wrong. If we are in stale branch, it will produce
        # a wrong version
        self.version = self.version.update(args.release_type)
        helper_branch = f"{self.version.major}.{self.version.minor}-prepare"
        with self._new_branch(helper_branch, self.release_commit):
            with self._checkout(helper_branch, True):
                self.update()
                self.version = self.version.update(args.release_type)
                with self._bump_testing_version(helper_branch, args):
                    yield

    @contextmanager
    def _bump_testing_version(self, helper_branch: str, args: argparse.Namespace):
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
    def _bump_prestable_version(self, release_branch: str, args: argparse.Namespace):
        new_version = self.version.patch_update()
        update_cmake_version(new_version)
        cmake_path = get_abs_path(FILE_WITH_VERSION_PATH)
        self.run(
            f"git commit -m 'Update version to {new_version.string}' '{cmake_path}'"
        )
        with self._push(release_branch, args):
            self.run(
                f"gh pr create --repo {args.repo} --title 'Release pull request for "
                f"branch {release_branch}' --head {release_branch} --body 'This "
                "PullRequest is a part of ClickHouse release cycle. It is used by CI "
                "system only. Do not perform any changes with it.' --label release"
            )
            # Here the prestable part is done
            yield

    @contextmanager
    def _create_gh_release(self, args: argparse.Namespace):
        with self._create_tag(args):
            # Preserve tag if version is changed
            tag = self.version.describe
            self.run(
                f"gh release create --prerelease --draft --repo {args.repo} '{tag}'"
            )
            try:
                yield
            except BaseException:
                logging.warning("Rolling back release publishing")
                self.run(f"gh release delete --yes --repo {args.repo} '{tag}'")
                raise

    @contextmanager
    def _create_tag(self, args: argparse.Namespace):
        tag = self.version.describe
        self.run(f"git tag -a -m 'Release {tag}' '{tag}'")
        try:
            with self._push(f"'{tag}'", args):
                yield
        except BaseException:
            logging.warning("Rolling back tag %s", tag)
            self.run(f"git tag -d '{tag}'")
            raise

    @contextmanager
    def _push(self, ref: str, args: argparse.Namespace):
        self.run(f"git push git@github.com:{args.repo}.git {ref}")
        try:
            yield
        except BaseException:
            logging.warning("Rolling back pushed ref %s", ref)
            self.run(f"git push -d git@github.com:{args.repo}.git {ref}")
            raise

    def do(self, args: argparse.Namespace):
        self.release_commit = args.commit

        if not args.no_check_dirty:
            logging.info("Checking if repo is clean")
            self.run("git diff HEAD --exit-code")

        if not args.no_check_branch:
            self.check_branch(args.release_type)

        if args.release_type in self.BIG:
            if args.no_prestable:
                logging.info("Skipping prestable stage")
            else:
                with self.prestable(args):
                    logging.info("Prestable part of the releasing is done")

            with self.testing(args):
                logging.info("Testing part of the releasing is done")


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
    parser.add_argument(
        "--no-publish-release",
        action="store_true",
        help="by default, 'major' and 'minor' types work only for master, and 'patch' ",
    )

    return parser.parse_args()


def prestable():
    pass


def main():
    logging.basicConfig(level=logging.INFO)
    args = parse_args()
    release = Release(get_version_from_repo())

    release.do(args)

    # if not args.no_publish_release:
    #    # Publish release on github for the current HEAD (master, if checked)
    #    git.run(f"gh release create --draft {git.new_tag} --target {git.sha}")

    ## Commit updated versions to HEAD and push to remote
    # write_versions(versions_file, new_versions)
    # git.run(f"git checkout -b {git.new_branch}-helper")
    # git.run(
    #    f"git commit -m 'Auto version update to [{new_versions['VERSION_STRING']}] "
    #    f"[{new_versions['VERSION_REVISION']}]' {versions_file}"
    # )
    # git.run(f"git push -u origin {git.new_branch}-helper")
    # git.run(
    #    f"gh pr create --title 'Update version after release {git.new_branch}' "
    #    f"--body-file '{git.root}/.github/PULL_REQUEST_TEMPLATE.md'"
    # )

    ## Create a new branch from the previous commit and push there with creating
    ## a PR
    # git.run(f"git checkout -b {git.new_branch} HEAD~")
    # write_versions(versions_file, versions)
    # git.run(
    #    f"git commit -m 'Auto version update to [{versions['VERSION_STRING']}] "
    #    f"[{versions['VERSION_REVISION']}]' {versions_file}"
    # )
    # git.run(f"git push -u origin {git.new_branch}")
    # git.run(
    #    "gh pr create --title 'Release pull request for branch "
    #    f"{versions['VERSION_MAJOR']}.{versions['VERSION_MINOR']}' --body "
    #    "'This PullRequest is part of ClickHouse release cycle. It is used by CI "
    #    "system only. Do not perform any changes with it.' --label release"
    # )


if __name__ == "__main__":
    main()
