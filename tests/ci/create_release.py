import argparse
import dataclasses
import json
import os

from contextlib import contextmanager
from copy import copy
from pathlib import Path
from typing import Iterator, List

from git_helper import Git, GIT_PREFIX
from ssh import SSHAgent
from s3_helper import S3Helper
from ci_utils import Shell, GH
from ci_buddy import CIBuddy
from version_helper import (
    FILE_WITH_VERSION_PATH,
    GENERATED_CONTRIBUTORS,
    get_abs_path,
    get_version_from_repo,
    update_cmake_version,
    update_contributors,
    VersionType,
)
from ci_config import CI

CMAKE_PATH = get_abs_path(FILE_WITH_VERSION_PATH)
CONTRIBUTORS_PATH = get_abs_path(GENERATED_CONTRIBUTORS)
RELEASE_INFO_FILE = "/tmp/release_info.json"


class ReleaseProgress:
    STARTED = "started"
    DOWNLOAD_PACKAGES = "download packages"
    PUSH_RELEASE_TAG = "push release tag"
    PUSH_NEW_RELEASE_BRANCH = "push new release branch"
    BUMP_VERSION = "bump version"
    CREATE_GH_RELEASE = "create GH release"
    EXPORT_TGZ = "export TGZ packages"
    EXPORT_RPM = "export RPM packages"
    EXPORT_DEB = "export DEB packages"
    TEST_TGZ = "test TGZ packages"
    TEST_RPM = "test RPM packages"
    TEST_DEB = "test DEB packages"
    MERGE_CREATED_PRS = "merge created PRs"
    COMPLETED = "completed"


class ReleaseProgressDescription:
    OK = "OK"
    FAILED = "FAILED"


class ReleaseContextManager:
    def __init__(self, release_progress):
        self.release_progress = release_progress
        self.release_info = None

    def __enter__(self):
        if self.release_progress == ReleaseProgress.STARTED:
            # create initial release info
            self.release_info = ReleaseInfo(
                release_branch="NA",
                commit_sha=args.ref,
                release_tag="NA",
                version="NA",
                codename="NA",
                previous_release_tag="NA",
                previous_release_sha="NA",
                release_progress=ReleaseProgress.STARTED,
                latest=False,
            ).dump()
        else:
            # fetch release info from fs and update
            self.release_info = ReleaseInfo.from_file()
            assert self.release_info
            assert (
                self.release_info.progress_status == ReleaseProgressDescription.OK
            ), "Must be OK on the start of new context"
            self.release_info.release_progress = self.release_progress
            self.release_info.dump()
        return self.release_info

    def __exit__(self, exc_type, exc_value, traceback):
        assert self.release_info
        if exc_type is not None:
            self.release_info.progress_status = ReleaseProgressDescription.FAILED
        else:
            self.release_info.progress_status = ReleaseProgressDescription.OK
        self.release_info.dump()


@dataclasses.dataclass
class ReleaseInfo:
    version: str
    release_tag: str
    release_branch: str
    commit_sha: str
    latest: bool
    # lts or stable
    codename: str
    previous_release_tag: str
    previous_release_sha: str
    changelog_pr: str = ""
    version_bump_pr: str = ""
    prs_merged: bool = False
    release_url: str = ""
    debian: str = ""
    rpm: str = ""
    tgz: str = ""
    docker: str = ""
    release_progress: str = ""
    progress_status: str = ""

    def is_patch(self):
        return self.release_branch != "master"

    def is_new_release_branch(self):
        return self.release_branch == "master"

    @staticmethod
    def from_file() -> "ReleaseInfo":
        with open(RELEASE_INFO_FILE, "r", encoding="utf-8") as json_file:
            res = json.load(json_file)
        return ReleaseInfo(**res)

    def dump(self):
        print(f"Dump release info into [{RELEASE_INFO_FILE}]")
        with open(RELEASE_INFO_FILE, "w", encoding="utf-8") as f:
            print(json.dumps(dataclasses.asdict(self), indent=2), file=f)
        return self

    def prepare(
        self, commit_ref: str, release_type: str, skip_tag_check: bool
    ) -> "ReleaseInfo":
        version = None
        release_branch = None
        release_tag = None
        previous_release_tag = None
        previous_release_sha = None
        latest_release = False
        codename = ""
        assert release_type in ("patch", "new")
        if release_type == "new":
            # check commit_ref is right and on a right branch
            Shell.check(
                f"git merge-base --is-ancestor {commit_ref} origin/master",
                strict=True,
                verbose=True,
            )
            with checkout(commit_ref):
                commit_sha = Shell.get_output_or_raise(f"git rev-list -n1 {commit_ref}")
                # Git() must be inside "with checkout" contextmanager
                git = Git()
                version = get_version_from_repo(git=git)
                release_branch = "master"
                expected_prev_tag = f"v{version.major}.{version.minor}.1.1-new"
                version.bump().with_description(VersionType.NEW)
                assert (
                    git.latest_tag == expected_prev_tag
                ), f"BUG: latest tag [{git.latest_tag}], expected [{expected_prev_tag}]"
                release_tag = version.describe
                previous_release_tag = expected_prev_tag
                previous_release_sha = Shell.get_output_or_raise(
                    f"git rev-list -n1 {previous_release_tag}"
                )
                assert previous_release_sha
        if release_type == "patch":
            with checkout(commit_ref):
                commit_sha = Shell.get_output_or_raise(f"git rev-list -n1 {commit_ref}")
                # Git() must be inside "with checkout" contextmanager
                git = Git()
                version = get_version_from_repo(git=git)
                codename = version.get_stable_release_type()
                version.with_description(codename)
                release_branch = f"{version.major}.{version.minor}"
                release_tag = version.describe
            Shell.check(
                f"{GIT_PREFIX} fetch origin {release_branch} --tags",
                strict=True,
                verbose=True,
            )
            # check commit is right and on a right branch
            Shell.check(
                f"git merge-base --is-ancestor {commit_ref} origin/{release_branch}",
                strict=True,
                verbose=True,
            )
            if version.patch == 1:
                expected_version = copy(version)
                previous_release_tag = f"v{version.major}.{version.minor}.1.1-new"
                expected_version.bump()
                expected_tag_prefix = (
                    f"v{expected_version.major}.{expected_version.minor}."
                )
                expected_tag_suffix = "-new"
            else:
                expected_tag_prefix = (
                    f"v{version.major}.{version.minor}.{version.patch-1}."
                )
                expected_tag_suffix = f"-{version.get_stable_release_type()}"
                previous_release_tag = git.latest_tag
            if git.latest_tag.startswith(
                expected_tag_prefix
            ) and git.latest_tag.endswith(expected_tag_suffix):
                pass
            elif not skip_tag_check:
                assert (
                    False
                ), f"BUG: Unexpected latest tag [{git.latest_tag}] expected [{expected_tag_prefix}*{expected_tag_suffix}]. Already Released?"

            previous_release_sha = Shell.get_output_or_raise(
                f"git rev-list -n1 {previous_release_tag}"
            )
            assert previous_release_sha

            if CI.GH.is_latest_release_branch(release_branch):
                print("This is going to be the latest release!")
                latest_release = True

        assert (
            release_branch
            and previous_release_tag
            and previous_release_sha
            and commit_sha
            and release_tag
            and version
            and (codename in ("lts", "stable") or release_type == "new")
        ), f"Check: {release_branch}, {previous_release_tag}, {previous_release_sha}, {commit_sha}, {release_tag}, {version}"

        self.release_branch = release_branch
        self.commit_sha = commit_sha
        self.release_tag = release_tag
        self.version = version.string
        self.codename = codename
        self.previous_release_tag = previous_release_tag
        self.previous_release_sha = previous_release_sha
        self.release_progress = ReleaseProgress.STARTED
        self.progress_status = ReleaseProgressDescription.OK
        self.latest = latest_release
        return self

    def push_release_tag(self, dry_run: bool) -> None:
        if dry_run:
            # remove locally created tag from prev run
            Shell.check(
                f"{GIT_PREFIX} tag -l | grep -q {self.release_tag} && git tag -d {self.release_tag}"
            )
        # Create release tag
        print(
            f"Create and push release tag [{self.release_tag}], commit [{self.commit_sha}]"
        )
        tag_message = f"Release {self.release_tag}"
        Shell.check(
            f"{GIT_PREFIX} tag -a -m '{tag_message}' {self.release_tag} {self.commit_sha}",
            strict=True,
            verbose=True,
        )
        cmd_push_tag = f"{GIT_PREFIX} push origin {self.release_tag}:{self.release_tag}"
        Shell.check(cmd_push_tag, dry_run=dry_run, strict=True, verbose=True)

    @staticmethod
    def _create_gh_label(label: str, color_hex: str, dry_run: bool) -> None:
        cmd = f"gh api repos/{CI.Envs.GITHUB_REPOSITORY}/labels -f name={label} -f color={color_hex}"
        Shell.check(cmd, dry_run=dry_run, strict=True)

    def push_new_release_branch(self, dry_run: bool) -> None:
        assert (
            self.release_branch == "master"
        ), "New release branch can be created only for release type [new]"
        git = Git()
        version = get_version_from_repo(git=git)
        new_release_branch = f"{version.major}.{version.minor}"
        stable_release_type = version.get_stable_release_type()
        version_after_release = copy(version)
        version_after_release.bump()
        assert (
            version_after_release.string == self.version
        ), f"Unexpected current version in git, must precede [{self.version}] by one step, actual [{version.string}]"
        if dry_run:
            # remove locally created branch from prev run
            Shell.check(
                f"{GIT_PREFIX} branch -l | grep -q {new_release_branch} && git branch -d {new_release_branch}"
            )
        print(
            f"Create and push new release branch [{new_release_branch}], commit [{self.commit_sha}]"
        )
        with checkout(self.release_branch):
            with checkout_new(new_release_branch):
                pr_labels = f"--label {CI.Labels.RELEASE}"
                if stable_release_type == VersionType.LTS:
                    pr_labels += f" --label {CI.Labels.RELEASE_LTS}"
                cmd_push_branch = (
                    f"{GIT_PREFIX} push --set-upstream origin {new_release_branch}"
                )
                Shell.check(cmd_push_branch, dry_run=dry_run, strict=True, verbose=True)

        print("Create and push backport tags for new release branch")
        ReleaseInfo._create_gh_label(
            f"v{new_release_branch}-must-backport", "10dbed", dry_run=dry_run
        )
        ReleaseInfo._create_gh_label(
            f"v{new_release_branch}-affected", "c2bfff", dry_run=dry_run
        )
        Shell.check(
            f"""gh pr create --repo {CI.Envs.GITHUB_REPOSITORY} --title 'Release pull request for branch {new_release_branch}'
            --head {new_release_branch} {pr_labels}
            --body 'This PullRequest is a part of ClickHouse release cycle. It is used by CI system only. Do not perform any changes with it.'
            """,
            dry_run=dry_run,
            strict=True,
            verbose=True,
        )

    def get_version_bump_branch(self):
        return f"bump_version_{self.version}"

    def update_version_and_contributors_list(self, dry_run: bool) -> None:
        # Bump version, update contributors list, create PR
        branch_upd_version_contributors = self.get_version_bump_branch()
        with checkout(self.commit_sha):
            git = Git()
            version = get_version_from_repo(git=git)
            if self.release_branch == "master":
                version.bump()
                version.with_description(VersionType.TESTING)
            else:
                version.with_description(version.get_stable_release_type())
            assert (
                version.string == self.version
            ), f"BUG: version in release info does not match version in git commit, expected [{self.version}], got [{version.string}]"
        with checkout(self.release_branch):
            with checkout_new(branch_upd_version_contributors):
                update_cmake_version(version)
                update_contributors(raise_error=True)
                cmd_commit_version_upd = f"{GIT_PREFIX} commit '{CMAKE_PATH}' '{CONTRIBUTORS_PATH}' -m 'Update autogenerated version to {self.version} and contributors'"
                cmd_push_branch = f"{GIT_PREFIX} push --set-upstream origin {branch_upd_version_contributors}"
                actor = os.getenv("GITHUB_ACTOR", "") or "me"
                body = f"Automatic version bump after release {self.release_tag}\n### Changelog category (leave one):\n- Not for changelog (changelog entry is not required)\n"
                cmd_create_pr = f"gh pr create --repo {CI.Envs.GITHUB_REPOSITORY} --title 'Update version after release' --head {branch_upd_version_contributors} --base {self.release_branch} --body \"{body}\" --assignee {actor}"
                Shell.check(
                    cmd_commit_version_upd, strict=True, dry_run=dry_run, verbose=True
                )
                Shell.check(cmd_push_branch, strict=True, dry_run=dry_run, verbose=True)
                Shell.check(cmd_create_pr, strict=True, dry_run=dry_run, verbose=True)
                if dry_run:
                    Shell.check(
                        f"{GIT_PREFIX} diff '{CMAKE_PATH}' '{CONTRIBUTORS_PATH}'",
                        verbose=True,
                    )
                    Shell.check(
                        f"{GIT_PREFIX} checkout '{CMAKE_PATH}' '{CONTRIBUTORS_PATH}'",
                        verbose=True,
                    )
                    self.version_bump_pr = "dry-run"
                else:
                    self.version_bump_pr = GH.get_pr_url_by_branch(
                        branch=branch_upd_version_contributors
                    )

    def get_change_log_branch(self):
        return f"auto/{self.release_tag}"

    def update_release_info(self, dry_run: bool) -> "ReleaseInfo":
        if self.release_branch != "master":
            if not self.changelog_pr:
                branch = self.get_change_log_branch()
                if not dry_run:
                    url = GH.get_pr_url_by_branch(branch=branch)
                else:
                    url = "dry-run"
                print(f"ChangeLog PR url [{url}]")
                self.changelog_pr = url

            if not self.version_bump_pr:
                branch = self.get_version_bump_branch()
                if not dry_run:
                    url = GH.get_pr_url_by_branch(branch=branch)
                else:
                    url = "dry-run"
                print(f"Version bump PR url [{url}]")
                self.version_bump_pr = url

            self.release_url = f"https://github.com/{CI.Envs.GITHUB_REPOSITORY}/releases/tag/{self.release_tag}"
            print(f"Release url [{self.release_url}]")

            self.docker = f"docker run --rm clickhouse/clickhouse:{self.version} clickhouse --version"
        self.dump()
        return self

    def create_gh_release(self, packages_files: List[str], dry_run: bool) -> None:
        repo = CI.Envs.GITHUB_REPOSITORY
        assert repo
        cmds = [
            f"gh release create --repo {repo} --title 'Release {self.release_tag}' {self.release_tag}"
        ]
        for file in packages_files:
            cmds.append(f"gh release upload {self.release_tag} {file}")
        if not dry_run:
            for cmd in cmds:
                Shell.check(cmd, strict=True, verbose=True)
            self.release_url = (
                f"https://github.com/{repo}/releases/tag/{self.release_tag}"
            )
        else:
            print("Dry-run, would run commands:")
            print("\n  * ".join(cmds))
            self.release_url = f"dry-run"
        self.dump()

    def merge_prs(self, dry_run: bool) -> None:
        repo = CI.Envs.GITHUB_REPOSITORY
        assert self.version_bump_pr
        if dry_run:
            version_bump_pr_num = 12345
        else:
            version_bump_pr_num = int(self.version_bump_pr.split("/")[-1])
        print("Merging Version bump PR")
        res_1 = Shell.check(
            f"gh pr merge {version_bump_pr_num} --repo {repo} --merge --auto",
            verbose=True,
            dry_run=dry_run,
        )

        res_2 = True
        if not self.release_tag.endswith("-new"):
            assert self.changelog_pr
            print("Merging ChangeLog PR")
            if dry_run:
                changelog_pr_num = 23456
            else:
                changelog_pr_num = int(self.changelog_pr.split("/")[-1])
            res_2 = Shell.check(
                f"gh pr merge {changelog_pr_num} --repo {repo} --merge --auto",
                verbose=True,
                dry_run=dry_run,
            )
        else:
            assert not self.changelog_pr

        self.prs_merged = res_1 and res_2


class RepoTypes:
    RPM = "rpm"
    DEBIAN = "deb"
    TGZ = "tgz"


class PackageDownloader:
    PACKAGES = (
        "clickhouse-client",
        "clickhouse-common-static",
        "clickhouse-common-static-dbg",
        "clickhouse-keeper",
        "clickhouse-keeper-dbg",
        "clickhouse-server",
    )

    EXTRA_PACKAGES = (
        "clickhouse-library-bridge",
        "clickhouse-odbc-bridge",
    )
    PACKAGE_TYPES = (CI.BuildNames.PACKAGE_RELEASE, CI.BuildNames.PACKAGE_AARCH64)
    MACOS_PACKAGE_TO_BIN_SUFFIX = {
        CI.BuildNames.BINARY_DARWIN: "macos",
        CI.BuildNames.BINARY_DARWIN_AARCH64: "macos-aarch64",
    }
    LOCAL_DIR = "/tmp/packages"

    @classmethod
    def _get_arch_suffix(cls, package_arch, repo_type):
        if package_arch == CI.BuildNames.PACKAGE_RELEASE:
            return (
                "amd64" if repo_type in (RepoTypes.DEBIAN, RepoTypes.TGZ) else "x86_64"
            )
        elif package_arch == CI.BuildNames.PACKAGE_AARCH64:
            return (
                "arm64" if repo_type in (RepoTypes.DEBIAN, RepoTypes.TGZ) else "aarch64"
            )
        else:
            assert False, "BUG"

    def __init__(self, release, commit_sha, version):
        assert version.startswith(release), "Invalid release branch or version"
        major, minor = map(int, release.split("."))
        self.package_names = list(self.PACKAGES)
        if major > 24 or (major == 24 and minor > 3):
            self.package_names += list(self.EXTRA_PACKAGES)
        self.release = release
        self.commit_sha = commit_sha
        self.version = version
        self.s3 = S3Helper()
        self.deb_package_files = []
        self.rpm_package_files = []
        self.tgz_package_files = []
        # just binaries for macos
        self.macos_package_files = ["clickhouse-macos", "clickhouse-macos-aarch64"]
        self.file_to_type = {}

        Shell.check(f"mkdir -p {self.LOCAL_DIR}")

        for package_type in self.PACKAGE_TYPES:
            for package in self.package_names:
                deb_package_file_name = f"{package}_{self.version}_{self._get_arch_suffix(package_type, RepoTypes.DEBIAN)}.deb"
                self.deb_package_files.append(deb_package_file_name)
                self.file_to_type[deb_package_file_name] = package_type

                rpm_package_file_name = f"{package}-{self.version}.{self._get_arch_suffix(package_type, RepoTypes.RPM)}.rpm"
                self.rpm_package_files.append(rpm_package_file_name)
                self.file_to_type[rpm_package_file_name] = package_type

                tgz_package_file_name = f"{package}-{self.version}-{self._get_arch_suffix(package_type, RepoTypes.TGZ)}.tgz"
                self.tgz_package_files.append(tgz_package_file_name)
                self.file_to_type[tgz_package_file_name] = package_type
                tgz_package_file_name += ".sha512"
                self.tgz_package_files.append(tgz_package_file_name)
                self.file_to_type[tgz_package_file_name] = package_type

    def get_deb_packages_files(self):
        return self.deb_package_files

    def get_rpm_packages_files(self):
        return self.rpm_package_files

    def get_tgz_packages_files(self):
        return self.tgz_package_files

    def get_macos_packages_files(self):
        return self.macos_package_files

    def get_packages_names(self):
        return self.package_names

    def get_all_packages_files(self):
        assert self.local_tgz_packages_ready()
        assert self.local_deb_packages_ready()
        assert self.local_rpm_packages_ready()
        assert self.local_macos_packages_ready()
        res = []
        for package_file in (
            self.deb_package_files
            + self.rpm_package_files
            + self.tgz_package_files
            + self.macos_package_files
        ):
            res.append(self.LOCAL_DIR + "/" + package_file)
        return res

    def run(self):
        Shell.check(f"rm -rf {self.LOCAL_DIR}/*")
        for package_file in (
            self.deb_package_files + self.rpm_package_files + self.tgz_package_files
        ):
            print(f"Downloading: [{package_file}]")
            s3_path = "/".join(
                [
                    self.release,
                    self.commit_sha,
                    self.file_to_type[package_file],
                    package_file,
                ]
            )
            self.s3.download_file(
                bucket=CI.Envs.S3_BUILDS_BUCKET,
                s3_path=s3_path,
                local_file_path="/".join([self.LOCAL_DIR, package_file]),
            )

        for macos_package, bin_suffix in self.MACOS_PACKAGE_TO_BIN_SUFFIX.items():
            binary_name = "clickhouse"
            destination_binary_name = f"{binary_name}-{bin_suffix}"
            assert destination_binary_name in self.macos_package_files
            print(
                f"Downloading: [{macos_package}] binary to [{destination_binary_name}]"
            )
            s3_path = "/".join(
                [
                    self.release,
                    self.commit_sha,
                    macos_package,
                    binary_name,
                ]
            )
            self.s3.download_file(
                bucket=CI.Envs.S3_BUILDS_BUCKET,
                s3_path=s3_path,
                local_file_path="/".join([self.LOCAL_DIR, destination_binary_name]),
            )

    def local_deb_packages_ready(self) -> bool:
        assert self.deb_package_files
        for package_file in self.deb_package_files:
            print(f"Check package is downloaded [{package_file}]")
            if not Path(self.LOCAL_DIR + "/" + package_file).is_file():
                return False
        return True

    def local_rpm_packages_ready(self) -> bool:
        assert self.rpm_package_files
        for package_file in self.rpm_package_files:
            print(f"Check package is downloaded [{package_file}]")
            if not Path(self.LOCAL_DIR + "/" + package_file).is_file():
                return False
        return True

    def local_tgz_packages_ready(self) -> bool:
        assert self.tgz_package_files
        for package_file in self.tgz_package_files:
            print(f"Check package is downloaded [{package_file}]")
            if not Path(self.LOCAL_DIR + "/" + package_file).is_file():
                return False
        return True

    def local_macos_packages_ready(self) -> bool:
        assert self.macos_package_files
        for package_file in self.macos_package_files:
            print(f"Check package is downloaded [{package_file}]")
            if not Path(self.LOCAL_DIR + "/" + package_file).is_file():
                return False
        return True


@contextmanager
def checkout(ref: str) -> Iterator[None]:
    orig_ref = Shell.get_output_or_raise(f"{GIT_PREFIX} symbolic-ref --short HEAD")
    rollback_cmd = f"{GIT_PREFIX} checkout {orig_ref}"
    assert orig_ref
    if ref not in (orig_ref,):
        Shell.check(f"{GIT_PREFIX} checkout {ref}", strict=True, verbose=True)
    try:
        yield
    except (Exception, KeyboardInterrupt) as e:
        print(f"ERROR: Exception [{e}]")
        Shell.check(rollback_cmd, verbose=True)
        raise
    Shell.check(rollback_cmd, verbose=True)


@contextmanager
def checkout_new(ref: str) -> Iterator[None]:
    orig_ref = Shell.get_output_or_raise(f"{GIT_PREFIX} symbolic-ref --short HEAD")
    rollback_cmd = f"{GIT_PREFIX} checkout {orig_ref}"
    assert orig_ref
    Shell.check(f"{GIT_PREFIX} checkout -b {ref}", strict=True, verbose=True)
    try:
        yield
    except (Exception, KeyboardInterrupt) as e:
        print(f"ERROR: Exception [{e}]")
        Shell.check(rollback_cmd, verbose=True)
        raise
    Shell.check(rollback_cmd, verbose=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="Creates release",
    )
    parser.add_argument(
        "--prepare-release-info",
        action="store_true",
        help="Initial step to prepare info like release branch, release tag, etc.",
    )
    parser.add_argument(
        "--skip-tag-check",
        action="store_true",
        help="To skip check against latest git tag on a release branch",
    )
    parser.add_argument(
        "--push-release-tag",
        action="store_true",
        help="Creates and pushes git tag",
    )
    parser.add_argument(
        "--push-new-release-branch",
        action="store_true",
        help="Creates and pushes new release branch and corresponding service gh tags for backports",
    )
    parser.add_argument(
        "--create-bump-version-pr",
        action="store_true",
        help="Updates version, contributors' list and creates PR",
    )
    parser.add_argument(
        "--download-packages",
        action="store_true",
        help="Downloads all required packages from s3",
    )
    parser.add_argument(
        "--create-gh-release",
        action="store_true",
        help="Create GH Release object and attach all packages",
    )
    parser.add_argument(
        "--merge-prs",
        action="store_true",
        help="Merge PRs with version, changelog updates",
    )
    parser.add_argument(
        "--post-status",
        action="store_true",
        help="Post release status into Slack",
    )
    parser.add_argument(
        "--ref",
        type=str,
        help="the commit hash or branch",
    )
    parser.add_argument(
        "--release-type",
        choices=("new", "patch"),
        # dest="release_type",
        help="a release type to bump the major.minor.patch version part, "
        "new branch is created only for the value 'new'",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="do not make any actual changes in the repo, just show what will be done",
    )
    parser.add_argument(
        "--set-progress-started",
        action="store_true",
        help="Set new progress step, --progress <PROGRESS STEP> must be set",
    )
    parser.add_argument(
        "--progress",
        type=str,
        help="Progress step name, see @ReleaseProgress",
    )
    parser.add_argument(
        "--set-progress-completed",
        action="store_true",
        help="Set current progress step to OK (completed)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    # prepare ssh for git if needed
    _ssh_agent = None
    _key_pub = None
    if os.getenv("ROBOT_CLICKHOUSE_SSH_KEY", ""):
        _key = os.getenv("ROBOT_CLICKHOUSE_SSH_KEY")
        _ssh_agent = SSHAgent()
        _key_pub = _ssh_agent.add(_key)
        _ssh_agent.print_keys()

    if args.prepare_release_info:
        with ReleaseContextManager(
            release_progress=ReleaseProgress.STARTED
        ) as release_info:
            assert (
                args.ref and args.release_type
            ), "--ref and --release-type must be provided with --prepare-release-info"
            release_info.prepare(
                commit_ref=args.ref,
                release_type=args.release_type,
                skip_tag_check=args.skip_tag_check,
            )

    if args.download_packages:
        with ReleaseContextManager(
            release_progress=ReleaseProgress.DOWNLOAD_PACKAGES
        ) as release_info:
            p = PackageDownloader(
                release=release_info.release_branch,
                commit_sha=release_info.commit_sha,
                version=release_info.version,
            )
            p.run()

    if args.push_release_tag:
        with ReleaseContextManager(
            release_progress=ReleaseProgress.PUSH_RELEASE_TAG
        ) as release_info:
            release_info.push_release_tag(dry_run=args.dry_run)

    if args.push_new_release_branch:
        with ReleaseContextManager(
            release_progress=ReleaseProgress.PUSH_NEW_RELEASE_BRANCH
        ) as release_info:
            release_info.push_new_release_branch(dry_run=args.dry_run)

    if args.create_bump_version_pr:
        with ReleaseContextManager(
            release_progress=ReleaseProgress.BUMP_VERSION
        ) as release_info:
            release_info.update_version_and_contributors_list(dry_run=args.dry_run)

    if args.create_gh_release:
        with ReleaseContextManager(
            release_progress=ReleaseProgress.CREATE_GH_RELEASE
        ) as release_info:
            p = PackageDownloader(
                release=release_info.release_branch,
                commit_sha=release_info.commit_sha,
                version=release_info.version,
            )
            release_info.create_gh_release(
                packages_files=p.get_all_packages_files(), dry_run=args.dry_run
            )

    if args.post_status:
        release_info = ReleaseInfo.from_file()
        if release_info.is_new_release_branch():
            title = "New release branch"
        else:
            title = "New release"
        if (
            release_info.progress_status == ReleaseProgressDescription.OK
            and release_info.release_progress == ReleaseProgress.COMPLETED
        ):
            title = "Completed: " + title
            CIBuddy(dry_run=args.dry_run).post_done(
                title, dataclasses.asdict(release_info)
            )
        else:
            title = "Failed: " + title
            CIBuddy(dry_run=args.dry_run).post_critical(
                title,
                dataclasses.asdict(release_info),
                channels=[CIBuddy.Channels.ALERTS, CIBuddy.Channels.INFO],
            )

    if args.set_progress_started:
        ri = ReleaseInfo.from_file()
        ri.release_progress = args.progress
        ri.progress_status = ReleaseProgressDescription.FAILED
        ri.dump()
        assert args.progress, "Progress step name must be provided"

    if args.set_progress_completed:
        ri = ReleaseInfo.from_file()
        assert (
            ri.progress_status == ReleaseProgressDescription.FAILED
        ), "Must be FAILED before set to OK"
        ri.progress_status = ReleaseProgressDescription.OK
        ri.dump()

    if args.merge_prs:
        with ReleaseContextManager(
            release_progress=ReleaseProgress.MERGE_CREATED_PRS
        ) as release_info:
            release_info.update_release_info(dry_run=args.dry_run)
            release_info.merge_prs(dry_run=args.dry_run)

    # tear down ssh
    if _ssh_agent and _key_pub:
        _ssh_agent.remove(_key_pub)


"""
Prepare release machine:

### INSTALL PACKAGES
sudo apt update
sudo apt install --yes --no-install-recommends python3-dev python3-pip gh unzip
sudo apt install --yes python3-boto3
sudo apt install --yes python3-github
sudo apt install --yes python3-unidiff
sudo apt install --yes python3-tqdm # cloud changelog
sudo apt install --yes python3-thefuzz # cloud changelog
sudo apt install --yes s3fs

### INSTALL AWS CLI
cd /tmp
curl "https://awscli.amazonaws.com/awscli-exe-linux-$(uname -m).zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install
rm -rf aws*
cd -

### INSTALL GH ACTIONS RUNNER:
# Create a folder
RUNNER_VERSION=2.317.0
cd ~
mkdir actions-runner && cd actions-runner
# Download the latest runner package
runner_arch() {
  case $(uname -m) in
    x86_64 )
      echo x64;;
    aarch64 )
      echo arm64;;
  esac
}
curl -O -L https://github.com/actions/runner/releases/download/v$RUNNER_VERSION/actions-runner-linux-$(runner_arch)-$RUNNER_VERSION.tar.gz
# Extract the installer
tar xzf ./actions-runner-linux-$(runner_arch)-$RUNNER_VERSION.tar.gz
rm ./actions-runner-linux-$(runner_arch)-$RUNNER_VERSION.tar.gz

### Install reprepro:
cd ~
sudo apt install dpkg-dev libgpgme-dev libdb-dev libbz2-dev liblzma-dev libarchive-dev shunit2 db-util debhelper
git clone https://salsa.debian.org/debian/reprepro.git
cd reprepro
dpkg-buildpackage -b --no-sign && sudo dpkg -i ../reprepro_$(dpkg-parsechangelog --show-field Version)_$(dpkg-architecture -q DEB_HOST_ARCH).deb

### Install createrepo-c:
sudo apt install createrepo-c
createrepo_c --version
#Version: 0.17.3 (Features: DeltaRPM LegacyWeakdeps )

### Import gpg sign key
gpg --import key.pgp
gpg --list-secret-keys

### Install docker
sudo su; cd ~

deb_arch() {
  case $(uname -m) in
    x86_64 )
      echo amd64;;
    aarch64 )
      echo arm64;;
  esac
}
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg

echo "deb [arch=$(deb_arch) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt-get update
sudo apt-get install --yes --no-install-recommends docker-ce docker-buildx-plugin docker-ce-cli containerd.io

sudo usermod -aG docker ubuntu

# enable ipv6 in containers (fixed-cidr-v6 is some random network mask)
cat <<EOT > /etc/docker/daemon.json
{
  "ipv6": true,
  "fixed-cidr-v6": "2001:db8:1::/64",
  "log-driver": "json-file",
  "log-opts": {
    "max-file": "5",
    "max-size": "1000m"
  },
  "insecure-registries" : ["dockerhub-proxy.dockerhub-proxy-zone:5000"],
  "registry-mirrors" : ["http://dockerhub-proxy.dockerhub-proxy-zone:5000"]
}
EOT

# if docker build does not work:
    sudo systemctl restart docker
    docker buildx rm mybuilder
    docker buildx create --name mybuilder --driver docker-container --use
    docker buildx inspect mybuilder --bootstrap

### Install tailscale

### Configure GH runner
"""
