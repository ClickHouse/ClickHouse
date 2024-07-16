import argparse
import dataclasses
import json
import os
import subprocess

from contextlib import contextmanager
from copy import copy
from pathlib import Path
from typing import Iterator, List

from git_helper import Git, GIT_PREFIX
from ssh import SSHAgent
from env_helper import GITHUB_REPOSITORY, S3_BUILDS_BUCKET
from s3_helper import S3Helper
from autoscale_runners_lambda.lambda_shared.pr import Labels
from ci_utils import Shell
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


class ShellRunner:

    @classmethod
    def run(
        cls, command, check_retcode=True, print_output=True, async_=False, dry_run=False
    ):
        if dry_run:
            print(f"Dry-run: Would run shell command: [{command}]")
            return 0, ""
        print(f"Running shell command: [{command}]")
        if async_:
            subprocess.Popen(command.split(" "))  # pylint:disable=consider-using-with
            return 0, ""
        result = subprocess.run(
            command + " 2>&1",
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True,
        )
        if print_output:
            print(result.stdout)
        if check_retcode:
            assert result.returncode == 0, f"Return code [{result.returncode}]"
        return result.returncode, result.stdout


@dataclasses.dataclass
class ReleaseInfo:
    version: str
    release_tag: str
    release_branch: str
    commit_sha: str
    # lts or stable
    codename: str
    previous_release_tag: str
    previous_release_sha: str

    @staticmethod
    def from_file(file_path: str) -> "ReleaseInfo":
        with open(file_path, "r", encoding="utf-8") as json_file:
            res = json.load(json_file)
        return ReleaseInfo(**res)

    @staticmethod
    def prepare(commit_ref: str, release_type: str, outfile: str) -> None:
        Path(outfile).parent.mkdir(parents=True, exist_ok=True)
        Path(outfile).unlink(missing_ok=True)
        version = None
        release_branch = None
        release_tag = None
        previous_release_tag = None
        previous_release_sha = None
        codename = None
        assert release_type in ("patch", "new")
        if release_type == "new":
            # check commit_ref is right and on a right branch
            ShellRunner.run(
                f"git merge-base --is-ancestor origin/{commit_ref} origin/master"
            )
            with checkout(commit_ref):
                _, commit_sha = ShellRunner.run(f"git rev-parse {commit_ref}")
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
                codename = (
                    VersionType.STABLE
                )  # dummy value (artifactory won't be updated for new release)
                previous_release_tag = expected_prev_tag
                previous_release_sha = Shell.run_strict(
                    f"git rev-parse {previous_release_tag}"
                )
                assert previous_release_sha
        if release_type == "patch":
            with checkout(commit_ref):
                _, commit_sha = ShellRunner.run(f"git rev-parse {commit_ref}")
                # Git() must be inside "with checkout" contextmanager
                git = Git()
                version = get_version_from_repo(git=git)
                codename = version.get_stable_release_type()
                version.with_description(codename)
                release_branch = f"{version.major}.{version.minor}"
                release_tag = version.describe
            ShellRunner.run(f"{GIT_PREFIX} fetch origin {release_branch} --tags")
            # check commit is right and on a right branch
            ShellRunner.run(
                f"git merge-base --is-ancestor {commit_ref} origin/{release_branch}"
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
            else:
                assert (
                    False
                ), f"BUG: Unexpected latest tag [{git.latest_tag}] expected [{expected_tag_prefix}*{expected_tag_suffix}]"

            previous_release_sha = Shell.run_strict(
                f"git rev-parse {previous_release_tag}"
            )
            assert previous_release_sha

        assert (
            release_branch
            and previous_release_tag
            and previous_release_sha
            and commit_sha
            and release_tag
            and version
            and codename in ("lts", "stable")
        )
        res = ReleaseInfo(
            release_branch=release_branch,
            commit_sha=commit_sha,
            release_tag=release_tag,
            version=version.string,
            codename=codename,
            previous_release_tag=previous_release_tag,
            previous_release_sha=previous_release_sha,
        )
        with open(outfile, "w", encoding="utf-8") as f:
            print(json.dumps(dataclasses.asdict(res), indent=2), file=f)

    def push_release_tag(self, dry_run: bool) -> None:
        if dry_run:
            # remove locally created tag from prev run
            ShellRunner.run(
                f"{GIT_PREFIX} tag -l | grep -q {self.release_tag} && git tag -d {self.release_tag} ||:"
            )
        # Create release tag
        print(
            f"Create and push release tag [{self.release_tag}], commit [{self.commit_sha}]"
        )
        tag_message = f"Release {self.release_tag}"
        ShellRunner.run(
            f"{GIT_PREFIX} tag -a -m '{tag_message}' {self.release_tag} {self.commit_sha}"
        )
        cmd_push_tag = f"{GIT_PREFIX} push origin {self.release_tag}:{self.release_tag}"
        ShellRunner.run(cmd_push_tag, dry_run=dry_run)

    @staticmethod
    def _create_gh_label(label: str, color_hex: str, dry_run: bool) -> None:
        cmd = f"gh api repos/{GITHUB_REPOSITORY}/labels -f name={label} -f color={color_hex}"
        ShellRunner.run(cmd, dry_run=dry_run)

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
            ShellRunner.run(
                f"{GIT_PREFIX} branch -l | grep -q {new_release_branch} && git branch -d {new_release_branch} ||:"
            )
        print(
            f"Create and push new release branch [{new_release_branch}], commit [{self.commit_sha}]"
        )
        with checkout(self.release_branch):
            with checkout_new(new_release_branch):
                pr_labels = f"--label {Labels.RELEASE}"
                if stable_release_type == VersionType.LTS:
                    pr_labels += f" --label {Labels.RELEASE_LTS}"
                cmd_push_branch = (
                    f"{GIT_PREFIX} push --set-upstream origin {new_release_branch}"
                )
                ShellRunner.run(cmd_push_branch, dry_run=dry_run)

        print("Create and push backport tags for new release branch")
        ReleaseInfo._create_gh_label(
            f"v{new_release_branch}-must-backport", "10dbed", dry_run=dry_run
        )
        ReleaseInfo._create_gh_label(
            f"v{new_release_branch}-affected", "c2bfff", dry_run=dry_run
        )
        ShellRunner.run(
            f"""gh pr create --repo {GITHUB_REPOSITORY} --title 'Release pull request for branch {new_release_branch}'
            --head {new_release_branch} {pr_labels}
            --body 'This PullRequest is a part of ClickHouse release cycle. It is used by CI system only. Do not perform any changes with it.'
            """,
            dry_run=dry_run,
        )

    def update_version_and_contributors_list(self, dry_run: bool) -> None:
        # Bump version, update contributors list, create PR
        branch_upd_version_contributors = f"bump_version_{self.version}"
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
                body_file = get_abs_path(".github/PULL_REQUEST_TEMPLATE.md")
                actor = os.getenv("GITHUB_ACTOR", "") or "me"
                cmd_create_pr = f"gh pr create --repo {GITHUB_REPOSITORY} --title 'Update version after release' --head {branch_upd_version_contributors} --base {self.release_branch} --body-file '{body_file} --label 'do not test' --assignee @{actor}"
                ShellRunner.run(cmd_commit_version_upd, dry_run=dry_run)
                ShellRunner.run(cmd_push_branch, dry_run=dry_run)
                ShellRunner.run(cmd_create_pr, dry_run=dry_run)
                if dry_run:
                    ShellRunner.run(
                        f"{GIT_PREFIX} diff '{CMAKE_PATH}' '{CONTRIBUTORS_PATH}'"
                    )
                    ShellRunner.run(
                        f"{GIT_PREFIX} checkout '{CMAKE_PATH}' '{CONTRIBUTORS_PATH}'"
                    )

    def create_gh_release(self, packages_files: List[str], dry_run: bool) -> None:
        repo = os.getenv("GITHUB_REPOSITORY")
        assert repo
        cmds = []
        cmds.append(
            f"gh release create --repo {repo} --title 'Release {self.release_tag}' {self.release_tag}"
        )
        for file in packages_files:
            cmds.append(f"gh release upload {self.release_tag} {file}")
        if not dry_run:
            for cmd in cmds:
                ShellRunner.run(cmd)
        else:
            print("Dry-run, would run commands:")
            print("\n  * ".join(cmds))


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

        ShellRunner.run(f"mkdir -p {self.LOCAL_DIR}")

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
        ShellRunner.run(f"rm -rf {self.LOCAL_DIR}/*")
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
                bucket=S3_BUILDS_BUCKET,
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
                bucket=S3_BUILDS_BUCKET,
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
        "--outfile",
        default="",
        type=str,
        help="output file to write json result to, if not set - stdout",
    )
    parser.add_argument(
        "--infile",
        default="",
        type=str,
        help="input file with release info",
    )

    return parser.parse_args()


@contextmanager
def checkout(ref: str) -> Iterator[None]:
    _, orig_ref = ShellRunner.run(f"{GIT_PREFIX} symbolic-ref --short HEAD")
    rollback_cmd = f"{GIT_PREFIX} checkout {orig_ref}"
    assert orig_ref
    if ref not in (orig_ref,):
        ShellRunner.run(f"{GIT_PREFIX} checkout {ref}")
    try:
        yield
    except (Exception, KeyboardInterrupt) as e:
        print(f"ERROR: Exception [{e}]")
        ShellRunner.run(rollback_cmd)
        raise
    ShellRunner.run(rollback_cmd)


@contextmanager
def checkout_new(ref: str) -> Iterator[None]:
    _, orig_ref = ShellRunner.run(f"{GIT_PREFIX} symbolic-ref --short HEAD")
    rollback_cmd = f"{GIT_PREFIX} checkout {orig_ref}"
    assert orig_ref
    ShellRunner.run(f"{GIT_PREFIX} checkout -b {ref}")
    try:
        yield
    except (Exception, KeyboardInterrupt) as e:
        print(f"ERROR: Exception [{e}]")
        ShellRunner.run(rollback_cmd)
        raise
    ShellRunner.run(rollback_cmd)


if __name__ == "__main__":
    args = parse_args()
    assert args.dry_run

    # prepare ssh for git if needed
    _ssh_agent = None
    _key_pub = None
    if os.getenv("ROBOT_CLICKHOUSE_SSH_KEY", ""):
        _key = os.getenv("ROBOT_CLICKHOUSE_SSH_KEY")
        _ssh_agent = SSHAgent()
        _key_pub = _ssh_agent.add(_key)
        _ssh_agent.print_keys()

    if args.prepare_release_info:
        assert (
            args.ref and args.release_type and args.outfile
        ), "--ref, --release-type and --outfile must be provided with --prepare-release-info"
        ReleaseInfo.prepare(
            commit_ref=args.ref, release_type=args.release_type, outfile=args.outfile
        )
    if args.push_release_tag:
        assert args.infile, "--infile <release info file path> must be provided"
        release_info = ReleaseInfo.from_file(args.infile)
        release_info.push_release_tag(dry_run=args.dry_run)
    if args.push_new_release_branch:
        assert args.infile, "--infile <release info file path> must be provided"
        release_info = ReleaseInfo.from_file(args.infile)
        release_info.push_new_release_branch(dry_run=args.dry_run)
    if args.create_bump_version_pr:
        # TODO: store link to PR in release info
        assert args.infile, "--infile <release info file path> must be provided"
        release_info = ReleaseInfo.from_file(args.infile)
        release_info.update_version_and_contributors_list(dry_run=args.dry_run)
    if args.download_packages:
        assert args.infile, "--infile <release info file path> must be provided"
        release_info = ReleaseInfo.from_file(args.infile)
        p = PackageDownloader(
            release=release_info.release_branch,
            commit_sha=release_info.commit_sha,
            version=release_info.version,
        )
        p.run()
    if args.create_gh_release:
        assert args.infile, "--infile <release info file path> must be provided"
        release_info = ReleaseInfo.from_file(args.infile)
        p = PackageDownloader(
            release=release_info.release_branch,
            commit_sha=release_info.commit_sha,
            version=release_info.version,
        )
        release_info.create_gh_release(p.get_all_packages_files(), args.dry_run)

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
