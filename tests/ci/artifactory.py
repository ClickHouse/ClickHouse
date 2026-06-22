import argparse
import time
from pathlib import Path
from shutil import copy2
from typing import Optional

from ci_utils import Shell, WithIter
from create_release import (
    PackageDownloader,
    ReleaseContextManager,
    ReleaseInfo,
    ReleaseProgress,
)


class MountPointApp(metaclass=WithIter):
    S3FS = "s3fs"
    GEESEFS = "geesefs"


class R2MountPoint:
    _TEST_BUCKET_NAME = "repo-test"
    _PROD_BUCKET_NAME = "packages"
    _CACHE_MAX_SIZE_GB = 20
    MOUNT_POINT = "/home/ubuntu/mountpoint"
    API_ENDPOINT = "https://d4fd593eebab2e3a58a599400c4cd64d.r2.cloudflarestorage.com"
    LOG_FILE = "/home/ubuntu/fuse_mount.log"
    # mod time is not required by reprepro and createrepo - disable to simplify bucket's mount sync (applicable fro rclone)
    NOMODTIME = True
    # enable debug messages in mount log
    DEBUG = True
    # enable cache for mountpoint
    CACHE_ENABLED = False

    def __init__(self, app: str, dry_run: bool) -> None:
        assert app in MountPointApp
        self.app = app
        if dry_run:
            self.bucket_name = self._TEST_BUCKET_NAME
        else:
            self.bucket_name = self._PROD_BUCKET_NAME

        self.aux_mount_options = ""
        if self.app == MountPointApp.S3FS:
            self.cache_dir = "/home/ubuntu/s3fs_cache"
            # self.aux_mount_options += "-o nomodtime " if self.NOMODTIME else "" not for s3fs
            self.aux_mount_options += "--debug " if self.DEBUG else ""
            self.aux_mount_options += (
                f"-o use_cache={self.cache_dir} -o cache_size_mb={self._CACHE_MAX_SIZE_GB * 1024} "
                if self.CACHE_ENABLED
                else ""
            )
            if not dry_run:
                self.aux_mount_options += (
                    "-o passwd_file /home/ubuntu/.passwd-s3fs_packages "
                )
            # without -o nomultipart there are errors like "Error 5 writing to /home/ubuntu/***.deb: Input/output error"
            self.mount_cmd = (
                f"s3fs {self.bucket_name} {self.MOUNT_POINT} -o url={self.API_ENDPOINT} "
                f"-o use_path_request_style -o umask=0000 -o nomultipart "
                f"-o logfile={self.LOG_FILE} {self.aux_mount_options}"
            )
        elif self.app == MountPointApp.GEESEFS:
            self.cache_dir = "/home/ubuntu/geesefs_cache"
            self.aux_mount_options += (
                f" --cache={self.cache_dir} " if self.CACHE_ENABLED else ""
            )
            if not dry_run:
                self.aux_mount_options += " --shared-config=/home/ubuntu/.r2_auth "
            else:
                self.aux_mount_options += " --shared-config=/home/ubuntu/.r2_auth_test "
            if self.DEBUG:
                self.aux_mount_options += " --debug_s3 "
            self.mount_cmd = (
                f"geesefs --endpoint={self.API_ENDPOINT} --cheap --memory-limit=1000 "
                f"--gc-interval=100 --max-flushers=10 --max-parallel-parts=1 --max-parallel-copy=10 "
                f"--log-file={self.LOG_FILE} {self.aux_mount_options} {self.bucket_name} {self.MOUNT_POINT}"
            )
        else:
            assert False

    def init(self):
        print(f"Mount bucket [{self.bucket_name}] to [{self.MOUNT_POINT}]")
        _CLEAN_LOG_FILE_CMD = f"tail -n 1000 {self.LOG_FILE} > {self.LOG_FILE}_tmp && mv {self.LOG_FILE}_tmp {self.LOG_FILE} ||:"
        _MKDIR_CMD = f"mkdir -p {self.MOUNT_POINT}"
        _MKDIR_FOR_CACHE = f"mkdir -p {self.cache_dir}"
        _UNMOUNT_CMD = (
            f"mount | grep -q {self.MOUNT_POINT} && umount {self.MOUNT_POINT} ||:"
        )

        _TEST_MOUNT_CMD = f"mount | grep -q {self.MOUNT_POINT}"
        Shell.check(_CLEAN_LOG_FILE_CMD, verbose=True)
        Shell.check(_UNMOUNT_CMD, verbose=True)
        Shell.check(_MKDIR_CMD, verbose=True)
        Shell.check(_MKDIR_FOR_CACHE, verbose=True)
        Shell.check(self.mount_cmd, strict=True, verbose=True)
        time.sleep(3)
        Shell.check(_TEST_MOUNT_CMD, strict=True, verbose=True)

    @classmethod
    def teardown(cls):
        Shell.check(f"umount {cls.MOUNT_POINT}", verbose=True)


class RepoCodenames(metaclass=WithIter):
    LTS = "lts"
    STABLE = "stable"


class DebianArtifactory:
    _TEST_REPO_URL = "https://pub-73dd1910f4284a81a02a67018967e028.r2.dev/deb"
    _PROD_REPO_URL = "https://packages.clickhouse.com/deb"

    def __init__(self, release_info: ReleaseInfo, dry_run: bool):
        self.release_info = release_info
        self.codename = release_info.codename
        self.version = release_info.version
        if dry_run:
            self.repo_url = self._TEST_REPO_URL
        else:
            self.repo_url = self._PROD_REPO_URL
        assert self.codename in RepoCodenames
        self.pd = PackageDownloader(
            release=release_info.release_branch,
            commit_sha=release_info.commit_sha,
            version=release_info.version,
        )

    def export_packages(self):
        assert self.pd.local_deb_packages_ready(), "BUG: Packages are not downloaded"
        print("Start adding packages")
        paths = [
            self.pd.LOCAL_DIR + "/" + file for file in self.pd.get_deb_packages_files()
        ]
        REPREPRO_CMD_PREFIX = f"reprepro --basedir {R2MountPoint.MOUNT_POINT}/configs/deb --outdir {R2MountPoint.MOUNT_POINT}/deb --verbose"
        cmd = f"{REPREPRO_CMD_PREFIX} includedeb {self.codename} {' '.join(paths)}"
        print("Running export commands:")
        Shell.check(cmd, strict=True, verbose=True)
        Shell.check("sync")

        if self.codename == RepoCodenames.LTS:
            packages_with_version = [
                package + "=" + self.version for package in self.pd.get_packages_names()
            ]
            print(
                f"Copy packages from {RepoCodenames.LTS} to {RepoCodenames.STABLE} repository"
            )
            cmd = f"{REPREPRO_CMD_PREFIX} copy {RepoCodenames.STABLE} {RepoCodenames.LTS} {' '.join(packages_with_version)}"
            print("Running copy command:")
            print(f"  {cmd}")
            Shell.check(cmd, strict=True)
            Shell.check("sync")
        time.sleep(10)
        Shell.check("lsof +D R2MountPoint.MOUNT_POINT", verbose=True)

    def test_packages(self):
        Shell.check("docker pull ubuntu:latest", strict=True)
        print(f"Test packages installation, version [{self.version}]")
        debian_command = (
            f"echo 'deb {self.repo_url} stable main' | "
            "tee /etc/apt/sources.list.d/clickhouse.list; apt update -y; "
            f"apt-get install -y clickhouse-common-static={self.version} clickhouse-client={self.version}"
        )
        cmd = (
            "docker run --rm ubuntu:latest bash -c "
            f'"apt update -y; apt install -y sudo gnupg ca-certificates; apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 8919F6BD2B48D754; {debian_command}"'
        )
        print("Running test command:")
        print(f"  {cmd}")
        assert Shell.check(cmd)
        print("Test packages installation, version [latest]")
        debian_command_2 = (
            f"echo 'deb {self.repo_url} stable main' | "
            "tee /etc/apt/sources.list.d/clickhouse.list; apt update -y; apt-get install -y clickhouse-common-static clickhouse-client"
        )
        cmd = (
            "docker run --rm ubuntu:latest bash -c "
            f'"apt update -y; apt install -y sudo gnupg ca-certificates; apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 8919F6BD2B48D754; {debian_command_2}"'
        )
        print("Running test command:")
        print(f"  {cmd}")
        assert Shell.check(cmd)
        self.release_info.debian = debian_command
        self.release_info.dump()


def _copy_if_not_exists(src: Path, dst: Path) -> Path:
    if dst.is_dir():
        dst = dst / src.name
    if not dst.exists():
        return copy2(src, dst)  # type: ignore
    if src.stat().st_size == dst.stat().st_size:
        return dst
    return copy2(src, dst)  # type: ignore


class RpmArtifactory:
    _TEST_REPO_URL = (
        "https://pub-73dd1910f4284a81a02a67018967e028.r2.dev/rpm/clickhouse.repo"
    )
    _PROD_REPO_URL = "https://packages.clickhouse.com/rpm/clickhouse.repo"
    _SIGN_KEY = "885E2BDCF96B0B45ABF058453E4AD4719DDE9A38"
    FEDORA_VERSION = 40

    def __init__(self, release_info: ReleaseInfo, dry_run: bool):
        self.release_info = release_info
        self.codename = release_info.codename
        self.version = release_info.version
        if dry_run:
            self.repo_url = self._TEST_REPO_URL
        else:
            self.repo_url = self._PROD_REPO_URL
        assert self.codename in RepoCodenames
        self.pd = PackageDownloader(
            release=release_info.release_branch,
            commit_sha=release_info.commit_sha,
            version=release_info.version,
        )

    def export_packages(self, codename: Optional[str] = None) -> None:
        assert self.pd.local_rpm_packages_ready(), "BUG: Packages are not downloaded"
        codename = codename or self.codename
        print(f"Start adding packages to [{codename}]")
        paths = [
            self.pd.LOCAL_DIR + "/" + file for file in self.pd.get_rpm_packages_files()
        ]

        dest_dir = Path(R2MountPoint.MOUNT_POINT) / "rpm" / codename

        for package in paths:
            _copy_if_not_exists(Path(package), dest_dir)

        # switching between different fuse providers invalidates --update option (apparently some fuse(s) can mess around with mtime)
        #   add --skip-stat to skip mtime check
        commands = (
            f"createrepo_c --local-sqlite --workers=2 --update --skip-stat --verbose {dest_dir}",
            f"gpg --sign-with {self._SIGN_KEY} --detach-sign --batch --yes --armor {dest_dir / 'repodata' / 'repomd.xml'}",
        )
        print(f"Exporting RPM packages into [{codename}]")

        for command in commands:
            Shell.check(command, strict=True, verbose=True)

        update_public_key = f"gpg --armor --export {self._SIGN_KEY}"
        pub_key_path = dest_dir / "repodata" / "repomd.xml.key"
        print("Updating repomd.xml.key")
        pub_key_path.write_text(Shell.get_output_or_raise(update_public_key))
        if codename == RepoCodenames.LTS:
            self.export_packages(RepoCodenames.STABLE)
        Shell.check("sync")

    def test_packages(self):
        Shell.check(f"docker pull fedora:{self.FEDORA_VERSION}", strict=True)
        print(f"Test package installation, version [{self.version}]")
        rpm_command = f"dnf config-manager --add-repo={self.repo_url} && dnf makecache && dnf -y install clickhouse-client-{self.version}-1"
        cmd = f'docker run --rm fedora:{self.FEDORA_VERSION} /bin/bash -c "dnf -y install dnf-plugins-core && dnf config-manager --add-repo={self.repo_url} && {rpm_command}"'
        print("Running test command:")
        print(f"  {cmd}")
        assert Shell.check(cmd)
        print("Test package installation, version [latest]")
        rpm_command_2 = f"dnf config-manager --add-repo={self.repo_url} && dnf makecache && dnf -y install clickhouse-client"
        cmd = f'docker run --rm fedora:{self.FEDORA_VERSION} /bin/bash -c "dnf -y install dnf-plugins-core && dnf config-manager --add-repo={self.repo_url} && {rpm_command_2}"'
        print("Running test command:")
        print(f"  {cmd}")
        assert Shell.check(cmd)
        self.release_info.rpm = rpm_command
        self.release_info.dump()


class TgzArtifactory:
    _TEST_REPO_URL = "https://pub-73dd1910f4284a81a02a67018967e028.r2.dev/tgz"
    _PROD_REPO_URL = "https://packages.clickhouse.com/tgz"

    def __init__(self, release_info: ReleaseInfo, dry_run: bool):
        self.release_info = release_info
        self.codename = release_info.codename
        self.version = release_info.version
        if dry_run:
            self.repo_url = self._TEST_REPO_URL
        else:
            self.repo_url = self._PROD_REPO_URL
        assert self.codename in RepoCodenames
        self.pd = PackageDownloader(
            release=release_info.release_branch,
            commit_sha=release_info.commit_sha,
            version=release_info.version,
        )

    def export_packages(self, codename: Optional[str] = None) -> None:
        assert self.pd.local_tgz_packages_ready(), "BUG: Packages are not downloaded"
        codename = codename or self.codename

        paths = [
            self.pd.LOCAL_DIR + "/" + file for file in self.pd.get_tgz_packages_files()
        ]

        dest_dir = Path(R2MountPoint.MOUNT_POINT) / "tgz" / codename

        print(f"Exporting TGZ packages into [{codename}]")

        for package in paths:
            _copy_if_not_exists(Path(package), dest_dir)

        if codename == RepoCodenames.LTS:
            self.export_packages(RepoCodenames.STABLE)
        Shell.check("sync")

    def test_packages(self):
        tgz_file = "/tmp/tmp.tgz"
        tgz_sha_file = "/tmp/tmp.tgz.sha512"
        cmd = f"curl -o {tgz_file} -f0 {self.repo_url}/stable/clickhouse-client-{self.version}-arm64.tgz"
        Shell.check(
            cmd,
            strict=True,
            verbose=True,
        )
        Shell.check(
            f"curl -o {tgz_sha_file} -f0 {self.repo_url}/stable/clickhouse-client-{self.version}-arm64.tgz.sha512",
            strict=True,
            verbose=True,
        )
        expected_checksum = Shell.get_output_or_raise(f"cut -d ' ' -f 1 {tgz_sha_file}")
        actual_checksum = Shell.get_output_or_raise(
            f"sha512sum {tgz_file} | cut -d ' ' -f 1"
        )
        assert (
            expected_checksum == actual_checksum
        ), f"[{actual_checksum} != {expected_checksum}]"
        Shell.check("rm /tmp/tmp.tgz*", verbose=True)
        self.release_info.tgz = cmd
        self.release_info.dump()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="Adds release packages to the repository",
    )
    parser.add_argument(
        "--export-debian",
        action="store_true",
        help="Export debian packages to repository",
    )
    parser.add_argument(
        "--export-rpm",
        action="store_true",
        help="Export rpm packages to repository",
    )
    parser.add_argument(
        "--export-tgz",
        action="store_true",
        help="Export tgz packages to repository",
    )
    parser.add_argument(
        "--test-debian",
        action="store_true",
        help="Test debian packages installation",
    )
    parser.add_argument(
        "--test-rpm",
        action="store_true",
        help="Test rpm packages installation",
    )
    parser.add_argument(
        "--test-tgz",
        action="store_true",
        help="Test tgz packages installation",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Dry run mode",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    """
    S3FS - very slow with a big repo
    RCLONE - fuse had many different errors with r2 remote and completely removed
    GEESEFS ?
    """
    mp = R2MountPoint(MountPointApp.GEESEFS, dry_run=args.dry_run)
    if args.export_debian:
        with ReleaseContextManager(
            release_progress=ReleaseProgress.EXPORT_DEB
        ) as release_info:
            mp.init()
            DebianArtifactory(release_info, dry_run=args.dry_run).export_packages()
            mp.teardown()
    if args.export_rpm:
        with ReleaseContextManager(
            release_progress=ReleaseProgress.EXPORT_RPM
        ) as release_info:
            mp.init()
            RpmArtifactory(release_info, dry_run=args.dry_run).export_packages()
            mp.teardown()
    if args.export_tgz:
        with ReleaseContextManager(
            release_progress=ReleaseProgress.EXPORT_TGZ
        ) as release_info:
            mp.init()
            TgzArtifactory(release_info, dry_run=args.dry_run).export_packages()
            mp.teardown()
    if args.test_debian:
        with ReleaseContextManager(
            release_progress=ReleaseProgress.TEST_DEB
        ) as release_info:
            DebianArtifactory(release_info, dry_run=args.dry_run).test_packages()
    if args.test_tgz:
        with ReleaseContextManager(
            release_progress=ReleaseProgress.TEST_TGZ
        ) as release_info:
            TgzArtifactory(release_info, dry_run=args.dry_run).test_packages()
    if args.test_rpm:
        with ReleaseContextManager(
            release_progress=ReleaseProgress.TEST_RPM
        ) as release_info:
            RpmArtifactory(release_info, dry_run=args.dry_run).test_packages()
