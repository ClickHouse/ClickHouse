import argparse
import time
from pathlib import Path
from typing import Optional
from shutil import copy2
from create_release import (
    PackageDownloader,
    ReleaseInfo,
    ReleaseContextManager,
    ReleaseProgress,
)
from ci_utils import WithIter, Shell


class MountPointApp(metaclass=WithIter):
    RCLONE = "rclone"
    S3FS = "s3fs"


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
    # TODO: which mode is better: minimal/writes/full/off
    _RCLONE_CACHE_MODE = "minimal"
    UMASK = "0000"

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
            # without -o nomultipart there are errors like "Error 5 writing to /home/ubuntu/***.deb: Input/output error"
            self.mount_cmd = f"s3fs {self.bucket_name} {self.MOUNT_POINT} -o url={self.API_ENDPOINT} -o use_path_request_style -o umask=0000 -o nomultipart -o logfile={self.LOG_FILE} {self.aux_mount_options}"
        elif self.app == MountPointApp.RCLONE:
            # run rclone mount process asynchronously, otherwise subprocess.run(daemonized command) will not return
            self.cache_dir = "/home/ubuntu/rclone_cache"
            self.aux_mount_options += "--no-modtime " if self.NOMODTIME else ""
            self.aux_mount_options += "-v " if self.DEBUG else ""  # -vv too verbose
            self.aux_mount_options += (
                f"--vfs-cache-mode {self._RCLONE_CACHE_MODE} --vfs-cache-max-size {self._CACHE_MAX_SIZE_GB}G"
                if self.CACHE_ENABLED
                else "--vfs-cache-mode off"
            )
            # Use --no-modtime to try to avoid: ERROR : rpm/lts/clickhouse-client-24.3.6.5.x86_64.rpm: Failed to apply pending mod time
            self.mount_cmd = f"rclone mount remote:{self.bucket_name} {self.MOUNT_POINT} --daemon --cache-dir {self.cache_dir} --umask 0000 --log-file {self.LOG_FILE} {self.aux_mount_options}"
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
        Shell.run(_CLEAN_LOG_FILE_CMD)
        Shell.run(_UNMOUNT_CMD)
        Shell.run(_MKDIR_CMD)
        Shell.run(_MKDIR_FOR_CACHE)
        if self.app == MountPointApp.S3FS:
            Shell.run(self.mount_cmd, check=True)
        else:
            # didn't manage to use simple run() and without blocking or failure
            Shell.run_as_daemon(self.mount_cmd)
        time.sleep(3)
        Shell.run(_TEST_MOUNT_CMD, check=True)

    @classmethod
    def teardown(cls):
        print(f"Unmount [{cls.MOUNT_POINT}]")
        Shell.run(f"umount {cls.MOUNT_POINT}")


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
        print("Running export command:")
        print(f"  {cmd}")
        Shell.run(cmd, check=True)
        Shell.run("sync")

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
            Shell.run(cmd, check=True)
            Shell.run("sync")

    def test_packages(self):
        Shell.run("docker pull ubuntu:latest")
        print(f"Test packages installation, version [{self.version}]")
        debian_command = f"echo 'deb {self.repo_url} stable main' | tee /etc/apt/sources.list.d/clickhouse.list; apt update -y; apt-get install -y clickhouse-common-static={self.version} clickhouse-client={self.version}"
        cmd = f'docker run --rm ubuntu:latest bash -c "apt update -y; apt install -y sudo gnupg ca-certificates; apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 8919F6BD2B48D754; {debian_command}"'
        print("Running test command:")
        print(f"  {cmd}")
        Shell.run(cmd, check=True)
        self.release_info.debian_command = debian_command
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

        commands = (
            f"createrepo_c --local-sqlite --workers=2 --update --verbose {dest_dir}",
            f"gpg --sign-with {self._SIGN_KEY} --detach-sign --batch --yes --armor {dest_dir / 'repodata' / 'repomd.xml'}",
        )
        print(f"Exporting RPM packages into [{codename}]")

        for command in commands:
            print("Running command:")
            print(f"    {command}")
            Shell.run(command, check=True)

        update_public_key = f"gpg --armor --export {self._SIGN_KEY}"
        pub_key_path = dest_dir / "repodata" / "repomd.xml.key"
        print("Updating repomd.xml.key")
        pub_key_path.write_text(Shell.run(update_public_key, check=True))
        if codename == RepoCodenames.LTS:
            self.export_packages(RepoCodenames.STABLE)
        Shell.run("sync")

    def test_packages(self):
        Shell.run("docker pull fedora:latest")
        print(f"Test package installation, version [{self.version}]")
        rpm_command = f"dnf config-manager --add-repo={self.repo_url} && dnf makecache && dnf -y install clickhouse-client-{self.version}-1"
        cmd = f'docker run --rm fedora:latest /bin/bash -c "dnf -y install dnf-plugins-core && dnf config-manager --add-repo={self.repo_url} && {rpm_command}"'
        print("Running test command:")
        print(f"  {cmd}")
        Shell.run(cmd, check=True)
        self.release_info.rpm_command = rpm_command
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
        Shell.run("sync")

    def test_packages(self):
        tgz_file = "/tmp/tmp.tgz"
        tgz_sha_file = "/tmp/tmp.tgz.sha512"
        cmd = f"curl -o {tgz_file} -f0 {self.repo_url}/stable/clickhouse-client-{self.version}-arm64.tgz"
        Shell.run(
            cmd,
            check=True,
        )
        Shell.run(
            f"curl -o {tgz_sha_file} -f0 {self.repo_url}/stable/clickhouse-client-{self.version}-arm64.tgz.sha512",
            check=True,
        )
        expected_checksum = Shell.run(f"cut -d ' ' -f 1 {tgz_sha_file}", check=True)
        actual_checksum = Shell.run(f"sha512sum {tgz_file} | cut -d ' ' -f 1")
        assert (
            expected_checksum == actual_checksum
        ), f"[{actual_checksum} != {expected_checksum}]"
        Shell.run("rm /tmp/tmp.tgz*")
        self.release_info.tgz_command = cmd
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
    Use S3FS. RCLONE has some errors with r2 remote which I didn't figure out how to resolve:
           ERROR : IO error: NotImplemented: versionId not implemented
           Failed to copy: NotImplemented: versionId not implemented
    """
    mp = R2MountPoint(MountPointApp.S3FS, dry_run=args.dry_run)
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
