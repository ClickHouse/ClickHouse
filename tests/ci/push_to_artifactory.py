#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
import logging
import os
import re
from collections import namedtuple
from typing import Dict, List, Tuple

from artifactory import ArtifactorySaaSPath  # type: ignore
from build_download_helper import dowload_build_with_progress
from env_helper import RUNNER_TEMP, S3_BUILDS_BUCKET
from git_helper import TAG_REGEXP, commit, removeprefix, removesuffix


# Necessary ENV variables
def getenv(name: str, default: str = None):
    env = os.getenv(name, default)
    if env is not None:
        return env
    raise KeyError(f"Necessary {name} environment is not set")


TEMP_PATH = os.path.join(RUNNER_TEMP, "push_to_artifactory")
# One of the following ENVs is necessary
JFROG_API_KEY = getenv("JFROG_API_KEY", "")
JFROG_TOKEN = getenv("JFROG_TOKEN", "")

CheckDesc = namedtuple("CheckDesc", ("check_name", "deb_arch", "rpm_arch"))


class Packages:
    checks = (
        CheckDesc("package_release", "amd64", "x86_64"),
        CheckDesc("package_aarch64", "arm64", "aarch64"),
    )
    packages = (
        "clickhouse-client",
        "clickhouse-common-static",
        "clickhouse-common-static-dbg",
        "clickhouse-server",
    )

    def __init__(self, version: str):
        # Dicts of name: s3_path_suffix
        self.deb = {}  # type: Dict[str, str]
        self.rpm = {}  # type: Dict[str, str]
        self.tgz = {}  # type: Dict[str, str]
        for check in self.checks:
            for name in self.packages:
                deb = f"{name}_{version}_{check.deb_arch}.deb"
                self.deb[deb] = f"{check.check_name}/{deb}"

                rpm = f"{name}-{version}.{check.rpm_arch}.rpm"
                self.rpm[rpm] = f"{check.check_name}/{rpm}"

                tgz = f"{name}-{version}-{check.deb_arch}.tgz"
                self.tgz[tgz] = f"{check.check_name}/{tgz}"

    def arch(self, deb_pkg: str) -> str:
        if deb_pkg not in self.deb:
            raise ValueError(f"{deb_pkg} not in {self.deb}")
        return removesuffix(deb_pkg, ".deb").split("_")[-1]

    def replace_with_fallback(self, name: str):
        if name.endswith(".deb"):
            suffix = self.deb.pop(name)
            self.deb[self.fallback_to_all(name)] = self.fallback_to_all(suffix)
        elif name.endswith(".rpm"):
            suffix = self.rpm.pop(name)
            self.rpm[self.fallback_to_all(name)] = self.fallback_to_all(suffix)
        elif name.endswith(".tgz"):
            suffix = self.tgz.pop(name)
            self.tgz[self.fallback_to_all(name)] = self.fallback_to_all(suffix)
        else:
            raise KeyError(f"unknown package type for {name}")

    @staticmethod
    def path(package_file: str) -> str:
        return os.path.join(TEMP_PATH, package_file)

    @staticmethod
    def fallback_to_all(url_or_name: str):
        """Until July 2022 we had clickhouse-server and clickhouse-client with
        arch 'all'"""
        # deb
        if url_or_name.endswith("amd64.deb") or url_or_name.endswith("arm64.deb"):
            return f"{url_or_name[:-9]}all.deb"
        # rpm
        if url_or_name.endswith("x86_64.rpm") or url_or_name.endswith("aarch64.rpm"):
            new = removesuffix(removesuffix(url_or_name, "x86_64.rpm"), "aarch64.rpm")
            return f"{new}noarch.rpm"
        # tgz
        if url_or_name.endswith("-amd64.tgz") or url_or_name.endswith("-arm64.tgz"):
            return f"{url_or_name[:-10]}.tgz"
        return url_or_name


class S3:
    template = (
        "https://s3.amazonaws.com/"
        # "clickhouse-builds/"
        f"{S3_BUILDS_BUCKET}/"
        # "33333/" or "21.11/" from --release, if pull request is omitted
        "{pr}/"
        # "2bef313f75e4cacc6ea2ef2133e8849ecf0385ec/"
        "{commit}/"
        # "package_release/clickhouse-common-static_21.11.5.0_amd64.deb"
        "{s3_path_suffix}"
    )

    def __init__(
        self,
        pr: int,
        commit: str,
        version: str,
        force_download: bool,
    ):
        self._common = dict(
            pr=pr,
            commit=commit,
        )
        self.force_download = force_download
        self.packages = Packages(version)

    def download_package(self, package_file: str, s3_path_suffix: str):
        path = Packages.path(package_file)
        fallback_path = Packages.fallback_to_all(path)
        if not self.force_download and (
            os.path.exists(path) or os.path.exists(fallback_path)
        ):
            if os.path.exists(fallback_path):
                self.packages.replace_with_fallback(package_file)

            return
        url = self.template.format_map(
            {**self._common, "s3_path_suffix": s3_path_suffix}
        )
        try:
            dowload_build_with_progress(url, path)
        except Exception as e:
            if "Cannot download dataset from" in e.args[0]:
                new_url = Packages.fallback_to_all(url)
                logging.warning(
                    "Fallback downloading %s for old release", fallback_path
                )
                dowload_build_with_progress(new_url, fallback_path)
                self.packages.replace_with_fallback(package_file)

    def download_deb(self):
        # Copy to have a way to pop/add fallback packages
        packages = self.packages.deb.copy()
        for package_file, s3_path_suffix in packages.items():
            self.download_package(package_file, s3_path_suffix)

    def download_rpm(self):
        # Copy to have a way to pop/add fallback packages
        packages = self.packages.rpm.copy()
        for package_file, s3_path_suffix in packages.items():
            self.download_package(package_file, s3_path_suffix)

    def download_tgz(self):
        # Copy to have a way to pop/add fallback packages
        packages = self.packages.tgz.copy()
        for package_file, s3_path_suffix in packages.items():
            self.download_package(package_file, s3_path_suffix)


class Release:
    def __init__(self, name: str):
        r = re.compile(TAG_REGEXP)
        # Automatically remove refs/tags/ if full refname passed here
        name = removeprefix(name, "refs/tags/")
        if not r.match(name):
            raise argparse.ArgumentTypeError(
                f"release name {name} does not match "
                "v12.1.2.15-(testing|prestable|stable|lts) pattern"
            )
        self._name = name
        self._version = removeprefix(self._name, "v")
        self._version = self.version.split("-")[0]
        self._version_parts = tuple(self.version.split("."))
        self._type = self._name.split("-")[-1]

    @property
    def version(self) -> str:
        return self._version

    @property
    def version_parts(self) -> Tuple[str, ...]:
        return self._version_parts

    @property
    def type(self) -> str:
        return self._type


class Artifactory:
    def __init__(
        self, url: str, release: str, deb_repo="deb", rpm_repo="rpm", tgz_repo="tgz"
    ):
        self._url = url
        self._release = release
        self._deb_url = "/".join((self._url, deb_repo, "pool", self._release)) + "/"
        self._rpm_url = "/".join((self._url, rpm_repo, self._release)) + "/"
        self._tgz_url = "/".join((self._url, tgz_repo, self._release)) + "/"
        # check the credentials ENVs for early exit
        self.__path_helper("_deb", "")

    def deploy_deb(self, packages: Packages):
        for package_file in packages.deb:
            path = packages.path(package_file)
            dist = self._release
            comp = "main"
            arch = packages.arch(package_file)
            logging.info(
                "Deploy %s(distribution=%s;component=%s;architecture=%s) "
                "to artifactory",
                path,
                dist,
                comp,
                arch,
            )
            self.deb_path(package_file).deploy_deb(path, dist, comp, arch)

    def deploy_rpm(self, packages: Packages):
        for package_file in packages.rpm:
            path = packages.path(package_file)
            logging.info("Deploy %s to artifactory", path)
            self.rpm_path(package_file).deploy_file(path)

    def deploy_tgz(self, packages: Packages):
        for package_file in packages.tgz:
            path = packages.path(package_file)
            logging.info("Deploy %s to artifactory", path)
            self.tgz_path(package_file).deploy_file(path)

    def __path_helper(self, name: str, package_file: str) -> ArtifactorySaaSPath:
        url = "/".join((getattr(self, name + "_url"), package_file))
        path = None
        if JFROG_API_KEY:
            path = ArtifactorySaaSPath(url, apikey=JFROG_API_KEY)
        elif JFROG_TOKEN:
            path = ArtifactorySaaSPath(url, token=JFROG_TOKEN)
        else:
            raise KeyError("Neither JFROG_API_KEY nor JFROG_TOKEN env are defined")
        return path

    def deb_path(self, package_file: str) -> ArtifactorySaaSPath:
        return self.__path_helper("_deb", package_file)

    def rpm_path(self, package_file: str) -> ArtifactorySaaSPath:
        return self.__path_helper("_rpm", package_file)

    def tgz_path(self, package_file: str) -> ArtifactorySaaSPath:
        return self.__path_helper("_tgz", package_file)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="Program to download artifacts from S3 and push them to "
        "artifactory. ENV variables JFROG_API_KEY and JFROG_TOKEN are used "
        "for authentication in the given order",
    )
    parser.add_argument(
        "--release",
        required=True,
        type=Release,
        help="release name, e.g. v12.13.14.15-prestable; 'refs/tags/' "
        "prefix is striped automatically",
    )
    parser.add_argument(
        "--pull-request",
        type=int,
        default=0,
        help="pull request number; if PR is omitted, the first two numbers "
        "from release will be used, e.g. 12.11",
    )
    parser.add_argument(
        "--commit", required=True, type=commit, help="commit hash for S3 bucket"
    )
    parser.add_argument(
        "--all", action="store_true", help="implies all deb, rpm and tgz"
    )
    parser.add_argument(
        "--deb", action="store_true", help="if Debian packages should be processed"
    )
    parser.add_argument(
        "--rpm", action="store_true", help="if RPM packages should be processed"
    )
    parser.add_argument(
        "--tgz",
        action="store_true",
        help="if tgz archives should be processed. They aren't pushed to artifactory",
    )
    parser.add_argument(
        "--artifactory-url",
        default="https://clickhousedb.jfrog.io/artifactory",
        help="SaaS Artifactory url",
    )
    parser.add_argument("--artifactory", default=True, help=argparse.SUPPRESS)
    parser.add_argument(
        "-n",
        "--no-artifactory",
        action="store_false",
        dest="artifactory",
        default=argparse.SUPPRESS,
        help="do not push packages to artifactory",
    )
    parser.add_argument("--force-download", default=True, help=argparse.SUPPRESS)
    parser.add_argument(
        "--no-force-download",
        action="store_false",
        dest="force_download",
        default=argparse.SUPPRESS,
        help="do not download packages again if they exist already",
    )

    args = parser.parse_args()
    if args.all:
        args.deb = args.rpm = args.tgz = True
    if not (args.deb or args.rpm or args.tgz):
        parser.error("at least one of --deb, --rpm or --tgz should be specified")
    if args.pull_request == 0:
        args.pull_request = ".".join(args.release.version_parts[:2])
    return args


def process_deb(s3: S3, art_clients: List[Artifactory]):
    s3.download_deb()
    for art_client in art_clients:
        art_client.deploy_deb(s3.packages)


def process_rpm(s3: S3, art_clients: List[Artifactory]):
    s3.download_rpm()
    for art_client in art_clients:
        art_client.deploy_rpm(s3.packages)


def process_tgz(s3: S3, art_clients: List[Artifactory]):
    s3.download_tgz()
    for art_client in art_clients:
        art_client.deploy_tgz(s3.packages)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    args = parse_args()
    os.makedirs(TEMP_PATH, exist_ok=True)
    s3 = S3(
        args.pull_request,
        args.commit,
        args.release.version,
        args.force_download,
    )
    art_clients = []
    if args.artifactory:
        art_clients.append(Artifactory(args.artifactory_url, args.release.type))
        if args.release.type == "lts":
            art_clients.append(Artifactory(args.artifactory_url, "stable"))

    if args.deb:
        process_deb(s3, art_clients)
    if args.rpm:
        process_rpm(s3, art_clients)
    if args.tgz:
        process_tgz(s3, art_clients)


if __name__ == "__main__":
    main()
