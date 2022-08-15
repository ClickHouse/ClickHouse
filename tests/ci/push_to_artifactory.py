#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
import logging
import os
import re

from artifactory import ArtifactorySaaSPath
from build_download_helper import dowload_build_with_progress


# Necessary ENV variables
def getenv(name, default=None):
    env = os.getenv(name, default)
    if env is not None:
        return env
    raise KeyError(f"Necessary {name} environment is not set")


TEMP_PATH = getenv("TEMP_PATH", ".")
# One of the following ENVs is necessary
JFROG_API_KEY = getenv("JFROG_API_KEY", "")
JFROG_TOKEN = getenv("JFROG_TOKEN", "")


class Packages:
    rpm_arch = dict(all="noarch", amd64="x86_64")
    packages = (
        ("clickhouse-client", "all"),
        ("clickhouse-common-static", "amd64"),
        ("clickhouse-common-static-dbg", "amd64"),
        ("clickhouse-server", "all"),
        ("clickhouse-test", "all"),
    )

    def __init__(self, version: str):
        self.deb = tuple(
            "_".join((name, version, arch + ".deb")) for name, arch in self.packages
        )

        rev = "2"
        self.rpm = tuple(
            "-".join((name, version, rev + "." + self.rpm_arch[arch] + ".rpm"))
            for name, arch in self.packages
        )

    def arch(self, deb_pkg: str) -> str:
        if deb_pkg not in self.deb:
            raise ValueError("{} not in {}".format(deb_pkg, self.deb))
        return deb_pkg.removesuffix(".deb").split("_")[-1]

    @staticmethod
    def path(package):
        return os.path.join(TEMP_PATH, package)


class S3:
    template = (
        "https://s3.amazonaws.com/"
        # "clickhouse-builds/"
        "{bucket_name}/"
        # "33333/" or "21.11/" from --release, if pull request is omitted
        "{pr}/"
        # "2bef313f75e4cacc6ea2ef2133e8849ecf0385ec/"
        "{commit}/"
        # "package_release/"
        "{check_name}/"
        # "clickhouse-common-static_21.11.5.0_amd64.deb"
        "{package}"
    )

    def __init__(
        self,
        bucket_name: str,
        pr: int,
        commit: str,
        check_name: str,
        version: str,
    ):
        self._common = dict(
            bucket_name=bucket_name,
            pr=pr,
            commit=commit,
            check_name=check_name,
        )
        self.packages = Packages(version)

    def download_package(self, package):
        url = self.template.format_map({**self._common, "package": package})
        dowload_build_with_progress(url, Packages.path(package))

    def download_deb(self):
        for package in self.packages.deb:
            self.download_package(package)

    def download_rpm(self):
        for package in self.packages.rpm:
            self.download_package(package)


class Release:
    def __init__(self, name: str) -> str:
        r = re.compile(r"^v\d{2}[.]\d+[.]\d+[.]\d+-(testing|prestable|stable|lts)$")
        if not r.match(name):
            raise argparse.ArgumentTypeError(
                "release name does not match "
                "v12.1.2.15-(testing|prestable|stable|lts) pattern"
            )
        self._name = name
        self._version = self._name.removeprefix("v")
        self._version = self.version.split("-")[0]
        self._version_parts = tuple(self.version.split("."))
        self._type = self._name.split("-")[-1]

    @property
    def version(self) -> str:
        return self._version

    @property
    def version_parts(self) -> str:
        return self._version_parts

    @property
    def type(self) -> str:
        return self._type


class Artifactory:
    def __init__(self, url: str, release: str, deb_repo="deb", rpm_repo="rpm"):
        self._url = url
        self._release = release
        self._deb_url = "/".join((self._url, deb_repo, "pool", self._release)) + "/"
        self._rpm_url = "/".join((self._url, rpm_repo, self._release)) + "/"
        # check the credentials ENVs for early exit
        self.__path_helper("_deb", "")

    def deploy_deb(self, packages: Packages):
        for package in packages.deb:
            path = packages.path(package)
            dist = self._release
            comp = "main"
            arch = packages.arch(package)
            logging.info(
                "Deploy %s(distribution=%s;component=%s;architecture=%s) to artifactory",
                path,
                dist,
                comp,
                arch,
            )
            self.deb(package).deploy_deb(path, dist, comp, arch)

    def deploy_rpm(self, packages: Packages):
        for package in packages.rpm:
            path = packages.path(package)
            logging.info("Deploy %s to artifactory", path)
            self.rpm(package).deploy_file(path)

    def __path_helper(self, name, package) -> ArtifactorySaaSPath:
        url = "/".join((getattr(self, name + "_url"), package))
        path = None
        if JFROG_API_KEY:
            path = ArtifactorySaaSPath(url, apikey=JFROG_API_KEY)
        elif JFROG_TOKEN:
            path = ArtifactorySaaSPath(url, token=JFROG_TOKEN)
        else:
            raise KeyError("Neither JFROG_API_KEY nor JFROG_TOKEN env are defined")
        return path

    def deb(self, package) -> ArtifactorySaaSPath:
        return self.__path_helper("_deb", package)

    def rpm(self, package) -> ArtifactorySaaSPath:
        return self.__path_helper("_rpm", package)


def commit(name):
    r = re.compile(r"^([0-9]|[a-f]){40}$")
    if not r.match(name):
        raise argparse.ArgumentTypeError(
            "commit hash should contain exactly 40 hex characters"
        )
    return name


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
        help="release name, e.g. v12.13.14.15-prestable",
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
        "--bucket-name",
        default="clickhouse-builds",
        help="AWS S3 bucket name",
    )
    parser.add_argument(
        "--check-name",
        default="package_release",
        help="check name, a part of bucket path, "
        "will be converted to lower case with spaces->underscore",
    )
    parser.add_argument(
        "--deb", action="store_true", help="if Debian packages should be processed"
    )
    parser.add_argument(
        "--rpm", action="store_true", help="if RPM packages should be processed"
    )
    parser.add_argument(
        "--artifactory-url", default="https://clickhousedb.jfrog.io/artifactory"
    )

    args = parser.parse_args()
    if not args.deb and not args.rpm:
        parser.error("at least one of --deb and --rpm should be specified")
    args.check_name = args.check_name.lower().replace(" ", "_")
    if args.pull_request == 0:
        args.pull_request = ".".join(args.release.version_parts[:2])
    return args


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    args = parse_args()
    s3 = S3(
        args.bucket_name,
        args.pull_request,
        args.commit,
        args.check_name,
        args.release.version,
    )
    art_client = Artifactory(args.artifactory_url, args.release.type)
    if args.deb:
        s3.download_deb()
        art_client.deploy_deb(s3.packages)
    if args.rpm:
        s3.download_rpm()
        art_client.deploy_rpm(s3.packages)


if __name__ == "__main__":
    main()
