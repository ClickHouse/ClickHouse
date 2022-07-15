#!/usr/bin/env python3

import re
import os
import logging

import requests  # type: ignore

from requests.adapters import HTTPAdapter  # type: ignore
from urllib3.util.retry import Retry  # type: ignore

CLICKHOUSE_TAGS_URL = "https://api.github.com/repos/ClickHouse/ClickHouse/tags"

DOWNLOAD_PREFIX = (
    "https://github.com/ClickHouse/ClickHouse/releases/download/v{version}-{type}/"
)
CLICKHOUSE_COMMON_STATIC_PACKET_NAME = "clickhouse-common-static_{version}_amd64.deb"
CLICKHOUSE_COMMON_STATIC_DBG_PACKET_NAME = (
    "clickhouse-common-static-dbg_{version}_amd64.deb"
)
CLICKHOUSE_SERVER_PACKET_NAME = "clickhouse-server_{version}_amd64.deb"
CLICKHOUSE_SERVER_PACKET_FALLBACK = "clickhouse-server_{version}_all.deb"
CLICKHOUSE_CLIENT_PACKET_NAME = "clickhouse-client_{version}_amd64.deb"
CLICKHOUSE_CLIENT_PACKET_FALLBACK = "clickhouse-client_{version}_all.deb"

PACKETS_DIR = "previous_release_package_folder/"
VERSION_PATTERN = r"((?:\d+\.)?(?:\d+\.)?(?:\d+\.)?\d+-[a-zA-Z]*)"


class Version:
    def __init__(self, version):
        self.version = version

    def __lt__(self, other):
        return list(map(int, self.version.split("."))) < list(
            map(int, other.version.split("."))
        )

    def __str__(self):
        return self.version


class ReleaseInfo:
    def __init__(self, version, release_type):
        self.version = version
        self.type = release_type

    def __repr__(self):
        return f"ReleaseInfo: {self.version}-{self.type}"


def find_previous_release(server_version, releases):
    releases.sort(key=lambda x: x.version, reverse=True)

    if server_version is None:
        return True, releases[0]

    for release in releases:
        if release.version < server_version:
            return True, release

    return False, None


def get_previous_release(server_version):
    page = 1
    found = False
    while not found:
        response = requests.get(CLICKHOUSE_TAGS_URL, {"page": page, "per_page": 100})
        if not response.ok:
            raise Exception(
                "Cannot load the list of tags from github: " + response.reason
            )

        releases_str = set(re.findall(VERSION_PATTERN, response.text))
        if len(releases_str) == 0:
            raise Exception(
                "Cannot find previous release for "
                + str(server_version)
                + " server version"
            )

        releases = list(
            map(
                lambda x: ReleaseInfo(Version(x.split("-")[0]), x.split("-")[1]),
                releases_str,
            )
        )
        found, previous_release = find_previous_release(server_version, releases)
        page += 1

    return previous_release


def download_packet(url, out_path, retries=10, backoff_factor=0.3):
    session = requests.Session()
    retry = Retry(
        total=retries, read=retries, connect=retries, backoff_factor=backoff_factor
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    response = session.get(url)
    response.raise_for_status()
    print(f"Download {url} to {out_path}")
    with open(out_path, "wb") as fd:
        fd.write(response.content)


def download_packets(release, dest_path=PACKETS_DIR):
    if not os.path.exists(dest_path):
        os.makedirs(dest_path)

    logging.info("Will download %s", release)

    def get_dest_path(pkg_name):
        return os.path.join(dest_path, pkg_name)

    for pkg in (
        CLICKHOUSE_COMMON_STATIC_PACKET_NAME,
        CLICKHOUSE_COMMON_STATIC_DBG_PACKET_NAME,
    ):
        url = (DOWNLOAD_PREFIX + pkg).format(version=release.version, type=release.type)
        pkg_name = get_dest_path(pkg.format(version=release.version))
        download_packet(url, pkg_name)

    for pkg, fallback in (
        (CLICKHOUSE_SERVER_PACKET_NAME, CLICKHOUSE_SERVER_PACKET_FALLBACK),
        (CLICKHOUSE_CLIENT_PACKET_NAME, CLICKHOUSE_CLIENT_PACKET_FALLBACK),
    ):
        url = (DOWNLOAD_PREFIX + pkg).format(version=release.version, type=release.type)
        pkg_name = get_dest_path(pkg.format(version=release.version))
        try:
            download_packet(url, pkg_name)
        except Exception:
            url = (DOWNLOAD_PREFIX + fallback).format(
                version=release.version, type=release.type
            )
            pkg_name = get_dest_path(fallback.format(version=release.version))
            download_packet(url, pkg_name)


def download_previous_release(dest_path):
    current_release = get_previous_release(None)
    download_packets(current_release, dest_path=dest_path)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    server_version = Version(input())
    previous_release = get_previous_release(server_version)
    download_packets(previous_release)
