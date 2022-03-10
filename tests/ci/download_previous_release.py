#!/usr/bin/env python3

###########################################################################
#                                                                         #
# TODO (@vdimir, @Avogar)                                                 #
# Merge with one from https://github.com/ClickHouse/ClickHouse/pull/27928 #
#                                                                         #
###########################################################################

import re
import os
import logging

import requests

CLICKHOUSE_TAGS_URL = "https://api.github.com/repos/ClickHouse/ClickHouse/tags"

CLICKHOUSE_COMMON_STATIC_DOWNLOAD_URL = "https://github.com/ClickHouse/ClickHouse/releases/download/v{version}-{type}/clickhouse-common-static_{version}_amd64.deb"
CLICKHOUSE_COMMON_STATIC_DBG_DOWNLOAD_URL = "https://github.com/ClickHouse/ClickHouse/releases/download/v{version}-{type}/clickhouse-common-static-dbg_{version}_amd64.deb"
CLICKHOUSE_SERVER_DOWNLOAD_URL = "https://github.com/ClickHouse/ClickHouse/releases/download/v{version}-{type}/clickhouse-server_{version}_all.deb"
CLICKHOUSE_CLIENT_DOWNLOAD_URL = "https://github.com/ClickHouse/ClickHouse/releases/download/v{version}-{type}/clickhouse-client_{version}_all.deb"


CLICKHOUSE_COMMON_STATIC_PACKET_NAME = "clickhouse-common-static_{version}_amd64.deb"
CLICKHOUSE_COMMON_STATIC_DBG_PACKET_NAME = "clickhouse-common-static-dbg_{version}_amd64.deb"
CLICKHOUSE_SERVER_PACKET_NAME = "clickhouse-server_{version}_all.deb"
CLICKHOUSE_CLIENT_PACKET_NAME = "clickhouse-client_{version}_all.deb"

PACKETS_DIR = "previous_release_package_folder/"
VERSION_PATTERN = r"((?:\d+\.)?(?:\d+\.)?(?:\d+\.)?\d+-[a-zA-Z]*)"


class Version:
    def __init__(self, version):
        self.version = version

    def __lt__(self, other):
        return list(map(int, self.version.split('.'))) < list(map(int, other.version.split('.')))

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


def get_previous_release(server_version=None):
    page = 1
    found = False
    while not found:
        response = requests.get(CLICKHOUSE_TAGS_URL, {'page': page, 'per_page': 100})
        if not response.ok:
            raise Exception('Cannot load the list of tags from github: ' + response.reason)

        releases_str = set(re.findall(VERSION_PATTERN, response.text))
        if len(releases_str) == 0:
            raise Exception('Cannot find previous release for ' + str(server_version) + ' server version')

        releases = list(map(lambda x: ReleaseInfo(Version(x.split('-')[0]), x.split('-')[1]), releases_str))
        found, previous_release = find_previous_release(server_version, releases)
        page += 1

    return previous_release


def download_packet(url, out_path):
    """
    TODO: use dowload_build_with_progress from build_download_helper.py
    """

    response = requests.get(url)
    logging.info('Downloading %s', url)
    if response.ok:
        open(out_path, 'wb').write(response.content)

def download_packets(release, dest_path=PACKETS_DIR):
    if not os.path.exists(dest_path):
        os.makedirs(dest_path)

    logging.info('Will download %s', release)

    download_packet(
        CLICKHOUSE_COMMON_STATIC_DOWNLOAD_URL.format(version=release.version, type=release.type),
        out_path=os.path.join(dest_path, CLICKHOUSE_COMMON_STATIC_PACKET_NAME.format(version=release.version)),
    )

    download_packet(
        CLICKHOUSE_COMMON_STATIC_DBG_DOWNLOAD_URL.format(version=release.version, type=release.type),
        out_path=os.path.join(dest_path, CLICKHOUSE_COMMON_STATIC_DBG_PACKET_NAME.format(version=release.version)),
    )

    download_packet(
        CLICKHOUSE_SERVER_DOWNLOAD_URL.format(version=release.version, type=release.type),
        out_path=os.path.join(dest_path, CLICKHOUSE_SERVER_PACKET_NAME.format(version=release.version)),
    )

    download_packet(
        CLICKHOUSE_CLIENT_DOWNLOAD_URL.format(version=release.version, type=release.type),
        out_path=os.path.join(dest_path, CLICKHOUSE_CLIENT_PACKET_NAME.format(version=release.version)),
    )


def download_previous_release(dest_path):
    current_release = get_previous_release(None)
    download_packets(current_release, dest_path=dest_path)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    server_version = Version(input())
    previous_release = get_previous_release(server_version)
    download_packets(previous_release)

