#!/usr/bin/env python3

import re
import logging

import requests

CLICKHOUSE_TAGS_URL = "https://api.github.com/repos/ClickHouse/ClickHouse/tags"
VERSION_PATTERN = r"(v(?:\d+\.)?(?:\d+\.)?(?:\d+\.)?\d+-[a-zA-Z]*)"


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
    def __init__(self, release_tag):
        self.version = Version(release_tag[1:].split("-")[0])
        self.type = release_tag[1:].split("-")[1]

    def __str__(self):
        return f"v{self.version}-{self.type}"

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

        releases = [ReleaseInfo(release) for release in releases_str]
        found, previous_release = find_previous_release(server_version, releases)
        page += 1

    return previous_release


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    server_version = Version(input())
    print(get_previous_release(server_version))
