#!/usr/bin/env python3

import logging
import re
from typing import List, Optional, Tuple

import requests

CLICKHOUSE_TAGS_URL = "https://api.github.com/repos/ClickHouse/ClickHouse/tags"
CLICKHOUSE_PACKAGE_URL = (
    "https://github.com/ClickHouse/ClickHouse/releases/download/"
    "v{version}-{type}/clickhouse-common-static_{version}_amd64.deb"
)
VERSION_PATTERN = r"(v(?:\d+\.)?(?:\d+\.)?(?:\d+\.)?\d+-[a-zA-Z]*)"

logger = logging.getLogger(__name__)


class Version:
    def __init__(self, version: str):
        self.version = version

    def __lt__(self, other: "Version") -> bool:
        return list(map(int, self.version.split("."))) < list(
            map(int, other.version.split("."))
        )

    def __str__(self):
        return self.version


class ReleaseInfo:
    def __init__(self, release_tag: str):
        self.version = Version(release_tag[1:].split("-")[0])
        self.type = release_tag[1:].split("-")[1]

    def __str__(self):
        return f"v{self.version}-{self.type}"

    def __repr__(self):
        return f"ReleaseInfo: {self.version}-{self.type}"


def find_previous_release(
    server_version: Optional[Version], releases: List[ReleaseInfo]
) -> Tuple[bool, Optional[ReleaseInfo]]:
    releases.sort(key=lambda x: x.version, reverse=True)

    if server_version is None:
        return True, releases[0]

    for release in releases:
        if release.version < server_version:
            # Check if the artifact exists on GitHub.
            # It can be not true for a short period of time
            # after creating a tag for a new release before uploading the packages.
            if (
                requests.head(
                    CLICKHOUSE_PACKAGE_URL.format(
                        version=release.version, type=release.type
                    ),
                    timeout=10,
                ).status_code
                != 404
            ):
                return True, release

            logger.debug(
                "The tag v%s-%s exists but the package is not yet available on GitHub",
                release.version,
                release.type,
            )

    return False, None


def get_previous_release(server_version: Optional[Version]) -> Optional[ReleaseInfo]:
    page = 1
    found = False
    while not found:
        response = requests.get(
            CLICKHOUSE_TAGS_URL, {"page": page, "per_page": 100}, timeout=10
        )
        if not response.ok:
            logger.error(
                "Cannot load the list of tags from github: %s", response.reason
            )
            response.raise_for_status()

        releases_str = set(re.findall(VERSION_PATTERN, response.text))
        if len(releases_str) == 0:
            raise ValueError(
                "Cannot find previous release for "
                + str(server_version)
                + " server version"
            )

        releases = [ReleaseInfo(release) for release in releases_str]
        found, previous_release = find_previous_release(server_version, releases)
        page += 1

    return previous_release


def main():
    logging.basicConfig(level=logging.INFO)
    server_version = Version(input())
    print(get_previous_release(server_version))


if __name__ == "__main__":
    main()
