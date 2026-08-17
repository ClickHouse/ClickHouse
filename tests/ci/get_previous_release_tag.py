#!/usr/bin/env python3

import logging
import re
from typing import Dict, List, Optional, Tuple

from build_download_helper import get_gh_api
from git_helper import TAG_REGEXP
from version_helper import (
    ClickHouseVersion,
    get_version_from_string,
    get_version_from_tag,
)

CLICKHOUSE_TAGS_URL = "https://api.github.com/repos/ClickHouse/ClickHouse/releases"

logger = logging.getLogger(__name__)


class ReleaseNotFoundException(Exception):
    pass


class ReleaseInfo:
    def __init__(self, release_tag: str, assets: Dict[str, str]):
        self.version = get_version_from_tag(release_tag)
        self.type = self.version.description
        self.assets = assets

    def __str__(self):
        return self.version.describe

    def __repr__(self):
        return f"ReleaseInfo: {self.version.describe}"


def find_previous_release(
    server_version: Optional[ClickHouseVersion], releases: List[ReleaseInfo]
) -> Tuple[bool, Optional[ReleaseInfo]]:
    releases.sort(key=lambda x: x.version, reverse=True)

    if server_version is None:
        return True, releases[0]

    for release in releases:
        if release.version < server_version:
            return True, release

    return False, None


def get_previous_release(
    server_version: Optional[ClickHouseVersion],
) -> Optional[ReleaseInfo]:
    # The endpoint orders releases by `created_at`, not by version, so later
    # pages hold only older releases: the newest release below `server_version`
    # is on the first page, and a first page that cannot answer is degraded.
    response = get_gh_api(
        CLICKHOUSE_TAGS_URL, params={"page": 1, "per_page": 100}, timeout=10
    )
    if not response.ok:
        logger.error("Cannot load the list of tags from github: %s", response.reason)
        response.raise_for_status()

    releases = response.json()

    release_infos = []  # type: List[ReleaseInfo]
    for r in releases:
        if re.match(TAG_REGEXP, r["tag_name"]):
            assets = {
                a["name"]: a["browser_download_url"]
                for a in r["assets"]
                if a["state"] == "uploaded"
            }
            release_infos.append(ReleaseInfo(r["tag_name"], assets))

    found, previous_release = find_previous_release(server_version, release_infos)
    if not found:
        raise ReleaseNotFoundException(
            f"Cannot find a release older than {server_version} among the "
            f"{len(release_infos)} most recent releases "
            f"(newest: {release_infos[0] if release_infos else 'none'}, "
            f"oldest: {release_infos[-1] if release_infos else 'none'})"
        )

    return previous_release


def get_release_by_tag(tag: str) -> ReleaseInfo:
    response = get_gh_api(f"{CLICKHOUSE_TAGS_URL}/tags/{tag}", timeout=10)
    release = response.json()
    assets = {
        a["name"]: a["browser_download_url"]
        for a in release["assets"]
        if a["state"] == "uploaded"
    }
    return ReleaseInfo(release["tag_name"], assets)


def main():
    logging.basicConfig(level=logging.INFO)
    version_string = input()
    version_string = version_string.split("+", maxsplit=1)[0]
    try:
        server_version = get_version_from_string(version_string)
    except ValueError:
        server_version = get_version_from_tag(version_string)
    print(get_previous_release(server_version))


if __name__ == "__main__":
    main()
