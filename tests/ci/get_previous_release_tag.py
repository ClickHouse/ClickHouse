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
PACKAGE_REGEXP = r"\Aclickhouse-common-static_.+[.]deb"

logger = logging.getLogger(__name__)


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
            # Check if the artifact exists on GitHub.
            # It can be not true for a short period of time
            # after creating a tag for a new release before uploading the packages.
            if any(re.match(PACKAGE_REGEXP, name) for name in release.assets.keys()):
                return True, release

            logger.debug(
                "The tag v%s-%s exists but the package is not yet available on GitHub",
                release.version,
                release.type,
            )

    return False, None


def get_previous_release(
    server_version: Optional[ClickHouseVersion],
) -> Optional[ReleaseInfo]:
    page = 1
    found = False
    while not found:
        response = get_gh_api(
            CLICKHOUSE_TAGS_URL, params={"page": page, "per_page": 100}, timeout=10
        )
        if not response.ok:
            logger.error(
                "Cannot load the list of tags from github: %s", response.reason
            )
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
        page += 1

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
