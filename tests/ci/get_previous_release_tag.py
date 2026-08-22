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
RELEASES_PER_PAGE = 100

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

    if not releases:
        return False, None

    if server_version is None:
        return True, releases[0]

    for release in releases:
        if release.version < server_version:
            # A tag exists for a short period before its packages are uploaded.
            if any(re.match(PACKAGE_REGEXP, name) for name in release.assets.keys()):
                return True, release

            logger.warning(
                "Skipping v%s-%s: no uploaded package matching %s",
                release.version,
                release.type,
                PACKAGE_REGEXP,
            )

    return False, None


def get_previous_release(
    server_version: Optional[ClickHouseVersion],
) -> Optional[ReleaseInfo]:
    response = get_gh_api(
        CLICKHOUSE_TAGS_URL,
        params={"page": 1, "per_page": RELEASES_PER_PAGE},
        timeout=10,
    )
    if not response.ok:
        logger.error("Cannot load the list of tags from github: %s", response.reason)
        response.raise_for_status()

    releases = response.json()
    # The first page carries the newest releases, so a page shorter than requested is a
    # degraded response rather than the end of the feed.
    if len(releases) < RELEASES_PER_PAGE:
        raise ReleaseNotFoundException(
            f"The first page of {CLICKHOUSE_TAGS_URL} returned {len(releases)} "
            f"releases, expected {RELEASES_PER_PAGE}"
        )

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

    # Only releases the page itself sorts below the answer show that the page reaches
    # past it; without any, an even newer eligible release could sit off the page.
    older_on_page = [r for r in release_infos if r.version < previous_release.version]
    if not older_on_page:
        raise ReleaseNotFoundException(
            f"Resolved {previous_release} as the release before {server_version}, but "
            f"it is the oldest of the {len(release_infos)} releases on the first page "
            f"of {CLICKHOUSE_TAGS_URL}, which cannot show that no newer one precedes it"
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
