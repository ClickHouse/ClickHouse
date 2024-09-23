#!/usr/bin/env python3

import logging
from pathlib import Path

from build_download_helper import DownloadException, download_build_with_progress
from get_previous_release_tag import (
    ReleaseInfo,
    get_previous_release,
    get_release_by_tag,
)

PACKAGES_DIR = Path("previous_release_package_folder")


def download_packages(
    release: ReleaseInfo, dest_path: Path = PACKAGES_DIR, debug: bool = False
) -> None:
    dest_path.mkdir(parents=True, exist_ok=True)

    logging.info("Will download %s", release)

    for pkg, url in release.assets.items():
        if not pkg.endswith("_amd64.deb") or (not debug and "-dbg_" in pkg):
            continue
        pkg_name = dest_path / pkg
        download_build_with_progress(url, pkg_name)


def download_last_release(dest_path: Path, debug: bool = False) -> None:
    current_release = get_previous_release(None)
    if current_release is None:
        raise DownloadException("The current release is not found")
    download_packages(current_release, dest_path=dest_path, debug=debug)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    release = get_release_by_tag(input())
    download_packages(release, debug=True)
