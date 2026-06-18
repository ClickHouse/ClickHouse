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

# The release packages are a hard prerequisite for the upgrade check: if any of
# them is missing the whole job is wasted on setup. Retry more persistently than
# the generic default to ride out transient GitHub/CDN hiccups (the per-attempt
# backoff is capped, so this extends the total time window rather than the sleep).
RELEASE_PACKAGE_DOWNLOAD_RETRIES = 10

# Packages that the upgrade check actually installs (see `install_packages` in
# `tests/docker_scripts/stress_tests.lib`). Only these are essential; a hiccup
# while downloading any of the other assets (e.g. `clickhouse-keeper`) must not
# fail the job.
REQUIRED_PACKAGE_PREFIXES = (
    "clickhouse-common-static_",
    "clickhouse-common-static-dbg_",
    "clickhouse-server_",
    "clickhouse-client_",
)


def download_packages(
    release: ReleaseInfo, dest_path: Path = PACKAGES_DIR, debug: bool = False
) -> None:
    dest_path.mkdir(parents=True, exist_ok=True)

    logging.info("Will download %s", release)

    failed = {}
    for pkg, url in release.assets.items():
        if not pkg.endswith("_amd64.deb") or (not debug and "-dbg_" in pkg):
            continue
        pkg_name = dest_path / pkg
        try:
            download_build_with_progress(
                url, pkg_name, retries=RELEASE_PACKAGE_DOWNLOAD_RETRIES
            )
        except DownloadException as e:
            failed[pkg] = str(e)
            logging.error("Failed to download %s: %s", pkg, e)

    # Do not silently skip a missing essential package: an incomplete set always
    # breaks the subsequent install, so fail with a clear, attributable reason
    # (the per-package message distinguishes a genuine 404 from a transient error).
    required_failed = {
        pkg: reason
        for pkg, reason in failed.items()
        if pkg.startswith(REQUIRED_PACKAGE_PREFIXES)
    }
    if required_failed:
        details = "; ".join(
            f"{pkg}: {reason}" for pkg, reason in required_failed.items()
        )
        raise DownloadException(
            f"Failed to download {len(required_failed)} required release package(s) "
            f"for {release}: {details}"
        )
    if failed:
        logging.warning(
            "Some non-essential packages failed to download: %s", ", ".join(failed)
        )


def download_last_release(dest_path: Path, debug: bool = False) -> None:
    current_release = get_previous_release(None)
    if current_release is None:
        raise DownloadException("The current release is not found")
    download_packages(current_release, dest_path=dest_path, debug=debug)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    release = get_release_by_tag(input())
    download_packages(release, debug=True)
