#!/usr/bin/env python
"""
This file is needed to avoid cicle import build_download_helper.py <=> env_helper.py
"""

import argparse
import logging
from pathlib import Path

from build_download_helper import download_build_with_progress
from ci_config import CI
from env_helper import RUNNER_TEMP, S3_ARTIFACT_DOWNLOAD_TEMPLATE
from git_helper import Git, commit
from version_helper import get_version_from_repo, version_arg


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="Script to download binary artifacts from S3. Downloaded artifacts "
        "are renamed to clickhouse-{static_binary_name}",
    )
    parser.add_argument(
        "--version",
        type=version_arg,
        default=get_version_from_repo().string,
        help="a version to generate a download url, get from the repo by default",
    )
    parser.add_argument(
        "--commit",
        type=commit,
        default=Git(True).sha,
        help="a version to generate a download url, get from the repo by default",
    )
    parser.add_argument("--rename", default=True, help=argparse.SUPPRESS)
    parser.add_argument(
        "--no-rename",
        dest="rename",
        action="store_false",
        default=argparse.SUPPRESS,
        help="if set, the downloaded binary won't be renamed to "
        "clickhouse-{static_binary_name}, makes sense only for a single build name",
    )
    parser.add_argument(
        "build_names",
        nargs="+",
        help="the build names to download",
    )
    args = parser.parse_args()
    if not args.rename and len(args.build_names) > 1:
        parser.error("`--no-rename` shouldn't be used with more than one build name")
    return args


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    args = parse_args()
    temp_path = Path(RUNNER_TEMP) / Path(__file__).name
    temp_path.mkdir(parents=True, exist_ok=True)
    for build in args.build_names:
        # check if it's in CI_CONFIG
        config = CI.JOB_CONFIGS[build].build_config
        assert config
        if args.rename and config.static_binary_name:
            path = temp_path / f"clickhouse-{config.static_binary_name}"
        else:
            path = temp_path / "clickhouse"

        url = S3_ARTIFACT_DOWNLOAD_TEMPLATE.format(
            pr_or_release=f"{args.version.major}.{args.version.minor}",
            commit=args.commit,
            build_name=build,
            artifact="clickhouse",
        )
        download_build_with_progress(url, path)


if __name__ == "__main__":
    main()
