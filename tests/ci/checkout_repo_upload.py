#!/usr/bin/env python3
import argparse
import logging
from pathlib import Path

from env_helper import GITHUB_WORKSPACE
from compress_files import compress_fast
from s3_helper import S3Helper
from git_helper import Git


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--workspace", default=GITHUB_WORKSPACE, type=Path)
    parser.add_argument("--tmp_path", default="/tmp", type=Path)
    parser.add_argument("--debug", action="store_true")
    return parser.parse_args()


def create_archive(workspace: Path, output_archive: Path) -> None:
    if output_archive.exists():
        logging.warning("Removing existing output archive %s", output_archive)
        output_archive.unlink()

    compress_fast(workspace, output_archive)


def main(args):
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    archive_file_name = "repo.tar.zst"
    output_archive = args.tmp_path / archive_file_name
    create_archive(args.workspace, output_archive)

    git_helper = Git()
    s3_helper = S3Helper()

    s3_helper.upload_actifact_to_s3(output_archive, git_helper.sha, archive_file_name)


if __name__ == "__main__":
    main(parse_args())
