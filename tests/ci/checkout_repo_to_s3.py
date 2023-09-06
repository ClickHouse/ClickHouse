#!/usr/bin/env python3

"""
This script is used for uploading and downloading git checkout to/from S3.

It is used by CI to store git checkouts between builds
and to avoid cloning git repo on every build.
"""

import argparse
import logging
from pathlib import Path

from env_helper import REPO_COPY, S3_DOWNLOAD, S3_BUILDS_BUCKET, TEMP_PATH
from compress_files import compress_fast, decompress_fast
from s3_helper import S3Helper
from git_helper import Git, git_runner
from build_download_helper import download_build_with_progress


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["upload", "download"])
    parser.add_argument("--debug", action="store_true")
    return parser.parse_args()


def get_remote_path(commit_sha: str) -> str:
    return f"ccache/git-checkout/{commit_sha[0]}/{commit_sha}.tar.zst"


def get_local_path(commit_sha: str) -> str:
    return Path(TEMP_PATH) / f"{commit_sha}.tar.zst"


def create_archive(repo_path: Path, output_archive: Path) -> None:
    if output_archive.exists():
        logging.warning("Removing existing output archive %s", output_archive)
        output_archive.unlink()

    compress_fast(repo_path, output_archive)


def main_upload(commit_sha, s3_helper: S3Helper) -> None:
    archive_local_path = get_local_path(commit_sha)
    create_archive(Path(REPO_COPY), archive_local_path)
    remote_path = get_remote_path(commit_sha)
    s3_helper.upload_build_file_to_s3(archive_local_path, remote_path)


def main_download(commit_sha) -> None:
    remote_path = get_remote_path(commit_sha)
    full_url = f"{S3_DOWNLOAD}/{S3_BUILDS_BUCKET}/{remote_path}"
    archive_local_path = get_local_path(commit_sha)
    download_build_with_progress(full_url, archive_local_path)

    repo_path = Path(REPO_COPY)
    if repo_path.name == "ClickHouse":
        repo_path = repo_path.parent

    if repo_path.exists():
        if len(list(repo_path.iterdir())) > 0:
            raise Exception(f"Path {repo_path} is not empty")
    else:
        repo_path.mkdir(parents=True)

    decompress_fast(archive_local_path, repo_path)


def main(args):
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    tmp_path = Path(TEMP_PATH)
    if not tmp_path.exists():
        tmp_path.mkdir(parents=True)

    commit_sha = git_runner("git rev-parse HEAD")
    s3_helper = S3Helper()
    if args.action == "upload":
        main_upload(commit_sha, s3_helper)
    else:
        main_download(commit_sha)


if __name__ == "__main__":
    main(parse_args())
