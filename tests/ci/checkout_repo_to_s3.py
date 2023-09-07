#!/usr/bin/env python3

"""
This script is used for uploading and downloading git checkout to/from S3.

It is used by CI to store git checkouts between builds
and to avoid cloning git repo on every build.
"""

import argparse
import logging
from pathlib import Path
import requests

from env_helper import REPO_COPY, S3_DOWNLOAD, S3_BUILDS_BUCKET, TEMP_PATH
from compress_files import compress_fast, decompress_fast
from s3_helper import S3Helper
from git_helper import Git, git_runner
from build_download_helper import download_build_with_progress


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["upload", "download"])
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--sha", help="Commit sha to used in archive name")
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


def main_upload(args, commit_sha, s3_helper: S3Helper) -> None:
    remote_path = get_remote_path(commit_sha)

    if args.debug:
        response = requests.head(f"{S3_DOWNLOAD}/{S3_BUILDS_BUCKET}/{remote_path}")
        if response.status_code == 200:
            logging.info("Archive %s already exists, skipping upload", remote_path)
            return
        logging.info(
            "Archive %s does not exist (response %s), uploading", remote_path, response
        )

    archive_local_path = get_local_path(commit_sha)
    create_archive(Path(REPO_COPY), archive_local_path)
    s3_helper.upload_build_file_to_s3(archive_local_path, remote_path)


def exctract_archive(repo_path: str, archive_path: str) -> None:
    exctract_to_path = Path(repo_path)
    if exctract_to_path.name == "ClickHouse":
        if exctract_to_path.exists():
            if len(list(exctract_to_path.iterdir())) > 0:
                raise Exception(f"Path {exctract_to_path} is not empty")
        exctract_to_path = exctract_to_path.parent

    exctract_to_path.mkdir(parents=True, exist_ok=True)
    decompress_fast(archive_path, exctract_to_path)


def main_download(args, commit_sha) -> None:
    remote_path = get_remote_path(commit_sha)
    full_url = f"{S3_DOWNLOAD}/{S3_BUILDS_BUCKET}/{remote_path}"
    archive_local_path = get_local_path(commit_sha)
    if archive_local_path.exists() and args.debug:
        logging.info("Using existing archive %s", archive_local_path)
    else:
        download_build_with_progress(full_url, archive_local_path)

    exctract_archive(REPO_COPY, archive_local_path)

    git_runner(f"git -C {REPO_COPY} submodule update --init")
    git_runner(f"git -C {REPO_COPY} status")


def main(args):
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    tmp_path = Path(TEMP_PATH)
    if not tmp_path.exists():
        tmp_path.mkdir(parents=True)

    commit_sha = git_runner("git rev-parse HEAD") if args.sha is None else args.sha
    s3_helper = S3Helper()
    if args.action == "upload":
        main_upload(args, commit_sha, s3_helper)
    else:
        main_download(args, commit_sha)


if __name__ == "__main__":
    main(parse_args())
