#!/usr/bin/env python3

import logging
import os
import shutil
from pathlib import Path

from build_download_helper import DownloadException, download_build_with_progress
from compress_files import compress_fast, decompress_fast
from digest_helper import digest_path
from env_helper import S3_BUILDS_BUCKET, S3_DOWNLOAD
from git_helper import git_runner
from s3_helper import S3Helper

DOWNLOAD_RETRIES_COUNT = 5


def get_ccache_if_not_exists(
    path_to_ccache_dir: Path,
    s3_helper: S3Helper,
    current_pr_number: int,
    temp_path: Path,
    release_pr: int,
) -> int:
    """returns: number of PR for downloaded PR. -1 if ccache not found"""
    ccache_name = path_to_ccache_dir.name
    cache_found = False
    prs_to_check = [current_pr_number]
    # Release PR is either 0 or defined
    if release_pr:
        prs_to_check.append(release_pr)
    ccache_pr = -1
    if current_pr_number != 0:
        prs_to_check.append(0)
    for pr_number in prs_to_check:
        logging.info("Searching cache for pr %s", pr_number)
        s3_path_prefix = str(pr_number) + "/ccaches"
        all_cache_objects = s3_helper.list_prefix(s3_path_prefix)
        logging.info("Found %s objects for pr %s", len(all_cache_objects), pr_number)
        objects = [obj for obj in all_cache_objects if ccache_name in obj]
        if not objects:
            continue
        logging.info(
            "Found ccache archives for pr %s: %s", pr_number, ", ".join(objects)
        )

        obj = objects[0]
        # There are multiple possible caches, the newest one ends with .tar.zst
        zst_cache = [obj for obj in objects if obj.endswith(".tar.zst")]
        if zst_cache:
            obj = zst_cache[0]

        logging.info("Found ccache on path %s", obj)
        url = f"{S3_DOWNLOAD}/{S3_BUILDS_BUCKET}/{obj}"
        compressed_cache = temp_path / os.path.basename(obj)
        download_build_with_progress(url, compressed_cache)

        path_to_decompress = path_to_ccache_dir.parent
        path_to_decompress.mkdir(parents=True, exist_ok=True)

        if path_to_ccache_dir.exists():
            shutil.rmtree(path_to_ccache_dir)
            logging.info("Ccache already exists, removing it")

        logging.info("Decompressing cache to path %s", path_to_decompress)
        decompress_fast(compressed_cache, path_to_decompress)
        logging.info("Files on path %s", os.listdir(path_to_decompress))
        cache_found = True
        ccache_pr = pr_number
        break

    if not cache_found:
        logging.info("ccache not found anywhere, cannot download anything :(")
        if path_to_ccache_dir.exists():
            logging.info("But at least we have some local cache")
    else:
        logging.info("ccache downloaded")

    return ccache_pr


def upload_ccache(
    path_to_ccache_dir: Path,
    s3_helper: S3Helper,
    current_pr_number: int,
    temp_path: Path,
) -> None:
    logging.info("Uploading cache %s for pr %s", path_to_ccache_dir, current_pr_number)
    ccache_name = path_to_ccache_dir.name
    compressed_cache_path = temp_path / f"{ccache_name}.tar.zst"
    compress_fast(path_to_ccache_dir, compressed_cache_path)

    s3_path = f"{current_pr_number}/ccaches/{compressed_cache_path.name}"
    logging.info("Will upload %s to path %s", compressed_cache_path, s3_path)
    s3_helper.upload_build_file_to_s3(compressed_cache_path, s3_path)
    logging.info("Upload finished")


class CacheError(Exception):
    pass


class Cache:
    """a generic class for all caches"""

    def __init__(
        self,
        directory: Path,
        temp_path: Path,
        archive_name: str,
        s3_helper: S3Helper,
    ):
        self.directory = directory
        self.temp_path = temp_path
        self.archive_name = archive_name
        self.s3_helper = s3_helper

    def _download(self, url: str, ignore_error: bool = False) -> None:
        self.temp_path.mkdir(parents=True, exist_ok=True)
        compressed_cache = self.temp_path / self.archive_name
        try:
            if url.startswith("file://"):
                local_s3_cache = Path(url[7:])
                if local_s3_cache.is_file():
                    shutil.copy2(local_s3_cache, compressed_cache)
                else:
                    logging.warning(
                        "The local cache file %s does not exist, creating empty directory",
                        local_s3_cache,
                    )
                    self.directory.mkdir(parents=True, exist_ok=True)
                    return
            else:
                download_build_with_progress(url, compressed_cache)
        except DownloadException as e:
            if not ignore_error:
                raise CacheError(f"Failed to download {url}") from e
            logging.warning("Unable downloading cache, creating empty directory")
            self.directory.mkdir(parents=True, exist_ok=True)
            return

        # decompress the cache and check if the necessary directory is there
        self.directory.parent.mkdir(parents=True, exist_ok=True)
        decompress_fast(compressed_cache, self.directory.parent)
        if not self.directory.exists():
            if not ignore_error:
                raise CacheError(
                    "The cache is downloaded and uncompressed, but directory "
                    f"{self.directory} does not exist"
                )
            logging.warning(
                "The cache archive was successfully downloaded and "
                "decompressed, but %s does not exitst. Creating empty one",
                self.directory,
            )
            self.directory.mkdir(parents=True, exist_ok=True)

    def _upload(self, s3_path: str, force_upload: bool = False) -> None:
        if not force_upload:
            existing_cache = self.s3_helper.list_prefix_non_recursive(s3_path)
            if existing_cache:
                logging.info("Remote cache %s already exist, won't reupload", s3_path)
                return

        logging.info("Compressing cache")
        archive_path = self.temp_path / self.archive_name
        compress_fast(self.directory, archive_path)
        logging.info("Uploading %s to S3 path %s", archive_path, s3_path)
        self.s3_helper.upload_build_file_to_s3(archive_path, s3_path)


class CargoCache(Cache):
    PREFIX = "ccache/cargo_cache"

    def __init__(
        self,
        directory: Path,
        temp_path: Path,
        s3_helper: S3Helper,
    ):
        cargo_lock_file = Path(git_runner.cwd) / "rust" / "Cargo.lock"
        self.lock_hash = digest_path(cargo_lock_file).hexdigest()
        self._force_upload_cache = False
        super().__init__(
            directory, temp_path, f"Cargo_cache_{self.lock_hash}.tar.zst", s3_helper
        )
        self._url = self.s3_helper.get_url(
            S3_BUILDS_BUCKET, f"{self.PREFIX}/{self.archive_name}"
        )

    def download(self):
        logging.info("Searching rust cache for Cargo.lock md5 %s", self.lock_hash)
        try:
            self._download(self._url, False)
        except CacheError:
            logging.warning("Unable downloading cargo cache, creating empty directory")
            logging.info("Cache for Cargo.lock md5 %s will be uploaded", self.lock_hash)
            self._force_upload_cache = True
            self.directory.mkdir(parents=True, exist_ok=True)

    def upload(self):
        self._upload(f"{self.PREFIX}/{self.archive_name}", self._force_upload_cache)


class GitHubCache(Cache):
    PREFIX = "ccache/github"

    def __init__(
        self,
        directory: Path,
        temp_path: Path,
        s3_helper: S3Helper,
    ):
        self.force_upload = True
        super().__init__(directory, temp_path, "GitHub.tar.zst", s3_helper)
        self._url = self.s3_helper.get_url(
            S3_BUILDS_BUCKET, f"{self.PREFIX}/{self.archive_name}"
        )

    def download(self):
        logging.info("Searching cache for GitHub class")
        self._download(self._url, True)

    def upload(self):
        self._upload(f"{self.PREFIX}/{self.archive_name}", True)
