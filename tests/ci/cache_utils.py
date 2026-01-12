#!/usr/bin/env python3

import logging
import shutil
from pathlib import Path

from build_download_helper import DownloadException, download_build_with_progress
from compress_files import compress_fast, decompress_fast
from env_helper import S3_BUILDS_BUCKET
from s3_helper import S3Helper

DOWNLOAD_RETRIES_COUNT = 5


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
