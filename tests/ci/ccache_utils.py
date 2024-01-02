#!/usr/bin/env python3

import logging
import os
import shutil
from pathlib import Path

from build_download_helper import download_build_with_progress
from compress_files import decompress_fast, compress_fast
from env_helper import S3_DOWNLOAD, S3_BUILDS_BUCKET
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
