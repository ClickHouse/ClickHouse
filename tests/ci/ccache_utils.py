#!/usr/bin/env python3

import logging
import time
import sys
import os
import shutil
from pathlib import Path

import requests  # type: ignore

from compress_files import decompress_fast, compress_fast
from env_helper import S3_DOWNLOAD, S3_BUILDS_BUCKET
from s3_helper import S3Helper

DOWNLOAD_RETRIES_COUNT = 5


def dowload_file_with_progress(url, path):
    logging.info("Downloading from %s to temp path %s", url, path)
    for i in range(DOWNLOAD_RETRIES_COUNT):
        try:
            with open(path, "wb") as f:
                response = requests.get(url, stream=True)
                response.raise_for_status()
                total_length = response.headers.get("content-length")
                if total_length is None or int(total_length) == 0:
                    logging.info(
                        "No content-length, will download file without progress"
                    )
                    f.write(response.content)
                else:
                    dl = 0
                    total_length = int(total_length)
                    logging.info("Content length is %ld bytes", total_length)
                    for data in response.iter_content(chunk_size=4096):
                        dl += len(data)
                        f.write(data)
                        if sys.stdout.isatty():
                            done = int(50 * dl / total_length)
                            percent = int(100 * float(dl) / total_length)
                            eq_str = "=" * done
                            space_str = " " * (50 - done)
                            sys.stdout.write(f"\r[{eq_str}{space_str}] {percent}%")
                            sys.stdout.flush()
            break
        except Exception as ex:
            sys.stdout.write("\n")
            time.sleep(3)
            logging.info("Exception while downloading %s, retry %s", ex, i + 1)
            if os.path.exists(path):
                os.remove(path)
    else:
        raise Exception(f"Cannot download dataset from {url}, all retries exceeded")

    sys.stdout.write("\n")
    logging.info("Downloading finished")


def get_ccache_if_not_exists(
    path_to_ccache_dir: str,
    s3_helper: S3Helper,
    current_pr_number: int,
    temp_path: str,
    release_pr: int,
) -> int:
    """returns: number of PR for downloaded PR. -1 if ccache not found"""
    ccache_name = os.path.basename(path_to_ccache_dir)
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
        compressed_cache = os.path.join(temp_path, os.path.basename(obj))
        dowload_file_with_progress(url, compressed_cache)

        path_to_decompress = str(Path(path_to_ccache_dir).parent)
        if not os.path.exists(path_to_decompress):
            os.makedirs(path_to_decompress)

        if os.path.exists(path_to_ccache_dir):
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
        if os.path.exists(path_to_ccache_dir):
            logging.info("But at least we have some local cache")
    else:
        logging.info("ccache downloaded")

    return ccache_pr


def upload_ccache(path_to_ccache_dir, s3_helper, current_pr_number, temp_path):
    logging.info("Uploading cache %s for pr %s", path_to_ccache_dir, current_pr_number)
    ccache_name = os.path.basename(path_to_ccache_dir)
    compressed_cache_path = os.path.join(temp_path, ccache_name + ".tar.gz")
    compress_fast(path_to_ccache_dir, compressed_cache_path)

    s3_path = (
        str(current_pr_number) + "/ccaches/" + os.path.basename(compressed_cache_path)
    )
    logging.info("Will upload %s to path %s", compressed_cache_path, s3_path)
    s3_helper.upload_build_file_to_s3(compressed_cache_path, s3_path)
    logging.info("Upload finished")
