#!/usr/bin/env python3

import os
import json
import logging
import sys
import time
from typing import Optional

import requests  # type: ignore

from ci_config import CI_CONFIG

DOWNLOAD_RETRIES_COUNT = 5


def get_with_retries(
    url: str,
    retries: int = DOWNLOAD_RETRIES_COUNT,
    sleep: int = 3,
    **kwargs,
) -> requests.Response:
    logging.info(
        "Getting URL with %i tries and sleep %i in between: %s", retries, sleep, url
    )
    exc = None  # type: Optional[Exception]
    for i in range(retries):
        try:
            response = requests.get(url, **kwargs)
            response.raise_for_status()
            break
        except Exception as e:
            if i + 1 < retries:
                logging.info("Exception '%s' while getting, retry %i", e, i + 1)
                time.sleep(sleep)

            exc = e
    else:
        raise Exception(exc)

    return response


def get_build_name_for_check(check_name):
    return CI_CONFIG["tests_config"][check_name]["required_build"]


def get_build_urls(build_name, reports_path):
    for root, _, files in os.walk(reports_path):
        for f in files:
            if build_name in f:
                logging.info("Found build report json %s", f)
                with open(os.path.join(root, f), "r", encoding="utf-8") as file_handler:
                    build_report = json.load(file_handler)
                    return build_report["build_urls"]
    return []


def dowload_build_with_progress(url, path):
    logging.info("Downloading from %s to temp path %s", url, path)
    for i in range(DOWNLOAD_RETRIES_COUNT):
        try:
            with open(path, "wb") as f:
                response = get_with_retries(url, retries=1, stream=True)
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
        except Exception:
            if sys.stdout.isatty():
                sys.stdout.write("\n")
            if i + 1 < DOWNLOAD_RETRIES_COUNT:
                time.sleep(3)

            if os.path.exists(path):
                os.remove(path)
    else:
        raise Exception(f"Cannot download dataset from {url}, all retries exceeded")

    if sys.stdout.isatty():
        sys.stdout.write("\n")
    logging.info("Downloading finished")


def download_builds(result_path, build_urls, filter_fn):
    for url in build_urls:
        if filter_fn(url):
            fname = os.path.basename(url.replace("%2B", "+").replace("%20", " "))
            logging.info("Will download %s to %s", fname, result_path)
            dowload_build_with_progress(url, os.path.join(result_path, fname))


def download_builds_filter(
    check_name, reports_path, result_path, filter_fn=lambda _: True
):
    build_name = get_build_name_for_check(check_name)
    urls = get_build_urls(build_name, reports_path)
    print(urls)

    if not urls:
        raise Exception("No build URLs found")

    download_builds(result_path, urls, filter_fn)


def download_all_deb_packages(check_name, reports_path, result_path):
    download_builds_filter(
        check_name, reports_path, result_path, lambda x: x.endswith("deb")
    )


def download_shared_build(check_name, reports_path, result_path):
    download_builds_filter(
        check_name, reports_path, result_path, lambda x: x.endswith("shared_build.tgz")
    )


def download_unit_tests(check_name, reports_path, result_path):
    download_builds_filter(
        check_name, reports_path, result_path, lambda x: x.endswith("unit_tests_dbms")
    )


def download_clickhouse_binary(check_name, reports_path, result_path):
    download_builds_filter(
        check_name, reports_path, result_path, lambda x: x.endswith("clickhouse")
    )


def download_performance_build(check_name, reports_path, result_path):
    download_builds_filter(
        check_name, reports_path, result_path, lambda x: x.endswith("performance.tgz")
    )
