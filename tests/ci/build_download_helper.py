#!/usr/bin/env python3

import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any, Callable, List

import requests  # type: ignore

import get_robot_token as grt  # we need an updated ROBOT_TOKEN
from ci_config import CI_CONFIG

DOWNLOAD_RETRIES_COUNT = 5


def get_with_retries(
    url: str,
    retries: int = DOWNLOAD_RETRIES_COUNT,
    sleep: int = 3,
    **kwargs: Any,
) -> requests.Response:
    logging.info(
        "Getting URL with %i tries and sleep %i in between: %s", retries, sleep, url
    )
    exc = Exception("A placeholder to satisfy typing and avoid nesting")
    for i in range(retries):
        try:
            response = requests.get(url, **kwargs)
            response.raise_for_status()
            return response
        except Exception as e:
            if i + 1 < retries:
                logging.info("Exception '%s' while getting, retry %i", e, i + 1)
                time.sleep(sleep)

            exc = e

    raise exc


def get_gh_api(
    url: str,
    retries: int = DOWNLOAD_RETRIES_COUNT,
    sleep: int = 3,
    **kwargs: Any,
) -> requests.Response:
    """It's a wrapper around get_with_retries that requests GH api w/o auth by
    default, and falls back to the get_best_robot_token in case of receiving
    "403 rate limit exceeded" error
    It sets auth automatically when ROBOT_TOKEN is already set by get_best_robot_token
    """

    def set_auth_header():
        if "headers" in kwargs:
            if "Authorization" not in kwargs["headers"]:
                kwargs["headers"][
                    "Authorization"
                ] = f"Bearer {grt.get_best_robot_token()}"
        else:
            kwargs["headers"] = {
                "Authorization": f"Bearer {grt.get_best_robot_token()}"
            }

    if grt.ROBOT_TOKEN is not None:
        set_auth_header()

    need_retry = False
    for _ in range(retries):
        try:
            response = get_with_retries(url, 1, sleep, **kwargs)
            response.raise_for_status()
            return response
        except requests.HTTPError as exc:
            if (
                exc.response.status_code == 403
                and b"rate limit exceeded"
                in exc.response._content  # pylint:disable=protected-access
            ):
                logging.warning(
                    "Received rate limit exception, setting the auth header and retry"
                )
                set_auth_header()
                need_retry = True
                break

    if need_retry:
        return get_with_retries(url, retries, sleep, **kwargs)


def get_build_name_for_check(check_name: str) -> str:
    return CI_CONFIG["tests_config"][check_name]["required_build"]  # type: ignore


def read_build_urls(build_name: str, reports_path: str) -> List[str]:
    for root, _, files in os.walk(reports_path):
        for f in files:
            if build_name in f:
                logging.info("Found build report json %s", f)
                with open(os.path.join(root, f), "r", encoding="utf-8") as file_handler:
                    build_report = json.load(file_handler)
                    return build_report["build_urls"]  # type: ignore
    return []


def download_build_with_progress(url: str, path: Path) -> None:
    logging.info("Downloading from %s to temp path %s", url, path)
    for i in range(DOWNLOAD_RETRIES_COUNT):
        try:
            response = get_with_retries(url, retries=1, stream=True)
            total_length = int(response.headers.get("content-length", 0))
            if path.is_file() and total_length and path.stat().st_size == total_length:
                logging.info(
                    "The file %s already exists and have a proper size %s",
                    path,
                    total_length,
                )
                return

            with open(path, "wb") as f:
                if total_length == 0:
                    logging.info(
                        "No content-length, will download file without progress"
                    )
                    f.write(response.content)
                else:
                    dl = 0

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


def download_builds(
    result_path: str, build_urls: List[str], filter_fn: Callable[[str], bool]
) -> None:
    for url in build_urls:
        if filter_fn(url):
            fname = os.path.basename(url.replace("%2B", "+").replace("%20", " "))
            logging.info("Will download %s to %s", fname, result_path)
            download_build_with_progress(url, Path(result_path) / fname)


def download_builds_filter(
    check_name, reports_path, result_path, filter_fn=lambda _: True
):
    build_name = get_build_name_for_check(check_name)
    urls = read_build_urls(build_name, reports_path)
    print(urls)

    if not urls:
        raise Exception("No build URLs found")

    download_builds(result_path, urls, filter_fn)


def download_all_deb_packages(check_name, reports_path, result_path):
    download_builds_filter(
        check_name, reports_path, result_path, lambda x: x.endswith("deb")
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
        check_name,
        reports_path,
        result_path,
        lambda x: x.endswith("performance.tar.zst"),
    )
