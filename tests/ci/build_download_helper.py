#!/usr/bin/env python3

import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any, Callable, List, Optional, Union

import requests

from ci_config import CI

try:
    # A work around for scripts using this downloading module without required deps
    import get_robot_token as grt  # we need an updated ROBOT_TOKEN
except ImportError:

    class grt:  # type: ignore
        ROBOT_TOKEN = None

        @staticmethod
        def get_best_robot_token() -> str:
            return ""


DOWNLOAD_RETRIES_COUNT = 5

logger = logging.getLogger(__name__)


class DownloadException(Exception):
    pass


class APIException(Exception):
    pass


def get_with_retries(
    url: str,
    retries: int = DOWNLOAD_RETRIES_COUNT,
    sleep: int = 3,
    **kwargs: Any,
) -> requests.Response:
    logger.info(
        "Getting URL with %i tries and sleep %i in between: %s", retries, sleep, url
    )
    exc = Exception("A placeholder to satisfy typing and avoid nesting")
    timeout = kwargs.pop("timeout", 30)
    for i in range(retries):
        try:
            response = requests.get(url, timeout=timeout, **kwargs)
            response.raise_for_status()
            return response
        except Exception as e:
            if i + 1 < retries:
                logger.info("Exception '%s' while getting, retry %i", e, i + 1)
                time.sleep(sleep)

            exc = e

    raise exc


def get_gh_api(
    url: str,
    retries: int = DOWNLOAD_RETRIES_COUNT,
    sleep: int = 3,
    **kwargs: Any,
) -> requests.Response:
    """
    Request GH api w/o auth by default, and failover to the get_best_robot_token in case of receiving
    "403 rate limit exceeded" or "404 not found" error
    It sets auth automatically when ROBOT_TOKEN is already set by get_best_robot_token
    """

    def set_auth_header():
        headers = kwargs.get("headers", {})
        if "Authorization" not in headers:
            headers["Authorization"] = f"Bearer {grt.get_best_robot_token()}"
        kwargs["headers"] = headers

    if grt.ROBOT_TOKEN is not None:
        set_auth_header()

    token_is_set = "Authorization" in kwargs.get("headers", {})
    exc = Exception("A placeholder to satisfy typing and avoid nesting")
    try_cnt = 0
    timeout = kwargs.pop("timeout", 30)
    while try_cnt < retries:
        try_cnt += 1
        try:
            response = requests.get(url, timeout=timeout, **kwargs)
            response.raise_for_status()
            return response
        except requests.HTTPError as e:
            exc = e
            ratelimit_exceeded = (
                e.response.status_code == 403
                and b"rate limit exceeded"
                # pylint:disable-next=protected-access
                in (e.response._content or b"")
            )
            try_auth = e.response.status_code == 404
            if (ratelimit_exceeded or try_auth) and not token_is_set:
                logger.warning(
                    "Received rate limit exception, setting the auth header and retry"
                )
                set_auth_header()
                token_is_set = True
                try_cnt = 0
                continue
        except Exception as e:
            exc = e

        if try_cnt < retries:
            logger.info("Exception '%s' while getting, retry %i", exc, try_cnt)
            time.sleep(sleep)

    raise APIException(f"Unable to request data from GH API: {url}") from exc


def read_build_urls(build_name: str, reports_path: Union[Path, str]) -> List[str]:
    for root, _, files in os.walk(reports_path):
        for file in files:
            if file.endswith(f"_{build_name}.json"):
                logger.info("Found build report json %s for %s", file, build_name)
                with open(
                    os.path.join(root, file), "r", encoding="utf-8"
                ) as file_handler:
                    build_report = json.load(file_handler)
                    return build_report["build_urls"]  # type: ignore

    logger.info("A build report is not found for %s", build_name)
    return []


def download_build_with_progress(url: str, path: Path) -> None:
    logger.info("Downloading from %s to temp path %s", url, path)
    for i in range(DOWNLOAD_RETRIES_COUNT):
        try:
            response = get_with_retries(url, retries=1, stream=True)
            total_length = int(response.headers.get("content-length", 0))
            if path.is_file() and total_length and path.stat().st_size == total_length:
                logger.info(
                    "The file %s already exists and have a proper size %s",
                    path,
                    total_length,
                )
                return

            with open(path, "wb") as f:
                if total_length == 0:
                    logger.info(
                        "No content-length, will download file without progress"
                    )
                    f.write(response.content)
                else:
                    dl = 0

                    logger.info("Content length is %ld bytes", total_length)
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
        except Exception as e:
            if sys.stdout.isatty():
                sys.stdout.write("\n")
            if path.exists():
                path.unlink()

            if i + 1 < DOWNLOAD_RETRIES_COUNT:
                time.sleep(3)
            else:
                raise DownloadException(
                    f"Cannot download dataset from {url}, all retries exceeded"
                ) from e

    if sys.stdout.isatty():
        sys.stdout.write("\n")
    logger.info("Downloading finished")


def download_builds(
    result_path: Path, build_urls: List[str], filter_fn: Callable[[str], bool]
) -> None:
    for url in build_urls:
        if filter_fn(url):
            fname = os.path.basename(url.replace("%2B", "+").replace("%20", " "))
            logger.info("Will download %s to %s", fname, result_path)
            download_build_with_progress(url, result_path / fname)


def download_builds_filter(
    check_name: str,
    reports_path: Union[Path, str],
    result_path: Path,
    filter_fn: Callable[[str], bool] = lambda _: True,
) -> None:
    build_name = CI.get_required_build_name(check_name)
    urls = read_build_urls(build_name, reports_path)
    logger.info("The build report for %s contains the next URLs: %s", build_name, urls)

    if not urls:
        raise DownloadException("No build URLs found")

    download_builds(result_path, urls, filter_fn)


def download_all_deb_packages(
    check_name: str, reports_path: Union[Path, str], result_path: Path
) -> None:
    download_builds_filter(
        check_name, reports_path, result_path, lambda x: x.endswith("deb")
    )


def download_unit_tests(
    check_name: str, reports_path: Union[Path, str], result_path: Path
) -> None:
    download_builds_filter(
        check_name, reports_path, result_path, lambda x: x.endswith("unit_tests_dbms")
    )


def download_clickhouse_binary(
    check_name: str, reports_path: Union[Path, str], result_path: Path
) -> None:
    download_builds_filter(
        check_name, reports_path, result_path, lambda x: x.endswith("clickhouse")
    )


def get_clickhouse_binary_url(
    check_name: str, reports_path: Union[Path, str]
) -> Optional[str]:
    build_name = CI.get_required_build_name(check_name)
    urls = read_build_urls(build_name, reports_path)
    logger.info("The build report for %s contains the next URLs: %s", build_name, urls)
    for url in urls:
        check_url = url
        if "?" in check_url:
            check_url = check_url.split("?")[0]

        if check_url.endswith("clickhouse"):
            return url

    return None


def download_performance_build(
    check_name: str, reports_path: Union[Path, str], result_path: Path
) -> None:
    download_builds_filter(
        check_name,
        reports_path,
        result_path,
        lambda x: x.endswith("performance.tar.zst"),
    )


def download_fuzzers(
    check_name: str, reports_path: Union[Path, str], result_path: Path
) -> None:
    download_builds_filter(
        check_name,
        reports_path,
        result_path,
        lambda x: x.endswith(("_fuzzer", ".dict", ".options", "_seed_corpus.zip")),
    )
