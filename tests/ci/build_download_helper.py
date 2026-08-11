#!/usr/bin/env python3
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any

import requests

# realpath, not abspath: this module is reached through a /usr/bin symlink, where abspath
# derives "/" as the repository root and the import below fails.
sys.path.insert(
    0, os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "..")
)

try:
    # Guarded like grt below: importers needing only the download helpers still work
    # where ci/ is absent, and get_gh_api fails when it is actually called.
    from ci.praktika.gh import GH
except ImportError:
    GH = None  # type: ignore[assignment]

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
# Cap the exponential backoff so that increasing the number of retries extends
# the total time we keep trying through a transient outage instead of exploding
# the per-attempt sleep.
DOWNLOAD_RETRY_MAX_BACKOFF = 60

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

    if GH is None:
        raise APIException(
            "praktika is not importable, so the GH API retry policy is unavailable"
        )

    # Mutated in place and passed by reference: the retry loop expands its kwargs once,
    # so rebinding this name would not reach the requests the failover must authorize.
    headers = kwargs.pop("headers", None) or {}

    def set_auth_header():
        if "Authorization" not in headers:
            headers["Authorization"] = f"Bearer {grt.get_best_robot_token()}"

    if grt.ROBOT_TOKEN is not None:
        set_auth_header()

    token_is_set = "Authorization" in headers

    def failover(e: requests.HTTPError) -> bool:
        """Set the robot token once and grant a fresh attempt budget."""
        nonlocal token_is_set
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
            return True
        return False

    timeout = kwargs.pop("timeout", 30)
    try:
        return GH.api_get(
            url,
            retries=retries,
            sleep=sleep,
            timeout=timeout,
            on_http_error=failover,
            headers=headers,
            **kwargs,
        )
    except RuntimeError as e:
        # Callers catch APIException to degrade instead of failing, so the praktika error
        # class must not leak out of here.
        raise APIException(f"Unable to request data from GH API: {url}") from e


def download_build_with_progress(
    url: str, path: Path, retries: int = DOWNLOAD_RETRIES_COUNT
) -> None:
    logger.info("Downloading from %s to temp path %s", url, path)
    for i in range(retries):
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
                dl = 0
                if total_length == 0:
                    logger.info(
                        "No content-length, will download file without progress"
                    )
                    content = response.content
                    dl = len(content)
                    f.write(content)
                else:
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

            # A truncated response is a transient failure too: without this check a
            # short read produces a corrupt file that only fails much later (e.g. as
            # an opaque `dpkg` error), hiding the real download problem.
            if total_length and dl != total_length:
                raise DownloadException(
                    f"Downloaded {dl} of {total_length} bytes from {url}"
                )
            break
        except Exception as e:
            if sys.stdout.isatty():
                sys.stdout.write("\n")
            if path.exists():
                path.unlink()

            # A 404 means the artifact genuinely does not exist (e.g. packages were
            # not uploaded for this tag yet). Retrying cannot help, so fail fast with
            # a clear, attributable message instead of burning the whole retry budget.
            status_code = getattr(getattr(e, "response", None), "status_code", None)
            if status_code == 404:
                raise DownloadException(
                    f"Cannot download {url}: the file does not exist (HTTP 404)"
                ) from e

            if i + 1 < retries:
                sleep_time = min(3 * (2**i), DOWNLOAD_RETRY_MAX_BACKOFF)
                logger.warning(
                    "Download attempt %i of %i for %s failed (%s), retrying in %i seconds",
                    i + 1,
                    retries,
                    url,
                    e,
                    sleep_time,
                )
                time.sleep(sleep_time)
            else:
                raise DownloadException(
                    f"Cannot download {url}, all {retries} retries exceeded; "
                    f"last error: {e}"
                ) from e

    if sys.stdout.isatty():
        sys.stdout.write("\n")
    logger.info("Downloading finished")
