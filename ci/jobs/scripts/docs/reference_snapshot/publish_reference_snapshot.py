#!/usr/bin/env python3
"""Publish the current Head reference snapshot to private Vercel Blob.

This job deliberately owns one mutable object:
``snapshots/head/reference-snapshot.tar.zst``. It is replaced by the nightly
build and is the sole build input for the Head reference deployment. Release
snapshots are a separate release-time archival concern; the nightly workflow
does not create an unbounded collection of snapshot directories.
"""

import argparse
import os
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


VERCEL_BLOB_API_URL = "https://vercel.com/api/blob"
VERCEL_BLOB_API_VERSION = "12"
HEAD_SNAPSHOT_PATH = "snapshots/head/reference-snapshot.tar.zst"
CONTENT_TYPE = "application/zstd"


def blob_store_id(token: str) -> str:
    # Vercel Blob read/write tokens encode the store id as their fourth
    # underscore-delimited component (the SDK uses the same parsing rule).
    parts = token.split("_")
    if len(parts) < 4 or not parts[3]:
        raise ValueError("Invalid Vercel Blob read/write token")
    return parts[3]


class VercelBlob:
    def __init__(self, token: str, api_url: str = VERCEL_BLOB_API_URL) -> None:
        self.token = token
        self.api_url = api_url.rstrip("/")
        self.store_id = blob_store_id(token)

    def put_head_snapshot(self, archive: Path) -> None:
        if not archive.is_file():
            raise FileNotFoundError(f"Reference snapshot artifact is missing: {archive}")
        request = Request(
            f"{self.api_url}/?pathname={HEAD_SNAPSHOT_PATH}",
            data=archive.read_bytes(),
            method="PUT",
            headers={
                "Authorization": f"Bearer {self.token}",
                "Content-Type": CONTENT_TYPE,
                "X-Api-Version": VERCEL_BLOB_API_VERSION,
                "X-Vercel-Blob-Store-Id": self.store_id,
                "X-Vercel-Blob-Access": "private",
                "X-Add-Random-Suffix": "0",
                "X-Allow-Overwrite": "1",
                "X-Content-Type": CONTENT_TYPE,
            },
        )
        last_error: Exception | None = None
        for attempt in range(3):
            try:
                with urlopen(request, timeout=60):
                    return
            except HTTPError as error:
                if error.code < 500:
                    body = error.read().decode("utf-8", errors="replace")
                    raise RuntimeError(f"Vercel Blob upload failed for {HEAD_SNAPSHOT_PATH}: HTTP {error.code}: {body}") from error
                last_error = error
            except URLError as error:
                last_error = error
            time.sleep(2**attempt)
        raise RuntimeError(f"Vercel Blob upload failed for {HEAD_SNAPSHOT_PATH}: {last_error}") from last_error


def publish(archive: Path, token: str, api_url: str = VERCEL_BLOB_API_URL) -> str:
    VercelBlob(token, api_url).put_head_snapshot(archive)
    return HEAD_SNAPSHOT_PATH


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", required=True, type=Path, help="Praktika reference-snapshot.tar.zst artifact")
    parser.add_argument("--token-env", default="VERCEL_REFERENCE_BLOB_READ_WRITE_TOKEN", help="Environment variable containing the Vercel Blob read/write token")
    parser.add_argument("--api-url", default=VERCEL_BLOB_API_URL, help="Vercel Blob API URL (test override)")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    token = os.environ.get(args.token_env)
    if not token:
        raise SystemExit(f"Required Vercel Blob token environment variable is unset: {args.token_env}")
    pathname = publish(args.archive, token, args.api_url)
    print(f"Published Head reference snapshot: {pathname}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
