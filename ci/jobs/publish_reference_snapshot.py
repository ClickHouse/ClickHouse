#!/usr/bin/env python3
"""Publish the trusted nightly reference snapshot artifact to Vercel Blob."""

import os
import subprocess
import sys
from pathlib import Path

from praktika.info import Info
from praktika.result import Result
from praktika.utils import Utils


REPO_ROOT = Path(Utils.cwd())
ARCHIVE = REPO_ROOT / "ci" / "tmp" / "reference-snapshot.tar.zst"
PUBLISHER = REPO_ROOT / "ci" / "jobs" / "scripts" / "docs" / "reference_snapshot" / "publish_reference_snapshot.py"
SECRET_NAME = "vercel-reference-blob-read-write-token"
TOKEN_ENV = "VERCEL_REFERENCE_BLOB_READ_WRITE_TOKEN"


def publish_snapshot() -> bool:
    if not ARCHIVE.is_file():
        raise FileNotFoundError(f"Reference snapshot artifact is missing: {ARCHIVE}")
    token = Info().get_secret(SECRET_NAME).get_value()
    environment = os.environ | {TOKEN_ENV: token}
    subprocess.run(
        [sys.executable, str(PUBLISHER), "--archive", str(ARCHIVE), "--token-env", TOKEN_ENV],
        check=True,
        env=environment,
    )
    return True


if __name__ == "__main__":
    os.chdir(REPO_ROOT)
    Result.create_from(
        results=[
            Result.from_commands_run(
                name="Publish reference snapshot to Vercel Blob",
                command=publish_snapshot,
                with_info_on_failure=True,
            )
        ]
    ).complete_job()
