#!/usr/bin/env python3
"""Publish the trusted nightly reference snapshot artifact to Vercel Blob."""

import os
import shlex
import subprocess
import sys
from pathlib import Path

from ci.defs.defs import BASE_BRANCH
from praktika.info import Info
from praktika.result import Result
from praktika.utils import Shell, Utils


REPO_ROOT = Path(Utils.cwd())
ARCHIVE = REPO_ROOT / "ci" / "tmp" / "reference-snapshot.tar.zst"
PUBLISHER = REPO_ROOT / "ci" / "jobs" / "scripts" / "docs" / "reference_snapshot" / "publish_reference_snapshot.py"
SECRET_NAME = "clickhouse-vercel-reference-blob-read-write-token"
TOKEN_ENV = "VERCEL_REFERENCE_BLOB_READ_WRITE_TOKEN"


def publish_snapshot() -> bool:
    if not ARCHIVE.is_file():
        raise FileNotFoundError(f"Reference snapshot artifact is missing: {ARCHIVE}")
    # Check the remote tip immediately before resolving the writer credential
    # and replacing the shared Head object. A GitHub Actions rerun otherwise
    # replays its original SHA and could regress Head after master advances.
    if not on_current_base_branch():
        return False
    token = Info().get_secret(SECRET_NAME).get_value()
    environment = os.environ | {TOKEN_ENV: token}
    subprocess.run(
        [sys.executable, str(PUBLISHER), "--archive", str(ARCHIVE), "--token-env", TOKEN_ENV],
        check=True,
        env=environment,
    )
    return True


def on_current_base_branch() -> bool:
    """Refuse to publish unless HEAD is the current tip of the base branch."""
    branch = Info().git_branch
    if branch != BASE_BRANCH:
        print(
            "Refusing to publish the shared Head reference snapshot from "
            f"'{branch}': run this workflow against '{BASE_BRANCH}' instead "
            "(for example, a workflow_dispatch from the base branch).",
            file=sys.stderr,
        )
        return False

    if not Shell.check(
        f"git fetch --no-tags origin {shlex.quote(BASE_BRANCH)}", verbose=True
    ):
        print(
            f"Refusing to publish the shared Head reference snapshot: could not fetch "
            f"the current '{BASE_BRANCH}' tip.",
            file=sys.stderr,
        )
        return False

    head = Shell.get_output("git rev-parse HEAD").strip()
    current_base = Shell.get_output("git rev-parse FETCH_HEAD").strip()
    if not head or not current_base or head != current_base:
        print(
            f"Refusing to publish the shared Head reference snapshot: HEAD "
            f"({head or 'unknown'}) is not the current '{BASE_BRANCH}' tip "
            f"({current_base or 'unknown'}).",
            file=sys.stderr,
        )
        return False
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
