#!/usr/bin/env python3
"""Publish the trusted nightly reference snapshot artifact to Vercel Blob."""

import os
import subprocess
import sys
from pathlib import Path

from ci.defs.defs import BASE_BRANCH
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


def on_base_branch() -> bool:
    """Refuse to replace the shared Head archive from a feature branch."""
    branch = Info().git_branch
    if branch != BASE_BRANCH:
        print(
            "Refusing to publish the shared Head reference snapshot from "
            f"'{branch}': run this workflow against '{BASE_BRANCH}' instead "
            "(for example, a workflow_dispatch from the base branch).",
            file=sys.stderr,
        )
        return False
    return True


if __name__ == "__main__":
    os.chdir(REPO_ROOT)
    results = [
        Result.from_commands_run(
            name="Run against base branch", command=on_base_branch
        )
    ]
    # A scheduled workflow also exposes workflow_dispatch. Fail closed when it
    # is run from another branch, before resolving the Vercel Blob writer.
    if results[-1].is_ok():
        results.append(
            Result.from_commands_run(
                name="Publish reference snapshot to Vercel Blob",
                command=publish_snapshot,
                with_info_on_failure=True,
            )
        )
    Result.create_from(results=results).complete_job()
