#!/usr/bin/env python3
from pathlib import Path

from ci.jobs.scripts.clickhouse_version import CHVersion
from ci.praktika._environment import _Environment
from ci.praktika.result import Result
from ci.praktika.s3 import S3
from ci.praktika.settings import Settings
from ci.praktika.utils import Shell, Utils

REPO_PATH = Path(Utils.cwd())

# The source tarball is published next to the release build's artifacts so that
# the legacy S3 layout (consumed outside this repo) keeps working.
S3_SUBDIR = "build_amd_release"


def checkout_submodules():
    return Shell.check(
        "git submodule sync && git submodule init", verbose=True
    ) and Shell.check(
        "contrib/update-submodules.sh --max-procs 10", verbose=True, retries=3
    )


def main():
    stopwatch = Utils.Stopwatch()

    version = CHVersion.get_version()
    tarball = REPO_PATH.parent / f"clickhouse-{version}.src.tar.gz"

    results = [
        Result.from_commands_run(
            name="Checkout submodules",
            command=checkout_submodules,
        )
    ]

    if results[-1].is_ok():
        results.append(
            Result.from_commands_run(
                name="Create source tar",
                command=f"tar czf {tarball} -C {REPO_PATH.parent} {REPO_PATH.name}",
            )
        )

    link = ""
    if results[-1].is_ok():
        s3_path = f"{Settings.S3_ARTIFACT_PATH}/{_Environment.get().get_s3_prefix()}/{S3_SUBDIR}"
        link = S3.copy_file_to_s3(s3_path=s3_path, local_path=str(tarball))

    Result.create_from(
        results=results,
        stopwatch=stopwatch,
        links=[link] if link else None,
    ).complete_job()


if __name__ == "__main__":
    main()
