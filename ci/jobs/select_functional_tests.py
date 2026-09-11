"""Produce the one selection artifact consumed by every PR stateless lane."""

import json

from ci.jobs.scripts.find_tests import Targeting
from ci.jobs.scripts.test_selection_manifest import (
    SELECTION_MANIFEST,
    cached_manifest,
    load_selection,
    selection_cache_key,
    selection_manifest,
)
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.s3 import S3
from ci.praktika.settings import Settings


def get_selection(info, targeter):
    """Share a fresh selection for each attempt, including failed-job-only reruns."""
    targeter.job_type = Targeting.STATELESS_JOB_TYPE
    SELECTION_MANIFEST.parent.mkdir(parents=True, exist_ok=True)

    def produce():
        targeter.get_all_relevant_tests_with_info(include_changed_tests=True)
        return selection_manifest(targeter.selection_diagnostics, info)

    manifest = cached_manifest(
        S3._ensure_boto3(),
        Settings.S3_ARTIFACT_BUCKET,
        selection_cache_key(info, targeter.config),
        produce,
    )
    SELECTION_MANIFEST.write_text(json.dumps(manifest, indent=2) + "\n")
    return load_selection(info)


def main():
    info = Info()
    targeter = Targeting(info)
    try:
        manifest = get_selection(info, targeter)
        result = Result.create_from(
            status=Result.Status.OK,
            info=f"Selected {len(manifest['tests'])} tests for {info.sha}",
            files=[str(SELECTION_MANIFEST)],
        )
    except Exception as ex:
        failure = SELECTION_MANIFEST.with_name("stateless-selection-error.json")
        failure.write_text(
            json.dumps({**targeter.selection_diagnostics, "error": str(ex)}, indent=2)
            + "\n"
        )
        result = Result.create_from(
            status=Result.Status.ERROR,
            info=f"Test selection failed: {ex}",
            files=[str(failure)],
        )
    result.complete_job()


if __name__ == "__main__":
    main()
