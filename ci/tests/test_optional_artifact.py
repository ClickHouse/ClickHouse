"""
Regression tests for optional S3 artifacts.

Background
----------
LLVM coverage jobs (Integration / Functional / Unit tests, amd_llvm_coverage)
run their coverage merge non-blocking: `llvm-profdata merge` can crash on a
corrupt .profraw and produce no .profdata, but the tests themselves all pass
and the job is meant to stay green ("do not block pipeline").

However each such job also declares `provides=[LLVM_COVERAGE_FILE_*]` with
path `./*.profdata`. The praktika post-run artifact upload treated a missing
providing artifact as a hard error and reddened the whole job to
check_status=error even though 0 tests failed. Observed on PRs #109097,
#108909, #104217, #97032 (all 2026-07-02): job.log shows
`ERROR: Failed to create final coverage file` (llvm-profdata SIGSEGV) then
`ERROR: Failed to upload artifact [LLVM_COVERAGE_FILE_it_6:./*.profdata]`.

Fix
---
`Artifact.Config` gained an `optional` flag. When a providing artifact marked
optional matches no file at upload time, the runner skips it with a warning
instead of setting Result.Status.ERROR. The coverage profdata artifacts are
marked optional; the downstream `LLVM Coverage` aggregation already globs
whatever .profdata files exist, so a missing batch is tolerated.

A missing optional artifact is skipped with a warning on any run (PR, master or
release); a missing non-optional artifact is still an error.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.praktika.artifact import Artifact
from ci.praktika.runner import Runner
from ci.defs.defs import ArtifactConfigs, ArtifactNames


def test_optional_defaults_to_false():
    """Existing artifacts stay non-optional (backward compatible)."""
    a = Artifact.Config(name="x", type=Artifact.Type.S3, path="./x")
    assert a.optional is False


def test_optional_field_is_settable():
    a = Artifact.Config(name="x", type=Artifact.Type.S3, path="./x", optional=True)
    assert a.optional is True


def test_coverage_profdata_artifacts_are_optional():
    """Every LLVM_COVERAGE_FILE* profdata artifact must be optional.

    These are produced by a non-blocking merge that can emit no file; a
    missing file must not redden a coverage job whose tests all passed.
    """
    profdata_artifacts = [
        a
        for a in ArtifactConfigs.llvm_profdata_file
        if a.name.startswith(ArtifactNames.LLVM_COVERAGE_FILE)
    ]
    # There is one per FT/IT batch plus the base name - guard against an empty
    # match (which would make this test vacuously pass).
    assert len(profdata_artifacts) >= 3
    for a in profdata_artifacts:
        assert a.optional is True, f"coverage artifact [{a.name}] must be optional"
        assert a.path == ["./*.profdata"]


def test_runner_skips_missing_optional_artifact():
    """A missing optional artifact is skipped with a warning on any run."""
    a = Artifact.Config(
        name="LLVM_COVERAGE_FILE_it_6",
        type=Artifact.Type.S3,
        path="./*.profdata",
        optional=True,
    )
    assert Runner._skip_missing_optional_artifact(a, "./*.profdata") is True


def test_runner_does_not_skip_missing_required_artifact():
    """A missing non-optional artifact is still an error."""
    a = Artifact.Config(
        name="CH_AMD_DEBUG",
        type=Artifact.Type.S3,
        path="./clickhouse",
        optional=False,
    )
    assert Runner._skip_missing_optional_artifact(a, "./clickhouse") is False


if __name__ == "__main__":
    test_optional_defaults_to_false()
    test_optional_field_is_settable()
    test_coverage_profdata_artifacts_are_optional()
    test_runner_skips_missing_optional_artifact()
    test_runner_does_not_skip_missing_required_artifact()
    print("All optional-artifact tests passed")
