"""
Bugfix validation aggregator.

This job depends on the four per-arch bugfix-validation result artifacts
produced by the per-arch sub-checks:

    - `Bugfix validation (functional tests, amd64)`
    - `Bugfix validation (functional tests, aarch64)`
    - `Bugfix validation (integration tests, amd64)`
    - `Bugfix validation (integration tests, aarch64)`

Each per-arch sub-check exits with the natural OK status (no
`force_success=True` flag is used) once it has run the test on master HEAD
and on the PR, captured the inverted result, and written a small JSON
artifact describing whether the new/modified test on that arch validated
the bug:

    {
        "validated": true | false,
        "info": "<human-readable description>",
        "arch": "amd64" | "aarch64",
        "kind": "functional tests" | "integration tests"
    }

The aggregator combines all four results and reports SUCCESS to GitHub iff
at least one arch validated the bug. This is the only bugfix-validation
check that blocks PR merge.

Artifact-not-found is treated as `validated == false` with a SKIPPED
sub-result, so an aggregator failure cannot be silently masked by the
absence of a per-arch JSON. Per-arch jobs that genuinely failed (e.g. due
to a sanitizer report or server crash) deliberately do not write the JSON
in that case, so the aggregator naturally falls back to SKIPPED for that
arch and the other arch can still validate the bug.
"""
import json
from pathlib import Path
from typing import Optional

from ci.defs.defs import ArtifactNames
from ci.praktika.result import Result
from ci.praktika.utils import Utils


TEMP_DIR = f"{Utils.cwd()}/ci/tmp"

# Maps each per-arch result-artifact name to a human-readable label.
PER_ARCH_RESULTS = [
    (ArtifactNames.BUGFIX_VALIDATE_FT_AMD_RESULT, "functional tests, amd64"),
    (ArtifactNames.BUGFIX_VALIDATE_FT_ARM_RESULT, "functional tests, aarch64"),
    (ArtifactNames.BUGFIX_VALIDATE_IT_AMD_RESULT, "integration tests, amd64"),
    (ArtifactNames.BUGFIX_VALIDATE_IT_ARM_RESULT, "integration tests, aarch64"),
]


def _load_per_arch_result(artifact_name: str) -> Optional[dict]:
    """Read a per-arch JSON result. Returns None if the file is missing.

    The praktika runner downloads each `requires`d artifact into TEMP_DIR
    using its base filename (`bugfix_validate_result.json`). When multiple
    artifacts share the same base filename, the runner places each into a
    name-prefixed subdirectory: `{TEMP_DIR}/{ARTIFACT_NAME}/...`.

    We probe both layouts so this script is robust to whichever scheme
    praktika ends up using when downloading multiple `bugfix_validate_results`
    artifacts that share the same on-disk path.
    """
    candidates = [
        Path(TEMP_DIR) / artifact_name / "bugfix_validate_result.json",
        Path(TEMP_DIR) / f"{artifact_name}.json",
        Path(TEMP_DIR) / "bugfix_validate_result.json",  # fallback
    ]
    for path in candidates:
        if path.is_file():
            try:
                with path.open("r") as f:
                    return json.load(f)
            except (OSError, json.JSONDecodeError) as e:
                print(f"WARN: failed to read {path}: {e}")
                return None
    return None


def main() -> int:
    sub_results = []
    any_validated = False
    any_present = False

    for artifact_name, label in PER_ARCH_RESULTS:
        data = _load_per_arch_result(artifact_name)
        if data is None:
            sub_results.append(
                Result(
                    name=label,
                    status=Result.Status.SKIPPED,
                    info=f"No result artifact found for {artifact_name}",
                )
            )
            print(f"[{label}] SKIPPED — no artifact at {artifact_name}")
            continue

        any_present = True
        validated = bool(data.get("validated", False))
        msg = str(data.get("info", ""))
        sub_results.append(
            Result(
                name=label,
                status=Result.Status.OK if validated else Result.Status.FAIL,
                info=msg,
            )
        )
        print(f"[{label}] {'VALIDATED' if validated else 'NOT VALIDATED'}: {msg}")
        if validated:
            any_validated = True

    overall = Result(
        name="Bugfix validation (final)",
        results=sub_results,
        status=Result.Status.OK if any_validated else Result.Status.FAIL,
    )

    if not any_present:
        overall.set_info(
            "No per-arch bugfix-validation result artifacts were available. "
            "This usually means all four per-arch jobs were skipped or did not "
            "emit their result file."
        )
    elif not any_validated:
        overall.set_info(
            "Bugfix validation could not be confirmed on any architecture. "
            "The new/modified test must FAIL on master HEAD AND PASS on the PR "
            "for at least one of {amd64, aarch64} to validate the fix."
        )
    else:
        validated_archs = [
            r.name for r in sub_results if r.status == Result.Status.OK
        ]
        overall.set_info(
            f"Validated on: {', '.join(validated_archs)}"
        )

    overall.complete_job()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
