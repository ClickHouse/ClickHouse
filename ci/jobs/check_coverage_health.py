"""Validate that every shard of this nightly run published usable coverage."""

import json
from pathlib import Path

from ci.jobs.scripts.test_selection_config import SELECTION_CONFIG
from ci.praktika.info import Info
from ci.praktika.result import Result


def validate_exports(exports, sha, run_id, config=SELECTION_CONFIG):
    if len(exports) != config.coverage_shards:
        raise ValueError("Coverage run is missing shard export metadata")
    shards = set()
    for export in exports:
        if export["commit_sha"] != sha or export["coverage_run_id"] != str(run_id):
            raise ValueError("Coverage export belongs to a different workflow run")
        shards.add(export["shard"])
        if (
            export["status"] != "OK"
            or export.get("selector_smoke", {}).get("status") != "OK"
        ):
            raise ValueError(f"Shard failed coverage validation: {export['shard']}")
        if len(export["exported_tests"]) < config.min_exported_tests_per_shard:
            raise ValueError(f"Insufficient useful coverage: {export['shard']}")
        if (
            export["selector_smoke"]["path_version"] != config.path_version
            or not export["randomized_settings"]
        ):
            raise ValueError("Coverage export contract mismatch")
    expected = {
        f"{shard}/{config.coverage_shards}"
        for shard in range(1, config.coverage_shards + 1)
    }
    if shards != expected:
        raise ValueError(f"Missing or duplicate coverage shards: {shards}")


def main():
    info = Info()
    files = [
        Path(f"ci/tmp/coverage-export-{shard}.json")
        for shard in range(1, SELECTION_CONFIG.coverage_shards + 1)
    ]
    try:
        exports = [json.loads(path.read_text()) for path in files]
        validate_exports(exports, info.sha, info.run_id)
        result = Result.create_from(
            status=Result.Status.OK,
            info="All randomized coverage shards exported and passed selection smoke",
            files=[str(path) for path in files],
        )
    except Exception as ex:
        info.add_workflow_error(f"No healthy coverage run published: {ex}")
        result = Result.create_from(
            status=Result.Status.ERROR,
            info=str(ex),
            files=[str(path) for path in files if path.exists()],
        )
    result.complete_job()


if __name__ == "__main__":
    main()
