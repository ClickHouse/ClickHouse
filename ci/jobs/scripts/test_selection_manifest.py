from pathlib import Path

# Written by every targeted job and attached to its report; nothing reads it back.
SELECTION_MANIFEST = Path("ci/tmp/stateless-selection.json")


def selection_manifest(diagnostics, info):
    """Keep selection inputs and scoring evidence without duplicated diagnostics."""

    def test_record(record):
        result = {
            "test": record["test"],
            "sources": record["sources"] if "sources" in record else [record["source"]],
        }
        if "score" in record:
            result["score"] = record["score"]
        if record.get("features"):
            result["coverage"] = [
                {
                    "region": feature["region"],
                    "owners": feature["region_owners"],
                    "exact_lines": feature["exact_lines"],
                    "hunks": feature["hunks"],
                    "runs": feature["coverage_run_frequency"],
                }
                for feature in record["features"]
            ]
        return result

    coverage_files = {path for path, _ in diagnostics["coverage_lines"]}
    manifest = {
        "pr_number": info.pr_number,
        "commit_sha": info.sha,
        "diff_base_sha": diagnostics["diff_base_sha"],
        "workflow_run_id": str(info.run_id),
        "workflow_run_attempt": info.run_attempt,
        "selector_version": diagnostics["selector_version"],
        "coverage_path_version": diagnostics["coverage_path_version"],
        "config": diagnostics["config"],
        "coverage_lines": diagnostics["coverage_lines"],
        "hunk_ranges": {
            path: ranges
            for path, ranges in diagnostics["hunk_ranges"].items()
            if path in coverage_files
        },
        "tests": [test_record(record) for record in diagnostics["selected"]],
        "rejected": [
            {**test_record(record), "reason": record["admission_reason"]}
            for record in diagnostics["rejected"]
        ],
        "mandatory_overflow": diagnostics["mandatory_overflow"],
        "cutoff": diagnostics["cutoff"],
    }
    if coverage_files:
        manifest.update(
            {
                "coverage_cutoff": diagnostics["coverage_cutoff"],
                "coverage_snapshots": diagnostics["coverage_snapshots"],
                "canary": {"status": diagnostics["canary"]["status"]},
            }
        )
    if diagnostics["missing_tests"]:
        manifest["missing_tests"] = [
            record["test"] for record in diagnostics["missing_tests"]
        ]
    return manifest
