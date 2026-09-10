import json
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

from ci.jobs.scripts.coverage_selection import validate_snapshots
from ci.jobs.scripts.test_selection_config import SELECTION_CONFIG

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
    }
    if coverage_files:
        manifest.update(
            {
                "cutoff": diagnostics["cutoff"],
                "coverage_snapshots": diagnostics["coverage_snapshots"],
                "canary": {"status": diagnostics["canary"]["status"]},
            }
        )
    if diagnostics["missing_tests"]:
        manifest["missing_tests"] = [
            record["test"] for record in diagnostics["missing_tests"]
        ]
    return manifest


def selection_cache_key(info, config=SELECTION_CONFIG):
    if not str(info.run_id).isdigit() or int(info.run_id) <= 0 or info.run_attempt <= 0:
        raise ValueError("Test selection requires a workflow run ID and attempt")
    return (
        f"PRs/{info.pr_number}/{info.sha}/test-selection/"
        f"{config.version}/{info.run_id}/{info.run_attempt}.json"
    )


def cached_manifest(client, bucket, key, produce):
    from botocore.exceptions import ClientError

    try:
        return json.loads(client.get_object(Bucket=bucket, Key=key)["Body"].read())
    except ClientError as ex:
        if ex.response["Error"]["Code"] != "NoSuchKey":
            raise
    manifest = produce()
    try:
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(manifest).encode(),
            ContentType="application/json",
            Tagging="retention=default",
            IfNoneMatch="*",
        )
        return manifest
    except ClientError as ex:
        if ex.response["Error"]["Code"] != "PreconditionFailed":
            raise
        # A concurrent producer won the conditional insert; every lane uses its list.
        return json.loads(client.get_object(Bucket=bucket, Key=key)["Body"].read())


def load_selection(info, config=SELECTION_CONFIG):
    manifest = json.loads(SELECTION_MANIFEST.read_text())
    expected = {
        "commit_sha": info.sha,
        "pr_number": info.pr_number,
        "workflow_run_id": str(info.run_id),
        "workflow_run_attempt": info.run_attempt,
        "selector_version": config.version,
        "coverage_path_version": config.path_version,
        "config": asdict(config),
    }
    for key, value in expected.items():
        if manifest.get(key) != value:
            raise ValueError(
                f"Selection manifest {key} mismatch: {manifest.get(key)!r} != {value!r}"
            )
    if manifest["coverage_lines"]:
        if manifest.get("canary", {}).get("status") != "OK":
            raise ValueError("Selection manifest has no successful coverage canary")
        validate_snapshots(
            manifest["coverage_snapshots"],
            datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            config,
        )
    tests = [record["test"] for record in manifest["tests"]]
    if len(tests) != len(set(tests)):
        raise ValueError("Selection manifest contains duplicate tests")
    return manifest
