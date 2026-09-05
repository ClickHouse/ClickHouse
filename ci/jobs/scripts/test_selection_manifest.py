import hashlib
import json
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

from ci.jobs.scripts.coverage_selection import validate_snapshots
from ci.jobs.scripts.test_selection_config import SELECTION_CONFIG

SELECTION_MANIFEST = Path("ci/tmp/stateless-selection.json")


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
        "selector_version": config.version,
        "coverage_path_version": config.path_version,
        "config": asdict(config),
    }
    for key, value in expected.items():
        if manifest.get(key) != value:
            raise ValueError(
                f"Selection manifest {key} mismatch: {manifest.get(key)!r} != {value!r}"
            )
    if manifest.get("canary", {}).get("status") != "OK":
        raise ValueError("Selection manifest has no successful coverage canary")
    validate_snapshots(
        manifest["coverage_snapshots"],
        datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        config,
    )
    if manifest["tests"] != [record["test"] for record in manifest["selected"]]:
        raise ValueError("Selection manifest list does not match candidate records")
    return manifest


def configuration_report(
    manifest,
    tests,
    job_name,
    events_path,
    exit_code=None,
    requested_repetitions=1,
    stop_reason=None,
):
    """Account for every base-selected test, including never-started tests."""
    records = {
        test: {
            "test": test,
            "selected": True,
            "compatible": test in tests,
            "events": [],
            "started": 0,
            "completed": 0,
            "repetitions": {},
        }
        for test in manifest["tests"]
    }
    if Path(events_path).exists():
        from ci.jobs.scripts.find_tests import Targeting

        for line in Path(events_path).read_text().splitlines():
            event = json.loads(line)
            name = Targeting.selection_test_name(event["test"])
            if name not in records:
                raise ValueError(
                    f"Runner executed a test outside the selection: {name}"
                )
            record = records[name]
            record["events"].append(event)
            state = event["event"]
            if state in ("started", "completed"):
                record[state] += 1
            if state == "filtered":
                record["compatible"] = False
                record["filter_reason"] = event["reason"]
            if state == "completed":
                fingerprint = event["settings_fingerprint"]
                record["repetitions"][fingerprint] = (
                    record["repetitions"].get(fingerprint, 0) + 1
                )
    for record in records.values():
        if not record["compatible"] and "filter_reason" not in record:
            record["filter_reason"] = "parallel/sequential flavor"
        record["repeated"] = record["completed"] > 1
        record["incomplete"] = record["compatible"] and (
            record["completed"] == 0 or record["started"] > record["completed"]
        )
    return {
        "configuration": job_name,
        "selector_version": manifest["selector_version"],
        "manifest_sha256": hashlib.sha256(
            json.dumps(manifest, sort_keys=True).encode()
        ).hexdigest(),
        "base_selection_count": len(manifest["tests"]),
        "flavor_selection_count": len(tests),
        "runner_exit_code": exit_code,
        "requested_repetitions": requested_repetitions,
        "stop_reason": stop_reason,
        "tests": list(records.values()),
    }
