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
