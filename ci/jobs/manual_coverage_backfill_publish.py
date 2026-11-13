"""One-off user-authorized manifest publication; this branch must not be merged."""

import argparse
import hashlib
import json
from pathlib import Path

import requests

from ci.jobs.scripts.performance_selection import validate_phases
from ci.praktika.result import Result
from ci.praktika.s3 import S3

SHA = "acd88ab8ffeee047001d2bbc14be30ddcff11df2"
PREFIX = f"clickhouse-test-reports/REFs/master/{SHA}/nightlycoverage/performance_comparison_amd_llvm_coverage_per_test_per_query_coverage_16_16"
URL = f"https://s3.amazonaws.com/{PREFIX}/manifest.json"
PAYLOAD = Path(__file__).with_name("manual_coverage_backfill_manifest.json")


def publish(check_only=False):
    payload = PAYLOAD.read_bytes()
    manifest = json.loads(payload)
    audit = manifest["temporary_manual_backfill"]
    assert audit["user_authorized"] and not audit["is_new_verified_collection"]
    assert manifest["sha"] == SHA and manifest["sharding"]["shard"] == 16
    assert (
        manifest["tests"]["limit_zero_primary_key_analysis.xml"]["status"] == "skipped"
    )
    assert sum(t["status"] == "collected" for t in manifest["tests"].values()) == 56
    response = requests.get(
        "https://play.clickhouse.com/",
        params={
            "user": "play",
            "query": """SELECT check_start_time,check_name,test_name,count() AS rows,
                countIf(empty(file) OR line_start=0 OR line_start>line_end OR branch_flag NOT IN (0,1,2)) AS invalid_rows
                FROM checks_coverage_lines
                WHERE check_start_time=toDateTime('2026-11-13 03:00:55','UTC')
                  AND check_name='Performance Comparison (amd_llvm_coverage_per_test, per_query_coverage, 16/16)'
                GROUP BY check_start_time,check_name,test_name FORMAT JSONEachRow""",
        },
        timeout=120,
    )
    response.raise_for_status()
    rows = [json.loads(line) for line in response.text.splitlines() if line]
    validate_phases(rows, [manifest])
    current = requests.get(URL, timeout=60)
    current.raise_for_status()
    if current.content == payload:
        print("The exact temporary manifest is already published.")
        return
    assert (
        hashlib.sha256(current.content).hexdigest() == audit["original_manifest_sha256"]
    )
    assert "ETag" in current.headers
    if check_only:
        print(
            "All target phases and the original manifest match; no S3 write requested."
        )
        return
    backup = Path("ci/tmp/original_coverage_manifest.json")
    backup.parent.mkdir(parents=True, exist_ok=True)
    backup.write_bytes(current.content)
    backup_key = f"{PREFIX}/manifest.before_user_backfill_2026_11_13.json"
    if not S3.put(backup_key, backup, text=True, if_none_matched=True):
        previous_backup = requests.get(
            f"https://s3.amazonaws.com/{backup_key}", timeout=60
        )
        previous_backup.raise_for_status()
        assert previous_backup.content == current.content
    assert S3.put(
        f"{PREFIX}/manifest.json",
        PAYLOAD,
        text=True,
        if_match=current.headers["ETag"],
        metadata={
            "temporary-user-approved-backfill": "true",
            "not-verified-new-collection": "true",
        },
    )
    published = requests.get(URL, timeout=60)
    published.raise_for_status()
    assert published.content == payload
    print(
        "Published temporary manifest after phase validation; original manifest preserved."
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--check-only", action="store_true")
    args = parser.parse_args()
    publish(args.check_only)
    if not args.check_only:
        Result.create_from(
            status=Result.Status.OK,
            info="User-authorized temporary backfill manifest; proper fresh collection still required",
        ).complete_job()
