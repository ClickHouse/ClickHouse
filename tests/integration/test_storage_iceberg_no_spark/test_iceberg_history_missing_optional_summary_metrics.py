import json
import re

import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    get_uuid_str,
)


def _metadata_dir(table_name):
    return f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}/metadata"


def _read_latest_metadata(instance, table_name):
    metadata_dir = _metadata_dir(table_name)
    latest = instance.exec_in_container(
        ["bash", "-c", f"ls -v {metadata_dir}/v*.metadata.json | tail -1"]
    ).strip()
    raw = instance.exec_in_container(["cat", latest])
    return json.loads(raw), latest


def _write_next_metadata(instance, table_name, meta, prev_path):
    metadata_dir = _metadata_dir(table_name)
    version_match = re.search(r"/v(\d+)[^/]*\.metadata\.json$", prev_path)
    new_version = int(version_match.group(1)) + 1
    new_path = f"{metadata_dir}/v{new_version}.metadata.json"
    new_content = json.dumps(meta, indent=4)
    instance.exec_in_container(
        ["bash", "-c", f"cat > {new_path} << 'JSONEOF'\n{new_content}\nJSONEOF"]
    )


@pytest.mark.parametrize("format_version", [1, 2])
def test_iceberg_history_missing_optional_summary_metrics(
    started_cluster_iceberg_no_spark, format_version
):
    """Reading a snapshot summary that omits the spec-optional metrics `added-files-size`
    and `changed-partition-count` must not throw. `OPTIMIZE TABLE` used to fail with
    `Invalid access: Can not convert empty value`, and `system.iceberg_history` silently
    dropped the rows. See issue #89037."""
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    table_name = (
        "test_iceberg_history_missing_optional_summary_metrics_"
        + str(format_version)
        + "_"
        + get_uuid_str()
    )

    create_iceberg_table(
        "local",
        instance,
        table_name,
        started_cluster_iceberg_no_spark,
        "(x Int)",
        format_version,
    )
    # Two `append` snapshots. `getHistory` parses every summary regardless of operation,
    # so `append` is required not to reach the parse but to avoid an unrelated rejection
    # after it: `tryGetAppendUpdate` skips `delete` and position-delete-only `overwrite`
    # and throws on `replace` and any other `overwrite`.
    instance.query(f"INSERT INTO {table_name} VALUES (1);")
    instance.query(f"INSERT INTO {table_name} VALUES (2);")

    meta, prev = _read_latest_metadata(instance, table_name)
    assert meta.get("snapshots"), "snapshot must be present after INSERT"

    removed = 0
    for snap in meta["snapshots"]:
        summary = snap.get("summary", {})
        assert (
            summary.get("operation") == "append"
        ), f"fixture requires append snapshots, got {summary.get('operation')!r}"
        # `checkIfIcebergHistorySupported` rejects an append with 0 added files, so only
        # the two fields from the bug report are removed.
        assert "added-data-files" in summary, "added-data-files must be kept"
        for field in ("added-files-size", "changed-partition-count"):
            if field in summary:
                del summary[field]
                removed += 1
    assert removed > 0, "no optional summary metric was removed: the fixture is vacuous"

    # A NEW metadata version, never an in-place rewrite: metadata JSON is cached
    # (`use_iceberg_metadata_files_cache` defaults to 1), so an in-place edit of the
    # already-read version is not re-read and this test would pass without the fix.
    _write_next_metadata(instance, table_name, meta, prev)

    # This is the statement from the bug report. `IcebergMetadata::optimize` calls
    # `getHistory` before `compactIcebergTable`, so the summary is read here for both
    # format versions and only the compaction that follows it is v2-only. On the cloud
    # build `OPTIMIZE` is gated by a member flag rather than this setting and that path
    # never calls `getHistory` at all.
    is_cloud = instance.query(
        "SELECT value FROM system.build_options WHERE name = 'CLICKHOUSE_CLOUD'"
    ).strip()
    try:
        instance.query(
            f"OPTIMIZE TABLE {table_name};",
            settings={"allow_experimental_iceberg_compaction": 1},
        )
    except Exception as exception:
        message = str(exception)
        assert (
            "Can not convert empty value" not in message
        ), f"OPTIMIZE hit the missing-optional-summary-metric conversion: {message}"
        # Anything else must be one of the two known post-`getHistory` rejections,
        # otherwise an unrelated failure would pass this check silently.
        tolerated = is_cloud == "1" or (
            format_version == 1
            and "Compaction is supported only for format_version 2" in message
        )
        assert tolerated, f"OPTIMIZE failed unexpectedly: {message}"

    # `StorageSystemIcebergHistory::fillData` swallows a `getHistory` exception, so the
    # regression drops the rows rather than reporting an error. The row count therefore
    # discriminates it on either build.
    #
    # The summary values additionally prove the STRIPPED metadata version is the one that
    # was read: `SnapshotSummary::forEachField` always emits these two metrics for an
    # `append` snapshot, so a stripped summary reports them as 0 while the previous,
    # unstripped version reports a non-zero byte count. Without this, reading the older
    # metadata file would satisfy every other assertion here.
    counters = (
        instance.query(
            f"SELECT count(), "
            f"countIf(summary['added-files-size'] = '0'), "
            f"countIf(summary['changed-partition-count'] = '0') "
            f"FROM system.iceberg_history "
            f"WHERE database = 'default' AND table = '{table_name}' FORMAT TSV"
        )
        .strip()
        .split("\t")
    )
    assert counters == ["2", "2", "2"], (
        f"system.iceberg_history must expose both snapshots of the stripped metadata "
        f"version with the absent metrics defaulted to 0, got {counters}"
    )

    assert (
        instance.query(f"SELECT x FROM {table_name} ORDER BY x FORMAT TSV") == "1\n2\n"
    )

    instance.query(f"DROP TABLE {table_name} SYNC;")
