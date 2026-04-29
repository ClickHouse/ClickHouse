"""
Integration test for parallel Iceberg manifest file loading
(iceberg_metadata_files_parallel_loading_threads setting).

Creates a table with 30 manifest files and verifies that parallel loading
(threads > 1) produces identical results to serial loading (threads = 1).

Timing improvement of parallel over serial requires real S3 latency to be
observable and is therefore validated via unit/profiling tests, not here.
"""

import pytest
import time

from helpers.iceberg_utils import create_iceberg_table, get_uuid_str


NUM_MANIFESTS = 30


@pytest.mark.parametrize("format_version", [2])
@pytest.mark.parametrize("storage_type", ["local"])
def test_parallel_manifest_loading_correctness(
    started_cluster_iceberg_no_spark, format_version, storage_type
):
    """Parallel and serial manifest loading must produce identical results."""
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    TABLE_NAME = "test_parallel_manifest_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_no_spark,
        "(id UInt32, val String)",
        format_version,
    )

    # Insert NUM_MANIFESTS separate batches — each INSERT produces one manifest file.
    for i in range(NUM_MANIFESTS):
        instance.query(
            f"INSERT INTO {TABLE_NAME} VALUES ({i}, 'row_{i}')",
            settings={"allow_insert_into_iceberg": 1},
        )

    # ── Serial baseline ──────────────────────────────────────────────────────
    serial_settings = {
        "iceberg_metadata_files_parallel_loading_threads": 1,
        "use_iceberg_metadata_files_cache": 0,  # force cold read each time
    }

    serial_count = int(
        instance.query(f"SELECT count() FROM {TABLE_NAME}", settings=serial_settings)
    )
    serial_sum = int(
        instance.query(f"SELECT sum(id) FROM {TABLE_NAME}", settings=serial_settings)
    )
    serial_rows = instance.query(
        f"SELECT id, val FROM {TABLE_NAME} ORDER BY id",
        settings=serial_settings,
    )

    # ── Parallel run ─────────────────────────────────────────────────────────
    parallel_settings = {
        "iceberg_metadata_files_parallel_loading_threads": 16,
        "use_iceberg_metadata_files_cache": 0,
    }

    parallel_count = int(
        instance.query(f"SELECT count() FROM {TABLE_NAME}", settings=parallel_settings)
    )
    parallel_sum = int(
        instance.query(f"SELECT sum(id) FROM {TABLE_NAME}", settings=parallel_settings)
    )
    parallel_rows = instance.query(
        f"SELECT id, val FROM {TABLE_NAME} ORDER BY id",
        settings=parallel_settings,
    )

    # ── Assertions ───────────────────────────────────────────────────────────
    assert serial_count == NUM_MANIFESTS, (
        f"Expected {NUM_MANIFESTS} rows (one per manifest), got {serial_count}"
    )
    assert parallel_count == serial_count, (
        f"count mismatch: serial={serial_count}, parallel={parallel_count}"
    )

    expected_sum = NUM_MANIFESTS * (NUM_MANIFESTS - 1) // 2
    assert serial_sum == expected_sum, (
        f"sum mismatch: expected {expected_sum}, serial={serial_sum}"
    )
    assert parallel_sum == serial_sum, (
        f"sum mismatch: serial={serial_sum}, parallel={parallel_sum}"
    )

    assert parallel_rows == serial_rows, (
        f"Row content mismatch between serial and parallel loading.\n"
        f"Serial:\n{serial_rows}\nParallel:\n{parallel_rows}"
    )


@pytest.mark.parametrize("format_version", [2])
@pytest.mark.parametrize("storage_type", ["local"])
def test_parallel_manifest_loading_profile_event(
    started_cluster_iceberg_no_spark, format_version, storage_type
):
    """
    With parallel_loading_threads > 1, the IcebergManifestFilesParallelFetchMicroseconds
    ProfileEvent must be incremented (confirming the parallel code path was taken).
    """
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    TABLE_NAME = "test_parallel_event_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_no_spark,
        "(id UInt32)",
        format_version,
    )

    for i in range(10):
        instance.query(
            f"INSERT INTO {TABLE_NAME} VALUES ({i})",
            settings={"allow_insert_into_iceberg": 1},
        )

    result = instance.query(
        f"""
        SELECT ProfileEvents['IcebergManifestFilesParallelFetchMicroseconds'] > 0 AS parallel_triggered
        FROM system.query_log
        WHERE
            query LIKE '%SELECT count() FROM {TABLE_NAME}%'
            AND type = 'QueryFinish'
        ORDER BY event_time DESC
        LIMIT 1
        """,
        # Run the query we're going to observe first
        settings={
            "iceberg_metadata_files_parallel_loading_threads": 8,
            "use_iceberg_metadata_files_cache": 0,
            "log_queries": 1,
        },
    )

    # Run the actual query first, then check query_log
    instance.query(
        f"SELECT count() FROM {TABLE_NAME}",
        settings={
            "iceberg_metadata_files_parallel_loading_threads": 8,
            "use_iceberg_metadata_files_cache": 0,
            "log_queries": 1,
        },
    )

    # Give query_log a moment to flush
    time.sleep(1)
    instance.query("SYSTEM FLUSH LOGS")

    parallel_triggered = instance.query(
        f"""
        SELECT ProfileEvents['IcebergManifestFilesParallelFetchMicroseconds'] > 0
        FROM system.query_log
        WHERE
            query LIKE '%SELECT count() FROM {TABLE_NAME}%'
            AND type = 'QueryFinish'
        ORDER BY event_time DESC
        LIMIT 1
        """
    ).strip()

    assert parallel_triggered == "1", (
        "IcebergManifestFilesParallelFetchMicroseconds was not incremented — "
        "the parallel code path was not taken."
    )
