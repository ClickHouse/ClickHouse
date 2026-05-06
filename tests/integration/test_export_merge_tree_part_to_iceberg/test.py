"""
Integration tests for EXPORT PART to an IcebergS3 destination.

These tests cover the data-movement path from a plain MergeTree table to an
IcebergS3 table using the single-part export operation:

    ALTER TABLE <mt> EXPORT PART '<part>' TO TABLE <iceberg>

Coverage:
    test_export_part_basic_to_iceberg                     – simple (id, year) schema; data + part_log checks
    test_export_part_all_iceberg_types                    – schema covering all major Iceberg data types
    test_export_multiple_parts_to_iceberg                 – two parts from different partitions land together
    test_export_part_with_year_transform_partition        – toYearNumSinceEpoch() partition expression
    test_export_part_with_bucket_partition                – icebergBucket(N, col) partition expression
    test_export_part_partition_key_mismatch_is_rejected   – mismatched partition spec rejected synchronously
"""

import logging
import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.export_partition_helpers import (
    first_partition_id,
    make_iceberg_s3,
    make_mt,
    unique_suffix,
)
from helpers.iceberg_export_stats import (
    assert_exported_stats,
    fetch_manifest_entries,
)


# ---------------------------------------------------------------------------
# Cluster fixture
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node1",
            main_configs=["configs/config.d/metadata_log.xml"],
            with_minio=True,
        )
        logging.info("Starting cluster...")
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def get_part(node, table: str, partition_id: str) -> str:
    """Return the name of the first active part of *table* in *partition_id*."""
    return node.query(
        f"SELECT name FROM system.parts "
        f"WHERE database = currentDatabase() AND table = '{table}' "
        f"AND partition_id = '{partition_id}' AND active "
        f"ORDER BY name LIMIT 1"
    ).strip()


def export_part(node, table: str, part: str, dest: str) -> None:
    node.query(
        f"ALTER TABLE {table} EXPORT PART '{part}' TO TABLE {dest} "
        f"SETTINGS allow_experimental_export_merge_tree_part = 1, "
        f"allow_experimental_insert_into_iceberg = 1"
    )


def wait_for_export_part(
    node,
    table: str,
    part: str,
    timeout: int = 60,
    poll_interval: float = 0.5,
) -> None:
    """Poll system.part_log until an ExportPart event appears for *part*."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        node.query("SYSTEM FLUSH LOGS")
        count = node.query(
            f"SELECT count() FROM system.part_log "
            f"WHERE event_type = 'ExportPart' "
            f"AND database = currentDatabase() "
            f"AND table = '{table}' "
            f"AND part_name = '{part}'"
        ).strip()
        if count != "0":
            return
        time.sleep(poll_interval)
    raise TimeoutError(
        f"ExportPart event for part {part!r} in table {table!r} "
        f"did not appear in system.part_log within {timeout}s"
    )


def assert_part_log(node, table: str, part: str) -> None:
    """Assert that system.part_log contains at least one ExportPart entry."""
    log_count = int(
        node.query(
            f"SELECT count() FROM system.part_log "
            f"WHERE event_type = 'ExportPart' "
            f"AND database = currentDatabase() "
            f"AND table = '{table}' "
            f"AND part_name = '{part}'"
        ).strip()
    )
    assert log_count >= 1, (
        f"Expected at least one ExportPart entry in system.part_log "
        f"for part {part!r} in table {table!r}, found {log_count}"
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_export_part_basic_to_iceberg(cluster):
    """
    Basic happy path: export a single MergeTree part to an IcebergS3 table and
    verify the row count, the content, and the system.part_log ExportPart entry.
    """
    node = cluster.instances["node1"]
    sfx = unique_suffix()
    mt = f"mt_basic_{sfx}"
    iceberg = f"iceberg_basic_{sfx}"

    make_mt(node, mt, "id Int32, year Int32", "year")
    make_iceberg_s3(node, iceberg, "id Int32, year Int32", "year")

    node.query(f"INSERT INTO {mt} VALUES (1, 2020), (2, 2020), (3, 2020), (4, 2021)")

    part_2020 = get_part(node, mt, "2020")
    export_part(node, mt, part_2020, iceberg)
    wait_for_export_part(node, mt, part_2020)

    count = int(node.query(f"SELECT count() FROM {iceberg}").strip())
    assert count == 3, f"Expected 3 rows in Iceberg table after export, got {count}"

    result = node.query(f"SELECT id, year FROM {iceberg} ORDER BY id").strip()
    assert result == "1\t2020\n2\t2020\n3\t2020", f"Unexpected exported data:\n{result}"

    assert_part_log(node, mt, part_2020)

    node.query(f"DROP TABLE IF EXISTS {mt} SYNC")
    node.query(f"DROP TABLE IF EXISTS {iceberg}")


def test_export_part_all_iceberg_types(cluster):
    """
    Export a part whose schema covers every ClickHouse type that getIcebergType()
    in Utils.cpp maps to an Iceberg primitive (see the switch statement):

        Iceberg type  ClickHouse column
        ------------- --------------------------
        int           id        Int32
        long          big_val   Int64
        float         f32       Float32
        double        f64       Float64
        date          event_dt  Date
        timestamp     ts        DateTime64(6)   (DateTime / DateTime64 both → "timestamp")
        string        name      String
        uuid          uid_val   UUID

    Types not in the switch (Bool/UInt8, FixedString, Decimal) are intentionally
    excluded — they throw BAD_ARGUMENTS from getIcebergType().

    Verifies that every column round-trips correctly through the Iceberg layer
    and that system.part_log records the ExportPart event.
    """
    node = cluster.instances["node1"]
    sfx = unique_suffix()
    mt = f"mt_types_{sfx}"
    iceberg = f"iceberg_types_{sfx}"

    columns = (
        "id Int32, "
        "big_val Int64, "
        "f32 Float32, "
        "f64 Float64, "
        "event_dt Date, "
        "ts DateTime64(6), "
        "name String, "
        "uid_val UUID, "
        "year Int32"
    )

    make_mt(node, mt, columns, "year", order_by="id")
    make_iceberg_s3(node, iceberg, columns, "year")

    node.query(
        f"""
        INSERT INTO {mt} (id, big_val, f32, f64, event_dt, ts, name, uid_val, year)
        VALUES (
            1,
            9999999999999,
            3.14,
            2.718281828459045,
            '2024-01-15',
            '2024-01-15 12:30:45.123456',
            'hello iceberg',
            '550e8400-e29b-41d4-a716-446655440000',
            2024
        )
        """
    )

    part = get_part(node, mt, "2024")
    export_part(node, mt, part, iceberg)
    wait_for_export_part(node, mt, part)

    count = int(node.query(f"SELECT count() FROM {iceberg}").strip())
    assert count == 1, f"Expected 1 row in Iceberg table, got {count}"

    row = node.query(
        f"SELECT id, big_val, name, year FROM {iceberg}"
    ).strip()
    assert "1" in row,              f"id column missing/wrong: {row}"
    assert "9999999999999" in row,  f"big_val column missing/wrong: {row}"
    assert "hello iceberg" in row,  f"name column missing/wrong: {row}"
    assert "2024" in row,           f"year column missing/wrong: {row}"

    # Verify date round-trip
    date_result = node.query(f"SELECT toString(event_dt) FROM {iceberg}").strip()
    assert date_result == "2024-01-15", f"Date round-trip failed: {date_result!r}"

    # Verify timestamp round-trip (date component is sufficient; exact time format varies)
    ts_result = node.query(f"SELECT ts FROM {iceberg}").strip()
    assert "2024-01-15" in ts_result, f"Timestamp date component missing: {ts_result!r}"

    # Verify UUID round-trip
    uid_result = node.query(f"SELECT toString(uid_val) FROM {iceberg}").strip()
    assert uid_result == "550e8400-e29b-41d4-a716-446655440000", (
        f"UUID round-trip failed: {uid_result!r}"
    )

    assert_part_log(node, mt, part)

    node.query(f"DROP TABLE IF EXISTS {mt} SYNC")
    node.query(f"DROP TABLE IF EXISTS {iceberg}")


def test_export_multiple_parts_to_iceberg(cluster):
    """
    Export parts from two different partitions to the same Iceberg table and
    verify that both land correctly without overwriting each other.
    system.part_log must contain one ExportPart entry per exported part.
    """
    node = cluster.instances["node1"]
    sfx = unique_suffix()
    mt = f"mt_multi_{sfx}"
    iceberg = f"iceberg_multi_{sfx}"

    make_mt(node, mt, "id Int32, year Int32", "year")
    make_iceberg_s3(node, iceberg, "id Int32, year Int32", "year")

    # Each INSERT creates a separate part per partition
    node.query(f"INSERT INTO {mt} VALUES (1, 2020), (2, 2020)")
    node.query(f"INSERT INTO {mt} VALUES (10, 2021), (11, 2021), (12, 2021)")

    part_2020 = get_part(node, mt, "2020")
    part_2021 = get_part(node, mt, "2021")

    export_part(node, mt, part_2020, iceberg)
    export_part(node, mt, part_2021, iceberg)

    wait_for_export_part(node, mt, part_2020)
    wait_for_export_part(node, mt, part_2021)

    total = int(node.query(f"SELECT count() FROM {iceberg}").strip())
    assert total == 5, f"Expected 5 rows total (2+3), got {total}"

    count_2020 = int(node.query(f"SELECT count() FROM {iceberg} WHERE year = 2020").strip())
    count_2021 = int(node.query(f"SELECT count() FROM {iceberg} WHERE year = 2021").strip())
    assert count_2020 == 2, f"Expected 2 rows for year=2020, got {count_2020}"
    assert count_2021 == 3, f"Expected 3 rows for year=2021, got {count_2021}"

    result_2020 = node.query(
        f"SELECT id FROM {iceberg} WHERE year = 2020 ORDER BY id"
    ).strip()
    assert result_2020 == "1\n2", f"Unexpected 2020 rows: {result_2020}"

    result_2021 = node.query(
        f"SELECT id FROM {iceberg} WHERE year = 2021 ORDER BY id"
    ).strip()
    assert result_2021 == "10\n11\n12", f"Unexpected 2021 rows: {result_2021}"

    assert_part_log(node, mt, part_2020)
    assert_part_log(node, mt, part_2021)

    node.query(f"DROP TABLE IF EXISTS {mt} SYNC")
    node.query(f"DROP TABLE IF EXISTS {iceberg}")


def test_export_part_with_year_transform_partition(cluster):
    """
    Export a part from a MergeTree table partitioned by toYearNumSinceEpoch(event_date)
    to an Iceberg table with the matching year-transform spec.

    Verifies that the Iceberg year-transform partition expression is accepted
    and that all rows survive the round-trip intact.
    """
    node = cluster.instances["node1"]
    sfx = unique_suffix()
    mt = f"mt_year_tf_{sfx}"
    iceberg = f"iceberg_year_tf_{sfx}"

    cols = "id Int64, event_date Date"
    partition_by = "toYearNumSinceEpoch(event_date)"

    make_mt(node, mt, cols, partition_by, order_by="id")
    make_iceberg_s3(node, iceberg, cols, partition_by)

    node.query(
        f"INSERT INTO {mt} VALUES "
        f"(1, '2023-03-15'), (2, '2023-11-01'), (3, '2023-06-30')"
    )

    pid = first_partition_id(node, mt)
    part = get_part(node, mt, pid)

    export_part(node, mt, part, iceberg)
    wait_for_export_part(node, mt, part)

    count = int(node.query(f"SELECT count() FROM {iceberg}").strip())
    assert count == 3, f"Expected 3 rows, got {count}"

    result = node.query(
        f"SELECT id, toString(event_date) FROM {iceberg} ORDER BY id"
    ).strip()
    assert "1\t2023-03-15" in result, f"Row 1 missing or incorrect:\n{result}"
    assert "2\t2023-11-01" in result, f"Row 2 missing or incorrect:\n{result}"
    assert "3\t2023-06-30" in result, f"Row 3 missing or incorrect:\n{result}"

    assert_part_log(node, mt, part)

    node.query(f"DROP TABLE IF EXISTS {mt} SYNC")
    node.query(f"DROP TABLE IF EXISTS {iceberg}")


def test_export_part_partition_key_mismatch_is_rejected(cluster):
    """
    EXPORT PART must synchronously reject (BAD_ARGUMENTS) when the source
    MergeTree partition key does not match the destination Iceberg partition
    spec. Export does not repartition data, so the two specs must agree on
    every field (same source column by Iceberg field-id and same transform,
    in the same order).

    Failing case: MergeTree PARTITION BY year, Iceberg PARTITION BY id.
    The part must NOT land in the Iceberg table.
    """
    node = cluster.instances["node1"]
    sfx = unique_suffix()
    mt = f"mt_pkey_mismatch_{sfx}"
    iceberg = f"iceberg_pkey_mismatch_{sfx}"

    make_mt(node, mt, "id Int32, year Int32", "year")
    make_iceberg_s3(node, iceberg, "id Int32, year Int32", "id")

    node.query(f"INSERT INTO {mt} VALUES (1, 2020), (2, 2020), (3, 2020)")

    part_2020 = get_part(node, mt, "2020")

    error = node.query_and_get_error(
        f"ALTER TABLE {mt} EXPORT PART '{part_2020}' TO TABLE {iceberg} "
        f"SETTINGS allow_experimental_export_merge_tree_part = 1, "
        f"allow_experimental_insert_into_iceberg = 1"
    )
    assert "BAD_ARGUMENTS" in error, (
        f"Expected BAD_ARGUMENTS for partition key mismatch, got: {error!r}"
    )

    count = int(node.query(f"SELECT count() FROM {iceberg}").strip())
    assert count == 0, (
        f"Expected 0 rows in Iceberg table after rejected export, got {count}"
    )

    node.query(f"DROP TABLE IF EXISTS {mt} SYNC")
    node.query(f"DROP TABLE IF EXISTS {iceberg}")


def test_export_part_with_bucket_partition(cluster):
    """
    Export a part from a MergeTree table partitioned by icebergBucket(8, user_id)
    to a matching Iceberg table.

    Verifies that the bucket partition expression is accepted for EXPORT PART and
    that data lands correctly in the Iceberg bucket partition.
    """
    node = cluster.instances["node1"]
    sfx = unique_suffix()
    mt = f"mt_bucket_{sfx}"
    iceberg = f"iceberg_bucket_{sfx}"

    cols = "id Int64, user_id Int64, value String"
    partition_by = "icebergBucket(8, user_id)"

    make_mt(node, mt, cols, partition_by)
    make_iceberg_s3(node, iceberg, cols, partition_by)

    # Both rows go to the same bucket (user_id=42 → bucket 2 for N=8)
    node.query(f"INSERT INTO {mt} VALUES (1, 42, 'hello'), (2, 42, 'world')")

    pid = first_partition_id(node, mt)
    part = get_part(node, mt, pid)

    export_part(node, mt, part, iceberg)
    wait_for_export_part(node, mt, part)

    count = int(node.query(f"SELECT count() FROM {iceberg}").strip())
    assert count == 2, f"Expected 2 rows in Iceberg table, got {count}"

    result = node.query(
        f"SELECT id, user_id, value FROM {iceberg} ORDER BY id"
    ).strip()
    assert "1\t42\thello" in result, f"Row 1 missing or incorrect:\n{result}"
    assert "2\t42\tworld" in result, f"Row 2 missing or incorrect:\n{result}"

    assert_part_log(node, mt, part)

    node.query(f"DROP TABLE IF EXISTS {mt} SYNC")
    node.query(f"DROP TABLE IF EXISTS {iceberg}")


def test_export_part_writes_column_statistics(cluster):
    """
    Export a MergeTree part that contains one NULL and verify that the resulting
    Iceberg manifest entry carries accurate per-file column statistics:
    record_count, file_size_in_bytes, column_sizes, null_value_counts,
    and lower/upper bounds (Int32 + String + Nullable(String) mix).
    """
    node = cluster.instances["node1"]
    sfx = unique_suffix()
    mt = f"mt_stats_{sfx}"
    iceberg = f"iceberg_stats_{sfx}"

    columns = "id Int32, name String, tag Nullable(String), year Int32"

    make_mt(node, mt, columns, "year", order_by="id")
    make_iceberg_s3(node, iceberg, columns, "year")

    node.query(
        f"""
        INSERT INTO {mt} (id, name, tag, year) VALUES
            (1, 'aaa', 'x',  2020),
            (2, 'mmm', NULL, 2020),
            (3, 'zzz', 'y',  2020),
            (4, 'kkk', 'z',  2021)
        """
    )

    part_2020 = get_part(node, mt, "2020")
    export_part(node, mt, part_2020, iceberg)
    wait_for_export_part(node, mt, part_2020)

    count = int(node.query(f"SELECT count() FROM {iceberg}").strip())
    assert count == 3, f"Expected 3 rows after export, got {count}"

    query_id = f"stats_part_{sfx}"
    node.query(
        f"SELECT * FROM {iceberg} ORDER BY id",
        query_id=query_id,
        settings={"iceberg_metadata_log_level": "manifest_file_entry"},
    )

    entries = fetch_manifest_entries(node, query_id)
    assert_exported_stats(entries)

    node.query(f"DROP TABLE IF EXISTS {mt} SYNC")
    node.query(f"DROP TABLE IF EXISTS {iceberg}")
