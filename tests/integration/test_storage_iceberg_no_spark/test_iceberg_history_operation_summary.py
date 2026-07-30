import json

import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    get_uuid_str,
)

EXPECTED_APPEND_SUMMARY_KEYS = {
    "added-data-files",
    "added-files-size",
    "added-records",
    "changed-partition-count",
    "total-data-files",
    "total-delete-files",
    "total-equality-deletes",
    "total-files-size",
    "total-position-deletes",
    "total-records",
}


def _read_history(instance, table_name):
    rows = (
        instance.query(
            f"SELECT operation, toJSONString(summary) "
            f"FROM system.iceberg_history "
            f"WHERE database = 'default' AND table = '{table_name}' "
            f"ORDER BY made_current_at FORMAT TSV"
        )
        .strip()
        .split("\n")
    )
    return [(op, json.loads(summary)) for op, summary in (row.split("\t", 1) for row in rows)]


def _read_current_summary(instance, table_name):
    operation, summary = (
        instance.query(
            f"SELECT operation, toJSONString(summary) "
            f"FROM system.iceberg_history "
            f"WHERE database = 'default' AND table = '{table_name}' "
            f"ORDER BY made_current_at DESC, snapshot_id DESC LIMIT 1 FORMAT TSV"
        )
        .strip()
        .split("\t", 1)
    )
    return operation, json.loads(summary)


def _assert_totals(summary, *, records, data_files, delete_files, position_deletes):
    assert summary["total-records"] == str(records)
    assert summary["total-data-files"] == str(data_files)
    assert summary["total-delete-files"] == str(delete_files)
    assert summary["total-position-deletes"] == str(position_deletes)
    assert summary["total-equality-deletes"] == "0"


@pytest.mark.parametrize("format_version", [1, 2])
def test_iceberg_history_append_operation_and_summary(
    started_cluster_iceberg_no_spark, format_version
):
    """`system.iceberg_history` must expose the `operation` and the full `summary` of
    each snapshot. ClickHouse both writes and reads the summary here, so this covers
    the write -> metadata -> read round-trip of our own summaries."""
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    table_name = (
        "test_iceberg_history_append_summary_"
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

    instance.query(f"INSERT INTO {table_name} VALUES (1), (2), (3);")
    instance.query(f"INSERT INTO {table_name} VALUES (4), (5);")

    history = _read_history(instance, table_name)
    assert len(history) == 2, f"expected two snapshots, got: {history}"

    (op1, s1), (op2, s2) = history

    # First snapshot: APPEND of 3 records into a single data file.
    assert op1 == "APPEND", f"unexpected operation: {op1}"
    assert EXPECTED_APPEND_SUMMARY_KEYS.issubset(set(s1)), f"missing summary keys: {EXPECTED_APPEND_SUMMARY_KEYS.difference(set(s1))}"
    assert s1["added-records"] == "3"
    assert s1["added-data-files"] == "1"
    assert s1["changed-partition-count"] == "1"
    assert int(s1["added-files-size"]) > 0, s1["added-files-size"]
    # Fresh table: running totals equal the deltas, no deletes yet.
    assert s1["total-records"] == "3"
    assert s1["total-data-files"] == "1"
    assert s1["total-files-size"] == s1["added-files-size"]
    assert s1["total-delete-files"] == "0"
    assert s1["total-position-deletes"] == "0"
    assert s1["total-equality-deletes"] == "0"

    # Second snapshot: APPEND of 2 more records; totals accumulate onto the parent.
    assert op2 == "APPEND", f"unexpected operation: {op2}"
    assert EXPECTED_APPEND_SUMMARY_KEYS.issubset(set(s2)), f"missing summary keys: {EXPECTED_APPEND_SUMMARY_KEYS.difference(set(s2))}"
    assert s2["added-records"] == "2"
    assert s2["added-data-files"] == "1"
    assert s2["changed-partition-count"] == "1"
    assert int(s2["added-files-size"]) > 0, s2["added-files-size"]
    assert s2["total-records"] == "5"
    assert s2["total-data-files"] == "2"
    assert int(s2["total-files-size"]) == int(s1["total-files-size"]) + int(s2["added-files-size"])
    assert s2["total-delete-files"] == "0"
    assert s2["total-position-deletes"] == "0"
    assert s2["total-equality-deletes"] == "0"


@pytest.mark.parametrize("storage_type", ["local", "s3"])
def test_iceberg_mutation_summary_totals(
    started_cluster_iceberg_no_spark, storage_type
):
    """Iceberg mutations must accumulate the physical data and position-delete totals.

    An `UPDATE` adds a replacement record and a position delete, while a `DELETE` only
    adds another position delete. The visible row count is therefore the physical record
    total minus the position-delete total.
    """
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    table_name = f"test_iceberg_mutation_summary_{storage_type}_{get_uuid_str()}"

    create_iceberg_table(
        storage_type,
        instance,
        table_name,
        started_cluster_iceberg_no_spark,
        "(id Int, value String)",
        2,
    )

    instance.query(f"INSERT INTO {table_name} VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    instance.query(
        f"ALTER TABLE {table_name} UPDATE value = 'updated' WHERE id = 2",
        settings={"mutations_sync": 1},
    )

    operation, summary = _read_current_summary(instance, table_name)
    assert operation == "OVERWRITE"
    assert summary["added-records"] == "1"
    assert summary["added-position-deletes"] == "1"
    _assert_totals(
        summary,
        records=4,
        data_files=2,
        delete_files=1,
        position_deletes=1,
    )
    assert instance.query(f"SELECT id, value FROM {table_name} ORDER BY id") == (
        "1\ta\n2\tupdated\n3\tc\n"
    )

    instance.query(
        f"ALTER TABLE {table_name} DELETE WHERE id = 1",
        settings={"mutations_sync": 1},
    )

    operation, summary = _read_current_summary(instance, table_name)
    assert operation == "OVERWRITE"
    assert summary["added-records"] == "0"
    assert summary["added-position-deletes"] == "1"
    _assert_totals(
        summary,
        records=4,
        data_files=2,
        delete_files=2,
        position_deletes=2,
    )
    assert instance.query(f"SELECT id, value FROM {table_name} ORDER BY id") == (
        "2\tupdated\n3\tc\n"
    )


@pytest.mark.parametrize("storage_type", ["local", "s3"])
def test_iceberg_optimize_summary_totals(
    started_cluster_iceberg_no_spark, storage_type
):
    """`OPTIMIZE TABLE` must not count carried-forward manifests more than once."""
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    table_name = f"test_iceberg_optimize_summary_{storage_type}_{get_uuid_str()}"

    create_iceberg_table(
        storage_type,
        instance,
        table_name,
        started_cluster_iceberg_no_spark,
        "(id Int, value String)",
        2,
    )

    instance.query(f"INSERT INTO {table_name} VALUES (1, 'a'), (2, 'b')")
    instance.query(f"INSERT INTO {table_name} VALUES (3, 'c'), (4, 'd')")
    instance.query(f"INSERT INTO {table_name} VALUES (5, 'e'), (6, 'f')")
    instance.query(
        f"ALTER TABLE {table_name} DELETE WHERE id = 2",
        settings={"mutations_sync": 1},
    )

    assert instance.query(f"SELECT count() FROM {table_name}").strip() == "5"

    instance.query(
        f"OPTIMIZE TABLE {table_name}",
        settings={"allow_experimental_iceberg_compaction": 1},
    )

    operation, summary = _read_current_summary(instance, table_name)
    assert operation == "APPEND"
    _assert_totals(
        summary,
        records=5,
        data_files=3,
        delete_files=0,
        position_deletes=0,
    )
    assert instance.query(f"SELECT id, value FROM {table_name} ORDER BY id") == (
        "1\ta\n3\tc\n4\td\n5\te\n6\tf\n"
    )
    assert instance.query(f"SELECT count() FROM {table_name}").strip() == "5"


@pytest.mark.parametrize("storage_type", ["local", "s3"])
def test_iceberg_optimize_manifest_preserves_mutation_totals(
    started_cluster_iceberg_no_spark, storage_type
):
    """`OPTIMIZE TABLE ... MANIFEST` must inherit every total from its parent."""
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    table_name = (
        f"test_iceberg_optimize_manifest_summary_{storage_type}_{get_uuid_str()}"
    )

    create_iceberg_table(
        storage_type,
        instance,
        table_name,
        started_cluster_iceberg_no_spark,
        "(id Int, value String)",
        2,
    )

    instance.query(f"INSERT INTO {table_name} VALUES (1, 'a'), (2, 'b')")
    instance.query(f"INSERT INTO {table_name} VALUES (3, 'c'), (4, 'd')")
    instance.query(f"INSERT INTO {table_name} VALUES (5, 'e'), (6, 'f')")
    instance.query(
        f"ALTER TABLE {table_name} DELETE WHERE id = 2",
        settings={"mutations_sync": 1},
    )

    _, summary_before = _read_current_summary(instance, table_name)
    _assert_totals(
        summary_before,
        records=6,
        data_files=3,
        delete_files=1,
        position_deletes=1,
    )

    instance.query(
        f"OPTIMIZE TABLE {table_name} MANIFEST",
        settings={
            "allow_experimental_iceberg_compaction": 1,
            "iceberg_manifest_min_count_to_compact": 2,
        },
    )

    operation, summary_after = _read_current_summary(instance, table_name)
    assert operation == "REPLACE"
    _assert_totals(
        summary_after,
        records=6,
        data_files=3,
        delete_files=1,
        position_deletes=1,
    )
    assert instance.query(f"SELECT count() FROM {table_name}").strip() == "5"
