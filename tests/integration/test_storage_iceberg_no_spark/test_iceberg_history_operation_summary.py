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
