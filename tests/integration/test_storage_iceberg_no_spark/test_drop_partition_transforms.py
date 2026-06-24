#!/usr/bin/env python3
"""ALTER TABLE ... DROP PARTITION <expr> against every Iceberg partition transform
ClickHouse can write (identity, icebergBucket, icebergTruncate, toYearNumSinceEpoch,
toMonthNumSinceEpoch, toRelativeDayNum, toRelativeHourNum, plus a multi-column case),
on both the S3 and local object-storage implementations.

For each scenario the expected survivors are derived from the table itself with
`WHERE NOT <partition predicate>` (evaluated before the drop), so bucket/truncate
hash assignments never have to be hard-coded.
"""

import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    drop_iceberg_table,
    get_uuid_str,
)

INSERT_SETTINGS = {"allow_insert_into_iceberg": 1}


def _run_drop_scenario(
    instance,
    cluster,
    storage_type,
    title,
    schema,
    partition_by,
    values,
    drop_expr,
    drop_predicate,
    select_cols,
):
    table = f"t_{title}_{storage_type}_{get_uuid_str()}"
    create_iceberg_table(
        storage_type,
        instance,
        table,
        cluster,
        schema=schema,
        partition_by=partition_by,
        format_version=2,
    )
    instance.query(f"INSERT INTO {table} VALUES {values}", settings=INSERT_SETTINGS)

    # Survivors expected after the drop = rows that do not belong to the dropped partition.
    expected = instance.query(
        f"SELECT {select_cols} FROM {table} WHERE NOT ({drop_predicate}) ORDER BY ALL"
    )
    dropped = int(
        instance.query(f"SELECT count() FROM {table} WHERE {drop_predicate}").strip()
    )
    assert dropped > 0, f"{title}: target partition is empty, scenario is meaningless"

    instance.query(
        f"ALTER TABLE {table} DROP PARTITION {drop_expr}", settings=INSERT_SETTINGS
    )

    actual = instance.query(f"SELECT {select_cols} FROM {table} ORDER BY ALL")
    assert actual == expected, f"{title} ({storage_type}): {actual!r} != {expected!r}"

    drop_iceberg_table(instance, table)


@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_drop_partition_transforms(started_cluster_iceberg_no_spark, storage_type):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    cluster = started_cluster_iceberg_no_spark

    def run(*args):
        _run_drop_scenario(instance, cluster, storage_type, *args)

    # identity -- scalar-literal and tuple-literal forms select the same partition.
    run("identity_scalar", "(a Int64, b String)", "(identity(a))",
        "(1, 'x'), (2, 'y'), (3, 'z')", "2", "a = 2", "a, b")
    run("identity_tuple", "(a Int64, b String)", "(identity(a))",
        "(1, 'x'), (2, 'y'), (3, 'z')", "tuple(2)", "a = 2", "a, b")

    # icebergBucket -- expression form (the user does not pre-compute the bucket).
    run("bucket_expr", "(id Int64, k String)", "(icebergBucket(4, k))",
        "(1, 'apple'), (2, 'banana'), (3, 'cherry'), (4, 'date')",
        "tuple(icebergBucket(4, 'banana'))",
        "icebergBucket(4, k) = icebergBucket(4, 'banana')", "id, k")

    # icebergTruncate on String -- literal and expression forms.
    run("truncate_literal", "(id Int64, k String)", "(icebergTruncate(3, k))",
        "(1, 'apple'), (2, 'apricot'), (3, 'banana'), (4, 'blueberry')",
        "'app'", "icebergTruncate(3, k) = 'app'", "id, k")
    run("truncate_expr", "(id Int64, k String)", "(icebergTruncate(3, k))",
        "(1, 'apple'), (2, 'apricot'), (3, 'banana'), (4, 'blueberry')",
        "tuple(icebergTruncate(3, 'apricot'))",
        "icebergTruncate(3, k) = icebergTruncate(3, 'apricot')", "id, k")

    # Temporal transforms.
    run("year", "(id Int64, d Date)", "(toYearNumSinceEpoch(d))",
        "(1, '2024-03-15'), (2, '2024-08-01'), (3, '2025-01-10'), (4, '2025-12-31')",
        "tuple(toYearNumSinceEpoch(toDate('2024-06-01')))",
        "toYearNumSinceEpoch(d) = toYearNumSinceEpoch(toDate('2024-06-01'))", "id, d")
    run("month", "(id Int64, d Date)", "(toMonthNumSinceEpoch(d))",
        "(1, '2025-01-05'), (2, '2025-01-29'), (3, '2025-02-10'), (4, '2025-03-15')",
        "tuple(toMonthNumSinceEpoch(toDate('2025-01-15')))",
        "toMonthNumSinceEpoch(d) = toMonthNumSinceEpoch(toDate('2025-01-15'))", "id, d")
    run("day", "(id Int64, d Date)", "(toRelativeDayNum(d))",
        "(1, '2025-05-01'), (2, '2025-05-01'), (3, '2025-05-02'), (4, '2025-05-03')",
        "tuple(toRelativeDayNum(toDate('2025-05-01')))",
        "toRelativeDayNum(d) = toRelativeDayNum(toDate('2025-05-01'))", "id, d")
    run("hour", "(id Int64, ts DateTime)", "(toRelativeHourNum(ts))",
        "(1, '2025-05-19 10:15:00'), (2, '2025-05-19 10:45:00'), (3, '2025-05-19 11:00:00'), (4, '2025-05-19 12:00:00')",
        "tuple(toRelativeHourNum(toDateTime('2025-05-19 10:30:00')))",
        "toRelativeHourNum(ts) = toRelativeHourNum(toDateTime('2025-05-19 10:30:00'))", "id, ts")


@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_drop_partition_multi_column_transform(started_cluster_iceberg_no_spark, storage_type):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    table = f"t_multi_{storage_type}_{get_uuid_str()}"
    create_iceberg_table(
        storage_type, instance, table, started_cluster_iceberg_no_spark,
        schema="(a Int64, b String, payload String)",
        partition_by="(identity(a), icebergBucket(4, b))",
        format_version=2,
    )
    instance.query(
        f"INSERT INTO {table} VALUES (1, 'apple', 'p1'), (1, 'banana', 'p2'), "
        f"(2, 'apple', 'p3'), (2, 'banana', 'p4'), (3, 'cherry', 'p5')",
        settings=INSERT_SETTINGS,
    )

    # Expression form: drop (a=1, bucket('apple')).
    pred1 = "a = 1 AND icebergBucket(4, b) = icebergBucket(4, 'apple')"
    expected1 = instance.query(f"SELECT a, b, payload FROM {table} WHERE NOT ({pred1}) ORDER BY ALL")
    instance.query(
        f"ALTER TABLE {table} DROP PARTITION tuple(1, icebergBucket(4, 'apple'))",
        settings=INSERT_SETTINGS,
    )
    assert instance.query(f"SELECT a, b, payload FROM {table} ORDER BY ALL") == expected1

    # Bare tuple-literal form with a pre-computed bucket index for the second partition.
    bucket_banana = instance.query("SELECT icebergBucket(4, 'banana')").strip()
    pred2 = "a = 2 AND icebergBucket(4, b) = icebergBucket(4, 'banana')"
    expected2 = instance.query(f"SELECT a, b, payload FROM {table} WHERE NOT ({pred2}) ORDER BY ALL")
    instance.query(
        f"ALTER TABLE {table} DROP PARTITION (2, {bucket_banana})",
        settings=INSERT_SETTINGS,
    )
    assert instance.query(f"SELECT a, b, payload FROM {table} ORDER BY ALL") == expected2

    drop_iceberg_table(instance, table)


@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_drop_partition_arity_mismatch(started_cluster_iceberg_no_spark, storage_type):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    table = f"t_arity_{storage_type}_{get_uuid_str()}"
    create_iceberg_table(
        storage_type, instance, table, started_cluster_iceberg_no_spark,
        schema="(a Int64, b String)", partition_by="(a, b)", format_version=2,
    )
    instance.query(f"INSERT INTO {table} VALUES (1, 'x')", settings=INSERT_SETTINGS)

    err = instance.query_and_get_error(
        f"ALTER TABLE {table} DROP PARTITION 1", settings=INSERT_SETTINGS
    )
    assert "INVALID_PARTITION_VALUE" in err

    drop_iceberg_table(instance, table)
