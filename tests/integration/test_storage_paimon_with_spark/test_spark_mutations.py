"""Customer journey: data corrections and GDPR-style deletes.

Spark UPDATE/DELETE on append-only Paimon tables are copy-on-write rewrites:
new data files are committed and the replaced ones become DELETE manifest
entries. ClickHouse must serve exactly the post-mutation data — returning
deleted rows (or both versions of updated rows) would be a compliance-grade
correctness failure.

(Primary-key tables — the other mutation path — are excluded until
https://github.com/ClickHouse/ClickHouse/issues/113360 is fixed.)
"""

import pytest

from helpers.paimon_utils import (
    create_spark_paimon_table,
    get_creation_expression,
    get_uuid_str,
    spark_table,
    upload_paimon_table,
)


@pytest.mark.parametrize("storage_type", ["s3", "azure"])
def test_spark_delete_and_update(started_cluster_paimon_with_spark, storage_type):
    started_cluster = started_cluster_paimon_with_spark
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    table_name = "test_mutations_" + storage_type + "_" + get_uuid_str()

    create_spark_paimon_table(spark, table_name, "id BIGINT, val STRING")
    spark.sql(f"INSERT INTO {spark_table(table_name)} SELECT id, 'orig' FROM range(5)")
    spark.sql(
        f"INSERT INTO {spark_table(table_name)} SELECT id + 5, 'orig' FROM range(5)"
    )

    spark.sql(f"UPDATE {spark_table(table_name)} SET val = 'updated' WHERE id = 3")
    spark.sql(f"DELETE FROM {spark_table(table_name)} WHERE id >= 8")
    upload_paimon_table(started_cluster, storage_type, table_name)

    table_expr = get_creation_expression(
        storage_type, table_name, started_cluster, table_function=True
    )
    # Exactly the post-mutation state: 8 rows, no deleted ids, no stale version
    # of the updated row.
    assert instance.query(f"SELECT count(), uniqExact(id) FROM {table_expr}") == "8\t8\n"
    assert instance.query(f"SELECT max(id) FROM {table_expr}") == "7\n"
    assert (
        instance.query(f"SELECT val FROM {table_expr} WHERE id = 3") == "updated\n"
    )
    assert (
        instance.query(f"SELECT count() FROM {table_expr} WHERE val = 'updated'")
        == "1\n"
    )


@pytest.mark.xfail(
    reason="https://github.com/ClickHouse/ClickHouse/issues/113360: primary keys "
    "are ignored by the read path (no merge-on-read), superseded row versions "
    "are returned",
    strict=True,
)
@pytest.mark.parametrize("storage_type", ["s3", "azure"])
def test_primary_key_table(started_cluster_paimon_with_spark, storage_type):
    """Customer journey: CDC/upserts into a primary-key table. Asserts the
    CORRECT (Paimon) semantics — latest row version per key — and is a strict
    xfail: when ClickHouse#113360 is fixed this starts failing-as-passed and
    must be promoted to a regular test."""
    started_cluster = started_cluster_paimon_with_spark
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    table_name = "test_pk_table_" + storage_type + "_" + get_uuid_str()

    create_spark_paimon_table(
        spark,
        table_name,
        "id INT, val STRING",
        tblproperties={"primary-key": "id", "bucket": "1"},
    )
    spark.sql(f"INSERT INTO {spark_table(table_name)} VALUES (1, 'old'), (2, 'two')")
    spark.sql(f"INSERT INTO {spark_table(table_name)} VALUES (1, 'new')")
    upload_paimon_table(started_cluster, storage_type, table_name)

    table_expr = get_creation_expression(
        storage_type, table_name, started_cluster, table_function=True
    )
    assert (
        instance.query(f"SELECT id, val FROM {table_expr} ORDER BY id")
        == "1\tnew\n2\ttwo\n"
    )
