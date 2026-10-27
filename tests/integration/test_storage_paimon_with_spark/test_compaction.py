"""Compaction coverage: COMPACT snapshots rewrite data files and mark the old
ones as DELETE entries in the manifests. Two things must hold in ClickHouse:

- full scans honor DELETE manifest entries (otherwise rows from compacted-away
  files are double-counted);
- incremental reads skip COMPACT snapshots (they contain no new data).

Snapshot expiration is deliberately not tested here: the write-locally-then-
upload strategy never deletes remote files, so expiry would not be faithfully
reproduced over object storage.
"""

import pytest

from helpers.paimon_utils import (
    compact_paimon_table,
    create_incremental_paimon_table,
    create_spark_paimon_table,
    drop_tables_at_exit,
    get_creation_expression,
    get_uuid_str,
    id_array_str,
    spark_table,
    upload_paimon_table,
    wait_until_query_result,
)


@pytest.mark.parametrize("storage_type", ["s3", "azure"])
def test_full_scan_after_compaction(started_cluster_paimon_with_spark, storage_type):
    started_cluster = started_cluster_paimon_with_spark
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    table_name = "test_compaction_scan_" + storage_type + "_" + get_uuid_str()

    create_spark_paimon_table(
        spark,
        table_name,
        "id BIGINT, val STRING",
        # Make every small file eligible for full compaction.
        tblproperties={"compaction.min.file-num": "2"},
    )
    for i in range(3):
        spark.sql(
            f"INSERT INTO {spark_table(table_name)} "
            f"SELECT id + {i * 10}, 'x' FROM range(10)"
        )
    compact_paimon_table(spark, table_name)
    upload_paimon_table(started_cluster, storage_type, table_name)

    table_expr = get_creation_expression(
        storage_type, table_name, started_cluster, table_function=True
    )
    # DELETE manifest entries must be honored: 30 rows, not 60.
    assert instance.query(f"SELECT count(), uniqExact(id) FROM {table_expr}") == "30\t30\n"
    assert instance.query(f"SELECT sum(id) FROM {table_expr}") == f"{sum(range(30))}\n"


@pytest.mark.parametrize("storage_type", ["s3", "azure"])
def test_incremental_read_skips_compact_snapshot(
    started_cluster_paimon_with_spark, storage_type
):
    started_cluster = started_cluster_paimon_with_spark
    spark = started_cluster.spark_session
    table_name = "test_compaction_inc_" + storage_type + "_" + get_uuid_str()

    instance = create_incremental_paimon_table(
        started_cluster, storage_type, table_name, tblproperties={"compaction.min.file-num": "2"}
    )
    with drop_tables_at_exit(instance, table_name):
        count_query = f"SELECT count() FROM {table_name}"
        wait_until_query_result(instance, count_query, "1\n")
        wait_until_query_result(instance, count_query, "0\n")

        # Two APPEND snapshots with a COMPACT snapshot in between, all arriving in
        # one upload: the incremental read must return exactly the appended 20
        # rows, never the compacted rewrite of the same data.
        spark.sql(f"INSERT INTO {spark_table(table_name)} SELECT id + 1, 'x' FROM range(10)")
        compact_paimon_table(spark, table_name)
        spark.sql(f"INSERT INTO {spark_table(table_name)} SELECT id + 11, 'x' FROM range(10)")
        upload_paimon_table(started_cluster, storage_type, table_name)

        # Assert the delivered id set: exactly the appended ids 1..20, each
        # once — a count of 20 could also be a compacted rewrite replaying
        # some ids while dropping others.
        ids_query = f"SELECT arraySort(groupArray(id)) FROM {table_name}"
        wait_until_query_result(instance, ids_query, id_array_str(range(1, 21)))
        wait_until_query_result(instance, ids_query, "[]\n")
