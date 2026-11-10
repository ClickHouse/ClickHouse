import json

from helpers.iceberg_utils import (
    create_iceberg_table,
    execute_spark_query_general,
    get_uuid_str,
)

DELETE_COUNTERS = [
    "removed-position-deletes",
    "removed-position-delete-files",
    "removed-equality-deletes",
    "removed-equality-delete-files",
    "removed-delete-files",
]


def get_summaries(instance, table_name):
    """Snapshot summaries of the table, oldest first."""
    rows = instance.query(
        f"""
        SELECT toJSONString(summary)
        FROM system.iceberg_history
        WHERE table = '{table_name}'
        ORDER BY made_current_at
        """
    ).splitlines()
    return [json.loads(row) for row in rows]


def create_table_with_rewritten_position_delete(cluster, instance, storage_type, table_name):
    """
    A merge-on-read `UPDATE` in Spark adds a data file and a position delete file, so its snapshot is
    an `overwrite`. The next `UPDATE` of the same data file replaces the position delete file with a larger
    one, so that `overwrite` summary carries `removed-position-deletes` and `removed-position-delete-files`.
    """
    spark = cluster.spark_session

    def spark_query(query):
        execute_spark_query_general(spark, cluster, storage_type, table_name, query)

    spark_query(
        f"""
        CREATE TABLE {table_name} (tag INT, value STRING)
        USING iceberg
        PARTITIONED BY (identity(tag))
        TBLPROPERTIES (
            'format-version' = '2',
            'write.update.mode' = 'merge-on-read'
        )
        """
    )
    # Separate appends keep every partition in its own manifest, which is what `DROP PARTITION` supports.
    spark_query(f"INSERT INTO {table_name} VALUES (1, 'a'), (1, 'b'), (1, 'c')")
    spark_query(f"INSERT INTO {table_name} VALUES (2, 'keep')")
    spark_query(f"UPDATE {table_name} SET value = 'a2' WHERE tag = 1 AND value = 'a'")
    spark_query(f"UPDATE {table_name} SET value = 'b2' WHERE tag = 1 AND value = 'b'")

    create_iceberg_table(storage_type, instance, table_name, cluster)

    parent_summary = get_summaries(instance, table_name)[-1]
    assert parent_summary["operation"] == "overwrite", json.dumps(parent_summary)
    assert parent_summary["removed-position-deletes"] == "1", json.dumps(parent_summary)
    assert parent_summary["removed-position-delete-files"] == "1", json.dumps(parent_summary)
    assert instance.query(f"SELECT * FROM {table_name} ORDER BY tag, value") == (
        "1\ta2\n1\tb2\n1\tc\n2\tkeep\n"
    )
    return parent_summary


def test_drop_partition_removing_delete_files_after_spark_overwrite(
    started_cluster_iceberg_with_spark,
):
    cluster = started_cluster_iceberg_with_spark
    instance = cluster.instances["node1"]
    storage_type = "local"
    table_name = f"test_drop_partition_overwrite_dup_{get_uuid_str()}"

    create_table_with_rewritten_position_delete(cluster, instance, storage_type, table_name)

    # The partition still holds the rewritten position delete file, so this commit
    # sets the same counters as the parent `overwrite` snapshot.
    instance.query(
        f"ALTER TABLE {table_name} DROP PARTITION 1",
        settings={"allow_insert_into_iceberg": 1},
    )
    assert instance.query(f"SELECT * FROM {table_name} ORDER BY tag, value") == "2\tkeep\n"

    summary = get_summaries(instance, table_name)[-1]
    assert summary["operation"] == "delete", json.dumps(summary)
    # Only the files removed by this statement are counted: the one position delete file with two positions.
    assert summary["removed-position-delete-files"] == "1", json.dumps(summary)
    assert summary["removed-position-deletes"] == "2", json.dumps(summary)
    assert "removed-delete-files" not in summary, json.dumps(summary)
    assert summary["total-data-files"] == "1", json.dumps(summary)
    assert summary["total-delete-files"] == "0", json.dumps(summary)
    assert summary["total-position-deletes"] == "0", json.dumps(summary)


def test_drop_partition_not_removing_delete_files_after_spark_overwrite(
    started_cluster_iceberg_with_spark,
):
    cluster = started_cluster_iceberg_with_spark
    instance = cluster.instances["node1"]
    storage_type = "local"
    table_name = f"test_drop_partition_overwrite_stale_{get_uuid_str()}"

    create_table_with_rewritten_position_delete(cluster, instance, storage_type, table_name)

    # Partition 2 has no delete files, so nothing of the parent's delete counters applies to this commit.
    instance.query(
        f"ALTER TABLE {table_name} DROP PARTITION 2",
        settings={"allow_insert_into_iceberg": 1},
    )
    assert instance.query(f"SELECT * FROM {table_name} ORDER BY tag, value") == "1\ta2\n1\tb2\n1\tc\n"

    summary = get_summaries(instance, table_name)[-1]
    assert summary["operation"] == "delete", json.dumps(summary)
    for counter in DELETE_COUNTERS:
        assert counter not in summary, (counter, json.dumps(summary))
    assert summary["total-delete-files"] == "1", json.dumps(summary)
    assert summary["total-position-deletes"] == "2", json.dumps(summary)
