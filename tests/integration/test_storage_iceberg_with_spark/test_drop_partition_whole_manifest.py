import os
import re

import pytest
from py4j.protocol import Py4JJavaError

from helpers.iceberg_utils import (
    create_iceberg_table,
    default_download_directory,
    execute_spark_query_general,
    get_uuid_str,
)


def update_spark_version_hint(cluster, storage_type, table_name):
    local_dir = f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}/"
    default_download_directory(cluster, storage_type, local_dir, local_dir)

    metadata_dir = os.path.join(local_dir, "metadata")
    versions = []
    for name in os.listdir(metadata_dir):
        match = re.match(r"v(\d+)(?:[-.].*)?\.metadata\.json$", name)
        if match:
            versions.append(int(match.group(1)))

    assert versions, "ClickHouse should have written a new metadata file"
    with open(os.path.join(metadata_dir, "version-hint.text"), "w") as hint:
        hint.write(str(max(versions)))

    return local_dir


@pytest.mark.parametrize("shared_manifest", [False, True], ids=["whole", "partial"])
def test_drop_partition_conflicts_with_prepared_overwrite(
    started_cluster_iceberg_with_spark, shared_manifest
):
    cluster = started_cluster_iceberg_with_spark
    instance = cluster.instances["node1"]
    spark = cluster.spark_session
    table_name = f"drop_conflict_{get_uuid_str()}"
    prepared_name = f"{table_name}_prepared"
    root = "/var/lib/clickhouse/user_files/iceberg_data/default"

    def spark_query(name, query):
        execute_spark_query_general(spark, cluster, "local", name, query)

    for name in (table_name, prepared_name):
        spark_query(
            name,
            f"CREATE TABLE {name} (tag INT, value STRING) USING iceberg "
            "PARTITIONED BY (identity(tag)) OPTIONS('format-version'='2')",
        )

    if shared_manifest:
        spark_query(table_name, f"INSERT INTO {table_name} VALUES (1, 'old'), (2, 'keep')")
    else:
        spark_query(table_name, f"INSERT INTO {table_name} VALUES (1, 'old')")
        spark_query(table_name, f"INSERT INTO {table_name} VALUES (2, 'keep')")
    spark_query(prepared_name, f"INSERT INTO {prepared_name} VALUES (1, 'replacement')")

    iceberg = spark._jvm.org.apache.iceberg
    tables = iceberg.hadoop.HadoopTables(spark._jsc.hadoopConfiguration())
    table = tables.load(f"{root}/{table_name}")
    prepared = tables.load(f"{root}/{prepared_name}")
    tasks = prepared.newScan().planFiles()
    try:
        replacement = tasks.iterator().next().file().copy()
    finally:
        tasks.close()

    # Prepare the write against the old snapshot, before ClickHouse drops the partition.
    starting_snapshot = table.currentSnapshot().snapshotId()
    pending = (
        table.newOverwrite()
        .overwriteByRowFilter(iceberg.expressions.Expressions.equal("tag", 1))
        .addFile(replacement)
        .validateFromSnapshot(starting_snapshot)
        .validateNoConflictingData()
        .validateNoConflictingDeletes()
    )

    create_iceberg_table("local", instance, table_name, cluster)
    instance.query(
        f"ALTER TABLE {table_name} DROP PARTITION 1",
        settings={"allow_insert_into_iceberg": 1},
    )
    assert instance.query(f"SELECT * FROM {table_name} ORDER BY tag") == "2\tkeep\n"
    local_dir = update_spark_version_hint(cluster, "local", table_name)
    table.refresh()
    dropped_snapshot = table.currentSnapshot().snapshotId()
    assert dropped_snapshot != starting_snapshot
    assert len(list(table.currentSnapshot().removedDataFiles(table.io()))) == 1

    # Commit against refreshed metadata: the prepared write must detect the intervening drop.
    with pytest.raises(Py4JJavaError, match="Found conflicting deleted files") as error:
        pending.commit()
    assert (
        error.value.java_exception.getClass().getName()
        == "org.apache.iceberg.exceptions.ValidationException"
    )

    rows = sorted(
        (row["tag"], row["value"])
        for row in spark.read.format("iceberg").load(local_dir).collect()
    )
    assert rows == [(2, "keep")]


@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_drop_partition_whole_manifest_spark_round_trip(
    started_cluster_iceberg_with_spark, storage_type
):
    cluster = started_cluster_iceberg_with_spark
    instance = cluster.instances["node1"]
    spark = cluster.spark_session
    table_name = f"test_drop_partition_whole_manifest_{storage_type}_{get_uuid_str()}"

    def spark_query(query):
        execute_spark_query_general(spark, cluster, storage_type, table_name, query)

    spark_query(
        f"""
        CREATE TABLE {table_name} (tag INT, value STRING)
        USING iceberg
        PARTITIONED BY (identity(tag))
        OPTIONS('format-version'='2')
        """
    )

    # Separate appends keep the partitions in separate manifests, which is the
    # whole-manifest case supported by this change.
    spark_query(f"INSERT INTO {table_name} VALUES (1, 'drop-a'), (1, 'drop-b')")
    spark_query(f"INSERT INTO {table_name} VALUES (2, 'keep-a')")
    spark_query(f"INSERT INTO {table_name} VALUES (3, 'keep-b')")

    create_iceberg_table(storage_type, instance, table_name, cluster)
    instance.query(
        f"ALTER TABLE {table_name} DROP PARTITION 1",
        settings={"allow_insert_into_iceberg": 1},
    )

    expected = "2\tkeep-a\n3\tkeep-b\n"
    assert instance.query(f"SELECT * FROM {table_name} ORDER BY tag") == expected

    history = instance.query(
        f"""
        SELECT operation, summary['removed-data-files'], summary['deleted-records'],
               summary['total-data-files'], summary['total-records']
        FROM system.iceberg_history
        WHERE table = '{table_name}' AND operation = 'DELETE'
        ORDER BY made_current_at DESC
        LIMIT 1
        """
    ).strip()
    assert history == "DELETE\t1\t2\t2\t2", history

    local_dir = update_spark_version_hint(cluster, storage_type, table_name)
    spark_rows = spark.read.format("iceberg").load(local_dir).collect()
    assert sorted((row["tag"], row["value"]) for row in spark_rows) == [
        (2, "keep-a"),
        (3, "keep-b"),
    ]

    # Verify that Spark reads an empty table after all partitions have been dropped.
    instance.query(
        f"ALTER TABLE {table_name} DROP PARTITION 2",
        settings={"allow_insert_into_iceberg": 1},
    )
    instance.query(
        f"ALTER TABLE {table_name} DROP PARTITION 3",
        settings={"allow_insert_into_iceberg": 1},
    )
    assert instance.query(f"SELECT count() FROM {table_name}") == "0\n"

    local_dir = update_spark_version_hint(cluster, storage_type, table_name)
    assert spark.read.format("iceberg").load(local_dir).collect() == []


@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_drop_partition_after_position_delete(
    started_cluster_iceberg_with_spark, storage_type
):
    cluster = started_cluster_iceberg_with_spark
    instance = cluster.instances["node1"]
    spark = cluster.spark_session
    table_name = f"test_drop_partition_after_position_delete_{storage_type}_{get_uuid_str()}"

    def spark_query(query):
        execute_spark_query_general(spark, cluster, storage_type, table_name, query)

    spark_query(
        f"""
        CREATE TABLE {table_name} (tag INT, value STRING)
        USING iceberg
        PARTITIONED BY (identity(tag))
        OPTIONS('format-version'='2')
        """
    )
    spark_query(f"INSERT INTO {table_name} VALUES (1, 'drop-a'), (1, 'drop-b')")
    spark_query(f"INSERT INTO {table_name} VALUES (2, 'keep')")

    create_iceberg_table(storage_type, instance, table_name, cluster)
    instance.query(
        f"ALTER TABLE {table_name} DELETE WHERE tag = 1 AND value = 'drop-a'",
        settings={"allow_insert_into_iceberg": 1},
    )
    assert instance.query(f"SELECT * FROM {table_name} ORDER BY tag, value") == (
        "1\tdrop-b\n2\tkeep\n"
    )

    instance.query(
        f"ALTER TABLE {table_name} DROP PARTITION 1",
        settings={"allow_insert_into_iceberg": 1},
    )
    assert instance.query(f"SELECT * FROM {table_name} ORDER BY tag, value") == (
        "2\tkeep\n"
    )

    local_dir = update_spark_version_hint(cluster, storage_type, table_name)
    spark_rows = spark.read.format("iceberg").load(local_dir).collect()
    assert [(row["tag"], row["value"]) for row in spark_rows] == [(2, "keep")]


@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_drop_partition_shared_spark_manifest(
    started_cluster_iceberg_with_spark, storage_type
):
    cluster = started_cluster_iceberg_with_spark
    instance = cluster.instances["node1"]
    spark = cluster.spark_session
    table_name = f"test_drop_partition_shared_manifest_{storage_type}_{get_uuid_str()}"

    def spark_query(query):
        execute_spark_query_general(spark, cluster, storage_type, table_name, query)

    spark_query(
        f"""
        CREATE TABLE {table_name} (tag INT, value STRING)
        USING iceberg
        PARTITIONED BY (identity(tag))
        OPTIONS('format-version'='2')
        """
    )
    # A single Spark append puts data files from all three partitions into one manifest.
    spark_query(
        f"INSERT INTO {table_name} VALUES (1, 'drop'), (2, 'keep-a'), (3, 'keep-b')"
    )

    create_iceberg_table(storage_type, instance, table_name, cluster)
    instance.query(
        f"ALTER TABLE {table_name} DROP PARTITION 1",
        settings={"allow_insert_into_iceberg": 1},
    )

    expected = "2\tkeep-a\n3\tkeep-b\n"
    assert instance.query(f"SELECT * FROM {table_name} ORDER BY tag") == expected

    local_dir = update_spark_version_hint(cluster, storage_type, table_name)
    spark_rows = spark.read.format("iceberg").load(local_dir).collect()
    assert sorted((row["tag"], row["value"]) for row in spark_rows) == [
        (2, "keep-a"),
        (3, "keep-b"),
    ]


@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_drop_partition_with_spark_evolved_spec_is_rejected(
    started_cluster_iceberg_with_spark, storage_type
):
    cluster = started_cluster_iceberg_with_spark
    instance = cluster.instances["node1"]
    spark = cluster.spark_session
    table_name = f"test_drop_partition_evolved_spec_{storage_type}_{get_uuid_str()}"

    def spark_query(query):
        execute_spark_query_general(spark, cluster, storage_type, table_name, query)

    spark_query(
        f"""
        CREATE TABLE {table_name} (tag INT, key STRING, value INT)
        USING iceberg
        PARTITIONED BY (identity(tag))
        OPTIONS('format-version'='2')
        """
    )
    spark_query(f"INSERT INTO {table_name} VALUES (1, 'a', 10), (2, 'b', 20)")
    spark_query(f"ALTER TABLE {table_name} ADD PARTITION FIELD identity(key)")
    spark_query(f"INSERT INTO {table_name} VALUES (1, 'c', 30), (3, 'd', 40)")

    create_iceberg_table(storage_type, instance, table_name, cluster)

    error = instance.query_and_get_error(
        f"ALTER TABLE {table_name} DROP PARTITION 1",
        settings={"allow_insert_into_iceberg": 1},
    )
    assert "NOT_IMPLEMENTED" in error, error
    assert "evolved partition specs" in error, error
    assert instance.query(f"SELECT count() FROM {table_name}") == "4\n"
