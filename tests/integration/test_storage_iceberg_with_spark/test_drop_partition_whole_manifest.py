import os
import re

import pytest

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

    # Verify that Spark accepts the header-only manifest list produced after
    # `DROP PARTITION` removes the last remaining manifests.
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
def test_drop_partition_shared_spark_manifest_is_rejected(
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
    error = instance.query_and_get_error(
        f"ALTER TABLE {table_name} DROP PARTITION 1",
        settings={"allow_insert_into_iceberg": 1},
    )
    assert "NOT_IMPLEMENTED" in error, error
    assert "rewriting a manifest" in error, error

    expected = "1\tdrop\n2\tkeep-a\n3\tkeep-b\n"
    assert instance.query(f"SELECT * FROM {table_name} ORDER BY tag") == expected


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
