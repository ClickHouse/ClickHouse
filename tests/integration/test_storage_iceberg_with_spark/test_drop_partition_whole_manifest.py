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
def test_drop_partition_shared_manifest(
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
    # The shared manifest is rewritten without the files of the dropped partition.
    instance.query(
        f"ALTER TABLE {table_name} DROP PARTITION 1",
        settings={"allow_insert_into_iceberg": 1},
    )

    expected = "2\tkeep-a\n3\tkeep-b\n"
    assert instance.query(f"SELECT * FROM {table_name} ORDER BY tag") == expected
    assert (
        instance.query(f"SELECT * FROM {table_name} WHERE tag = 3") == "3\tkeep-b\n"
    )

    # With `iceberg_insert_single_manifest_for_all_partitions`, a ClickHouse INSERT spanning several partitions writes them into one manifest as well.
    instance.query(
        f"INSERT INTO {table_name} VALUES (4, 'new-a'), (5, 'new-b'), (2, 'new-c')",
        settings={
            "allow_insert_into_iceberg": 1,
            "iceberg_insert_single_manifest_for_all_partitions": 1,
        },
    )
    instance.query(
        f"ALTER TABLE {table_name} DROP PARTITION 4",
        settings={"allow_insert_into_iceberg": 1},
    )

    expected = [(2, "keep-a"), (2, "new-c"), (3, "keep-b"), (5, "new-b")]
    assert instance.query(f"SELECT * FROM {table_name} ORDER BY tag, value") == "".join(
        f"{tag}\t{value}\n" for tag, value in expected
    )

    local_dir = update_spark_version_hint(cluster, storage_type, table_name)
    df = spark.read.format("iceberg").load(local_dir)
    assert sorted((row["tag"], row["value"]) for row in df.collect()) == expected
    assert sorted(row["value"] for row in df.where("tag = 2").collect()) == [
        "keep-a",
        "new-c",
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


@pytest.mark.parametrize("storage_type", ["local"])
def test_merge_float_partitioned_spark_manifests_on_insert(
    started_cluster_iceberg_with_spark, storage_type
):
    cluster = started_cluster_iceberg_with_spark
    instance = cluster.instances["node1"]
    spark = cluster.spark_session
    table_name = f"test_merge_float_partitioned_manifests_{storage_type}_{get_uuid_str()}"

    def spark_query(query):
        execute_spark_query_general(spark, cluster, storage_type, table_name, query)

    spark_query(
        f"""
        CREATE TABLE {table_name} (f FLOAT, value INT)
        USING iceberg
        PARTITIONED BY (identity(f))
        OPTIONS('format-version'='2')
        """
    )
    spark_query(f"INSERT INTO {table_name} VALUES (CAST(1.5 AS FLOAT), 1)")
    spark_query(f"INSERT INTO {table_name} VALUES (CAST(2.5 AS FLOAT), 2)")
    # New data files are unpartitioned, so the manifests of the old spec stay small and are merged.
    spark_query(f"ALTER TABLE {table_name} DROP PARTITION FIELD f")

    create_iceberg_table(storage_type, instance, table_name, cluster)
    for value in (3, 4):
        instance.query(
            f"INSERT INTO {table_name} VALUES ({value}.5, {value})",
            settings={
                "allow_insert_into_iceberg": 1,
                "iceberg_manifest_min_count_to_compact": 2,
            },
        )

    expected = "1.5\t1\n2.5\t2\n3.5\t3\n4.5\t4\n"
    assert instance.query(f"SELECT * FROM {table_name} ORDER BY value") == expected

    # The `float` partition values of the rewritten manifest are readable by Spark.
    local_dir = update_spark_version_hint(cluster, storage_type, table_name)
    spark_rows = spark.read.format("iceberg").load(local_dir).collect()
    assert sorted((row["f"], row["value"]) for row in spark_rows) == [
        (1.5, 1),
        (2.5, 2),
        (3.5, 3),
        (4.5, 4),
    ]


@pytest.mark.parametrize("storage_type", ["local"])
def test_insert_skips_merge_of_unsupported_partition_manifests(
    started_cluster_iceberg_with_spark, storage_type
):
    cluster = started_cluster_iceberg_with_spark
    instance = cluster.instances["node1"]
    spark = cluster.spark_session
    table_name = f"test_merge_unsupported_partition_manifests_{storage_type}_{get_uuid_str()}"

    def spark_query(query):
        execute_spark_query_general(spark, cluster, storage_type, table_name, query)

    # The manifests of the old spec cannot be rewritten: an Iceberg `binary` partition value must stay Avro `bytes`,
    # and the source of the other partition field is a nested column. The `INSERT` still succeeds without merging them.
    spark_query(
        f"""
        CREATE TABLE {table_name} (b BINARY, s STRUCT<x: INT>, value INT)
        USING iceberg
        PARTITIONED BY (b, s.x)
        OPTIONS('format-version'='2')
        """
    )
    spark_query(f"INSERT INTO {table_name} VALUES (X'FF', named_struct('x', 1), 1)")
    spark_query(f"INSERT INTO {table_name} VALUES (X'FE', named_struct('x', 2), 2)")
    spark_query(f"ALTER TABLE {table_name} DROP PARTITION FIELD b")
    spark_query(f"ALTER TABLE {table_name} DROP PARTITION FIELD s.x")

    create_iceberg_table(storage_type, instance, table_name, cluster)
    for value in (3, 4):
        instance.query(
            f"INSERT INTO {table_name} (value) VALUES ({value})",
            settings={
                "allow_insert_into_iceberg": 1,
                "iceberg_manifest_min_count_to_compact": 2,
            },
        )

    assert instance.query(f"SELECT hex(b), value FROM {table_name} ORDER BY value") == (
        "FF\t1\nFE\t2\n\\N\t3\n\\N\t4\n"
    )

    local_dir = update_spark_version_hint(cluster, storage_type, table_name)
    spark_rows = spark.read.format("iceberg").load(local_dir).collect()
    assert sorted((row["value"], row["b"]) for row in spark_rows) == [
        (1, bytearray(b"\xff")),
        (2, bytearray(b"\xfe")),
        (3, None),
        (4, None),
    ]
