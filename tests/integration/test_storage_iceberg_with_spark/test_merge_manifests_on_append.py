"""The merge of manifests on a ClickHouse append to an Iceberg table written by Spark.

The merge is an optimization: the append must succeed, and keep the manifests as they are when they cannot be
rewritten.
"""

import glob
import json
import os
import re

import avro.datafile
import avro.io
import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    default_download_directory,
    execute_spark_query_general,
    get_uuid_str,
)

MERGE_SETTINGS = {"allow_insert_into_iceberg": 1, "iceberg_manifest_min_count_to_compact": 2}


def download_table(cluster, storage_type, table_name):
    local_dir = f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}/"
    default_download_directory(cluster, storage_type, local_dir, local_dir)
    return local_dir


def latest_metadata(local_dir):
    def version(path):
        return int(re.search(r"v(\d+)[^/]*\.metadata\.json$", path).group(1))

    paths = glob.glob(os.path.join(local_dir, "metadata", "*.metadata.json"))
    return json.load(open(max(paths, key=version)))


def current_manifests(local_dir):
    metadata = latest_metadata(local_dir)
    snapshot = next(s for s in metadata["snapshots"] if s["snapshot-id"] == metadata["current-snapshot-id"])
    manifest_list = snapshot["manifest-list"].removeprefix("file://")
    manifest_list = os.path.join(local_dir, "metadata", os.path.basename(manifest_list))
    with open(manifest_list, "rb") as f:
        return list(avro.datafile.DataFileReader(f, avro.io.DatumReader()))


def update_version_hint(local_dir):
    metadata_dir = os.path.join(local_dir, "metadata")
    versions = [
        int(m.group(1))
        for m in (re.match(r"v(\d+)(?:[-.].*)?\.metadata\.json$", name) for name in os.listdir(metadata_dir))
        if m
    ]
    with open(os.path.join(metadata_dir, "version-hint.text"), "w") as hint:
        hint.write(str(max(versions)))


@pytest.mark.parametrize("storage_type", ["local"])
def test_merge_manifests_after_schema_cleanup(started_cluster_iceberg_with_spark, storage_type):
    """Live manifests can be written under a schema that expiring snapshots removed from the metadata."""
    cluster = started_cluster_iceberg_with_spark
    instance = cluster.instances["node1"]
    spark = cluster.spark_session
    table_name = f"test_merge_manifests_after_schema_cleanup_{get_uuid_str()}"

    def spark_query(query):
        execute_spark_query_general(spark, cluster, storage_type, table_name, query)

    spark_query(f"CREATE TABLE {table_name} (a INT) USING iceberg OPTIONS('format-version'='2')")
    spark_query(f"INSERT INTO {table_name} VALUES (1)")
    spark_query(f"INSERT INTO {table_name} VALUES (2)")
    spark_query(f"ALTER TABLE {table_name} ADD COLUMN b INT")
    spark_query(f"INSERT INTO {table_name} VALUES (3, 30)")
    spark_query(
        f"""
        CALL system.expire_snapshots(
            table => '{table_name}',
            older_than => TIMESTAMP '2100-01-01 00:00:00',
            retain_last => 1,
            clean_expired_metadata => true)
        """
    )

    local_dir = download_table(cluster, storage_type, table_name)
    schema_ids = [schema["schema-id"] for schema in latest_metadata(local_dir)["schemas"]]
    assert schema_ids == [1], schema_ids
    assert len(current_manifests(local_dir)) == 3

    create_iceberg_table(storage_type, instance, table_name, cluster)
    instance.query(f"INSERT INTO {table_name} VALUES (4, 40)", settings=MERGE_SETTINGS)

    expected = "1\t\\N\n2\t\\N\n3\t30\n4\t40\n"
    assert instance.query(f"SELECT * FROM {table_name} ORDER BY a") == expected

    local_dir = download_table(cluster, storage_type, table_name)
    # The two manifests of schema 0 and the one of schema 1 are merged per schema.
    assert len(current_manifests(local_dir)) < 4

    update_version_hint(local_dir)
    rows = spark.read.format("iceberg").load(local_dir).collect()
    assert sorted((row["a"], row["b"]) for row in rows) == [(1, None), (2, None), (3, 30), (4, 40)]


@pytest.mark.parametrize("storage_type", ["local"])
@pytest.mark.parametrize("nested", [False, True])
def test_merge_manifests_unsupported_historical_transform(started_cluster_iceberg_with_spark, storage_type, nested):
    """`icebergTruncate` does not support `decimal(20, 2)`, so the manifests of such a spec are kept as they are.

    With `nested`, the source column is a field of a struct, which the check of the transform must also find.
    """
    cluster = started_cluster_iceberg_with_spark
    instance = cluster.instances["node1"]
    spark = cluster.spark_session
    table_name = f"test_merge_manifests_unsupported_historical_transform_{get_uuid_str()}"

    def spark_query(query):
        execute_spark_query_general(spark, cluster, storage_type, table_name, query)

    if nested:
        column, source = "s STRUCT<d: DECIMAL(20, 2)>", "s.d"

        def value(d):
            return f"named_struct('d', CAST({d} AS DECIMAL(20, 2)))"

        def clickhouse_value(d):
            return f"tuple({d})"

    else:
        column, source = "d DECIMAL(20, 2)", "d"

        def value(d):
            return d

        clickhouse_value = value

    spark_query(
        f"""
        CREATE TABLE {table_name} ({column}, v INT)
        USING iceberg
        PARTITIONED BY (truncate(10, {source}))
        OPTIONS('format-version'='2')
        """
    )
    spark_query(f"INSERT INTO {table_name} VALUES ({value('1.50')}, 1)")
    spark_query(f"INSERT INTO {table_name} VALUES ({value('25.00')}, 2)")
    spark_query(f"ALTER TABLE {table_name} DROP PARTITION FIELD truncate(10, {source})")

    create_iceberg_table(storage_type, instance, table_name, cluster)
    instance.query(f"INSERT INTO {table_name} VALUES ({clickhouse_value('3.25')}, 3)", settings=MERGE_SETTINGS)

    local_dir = download_table(cluster, storage_type, table_name)
    assert len(current_manifests(local_dir)) == 3

    update_version_hint(local_dir)
    rows = spark.read.format("iceberg").load(local_dir).collect()
    assert sorted(row["v"] for row in rows) == [1, 2, 3]
