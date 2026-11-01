import glob
import json
import os
import re
import time

import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    default_download_directory,
    default_upload_directory,
    get_uuid_str,
)

ICEBERG_SETTINGS = {"allow_insert_into_iceberg": 1}
LOCAL_TABLE_PREFIX = "/var/lib/clickhouse/user_files/iceberg_data/default"
S3_TABLE_PREFIX = "var/lib/clickhouse/user_files/iceberg_data/default"


def metadata_version(name):
    return int(re.match(r"v(\d+)\.", name).group(1))


def list_metadata_files(cluster, storage_type, table_name):
    """Names of the `*.metadata.json` objects in the table's storage."""
    if storage_type == "local":
        output = cluster.instances["node1"].exec_in_container(
            ["bash", "-c", f"ls {LOCAL_TABLE_PREFIX}/{table_name}/metadata"]
        )
        names = output.split()
    else:
        names = [
            obj.object_name.rsplit("/", 1)[-1]
            for obj in cluster.minio_client.list_objects(
                cluster.minio_bucket, prefix=f"{S3_TABLE_PREFIX}/{table_name}/metadata/", recursive=True
            )
        ]
    return {name for name in names if name.endswith(".metadata.json")}


def read_latest_metadata(cluster, storage_type, table_name):
    name = max(list_metadata_files(cluster, storage_type, table_name), key=metadata_version)
    if storage_type == "local":
        raw = cluster.instances["node1"].exec_in_container(
            ["cat", f"{LOCAL_TABLE_PREFIX}/{table_name}/metadata/{name}"]
        )
    else:
        raw = cluster.minio_client.get_object(
            cluster.minio_bucket, f"{S3_TABLE_PREFIX}/{table_name}/metadata/{name}"
        ).read()
    metadata = json.loads(raw)
    log = [os.path.basename(entry["metadata-file"]) for entry in metadata.get("metadata-log", [])]
    return name, log


def download_for_spark(cluster, storage_type, table_name):
    """Copy the table to the host and point version-hint.text at the newest version, as Spark expects."""
    table_dir = f"{LOCAL_TABLE_PREFIX}/{table_name}/"
    default_download_directory(cluster, storage_type, table_dir, table_dir)
    versions = [metadata_version(os.path.basename(f)) for f in glob.glob(f"{table_dir}metadata/v*.metadata.json")]
    with open(f"{table_dir}metadata/version-hint.text", "w") as f:
        f.write(str(max(versions)))
    return table_dir


def create_table_keeping_two_previous_files(cluster, storage_type, table_name, schema, **kwargs):
    """Create the table and set `previous-versions-max = 2` and `delete-after-commit.enabled = true` with Spark."""
    create_iceberg_table(storage_type, cluster.instances["node1"], table_name, cluster, schema, **kwargs)
    # ClickHouse has no DDL for Iceberg table properties.
    table_dir = download_for_spark(cluster, storage_type, table_name)
    cluster.spark_session.sql(
        f"ALTER TABLE {table_name} SET TBLPROPERTIES("
        "'write.metadata.previous-versions-max' = '2', 'write.metadata.delete-after-commit.enabled' = 'true')"
    )
    default_upload_directory(cluster, storage_type, table_dir, table_dir)


def test_metadata_log_default_limit(started_cluster_iceberg_with_spark):
    """Without table properties `metadata-log` keeps 100 previous files, so `remove_orphan_files` collects older ones."""
    cluster = started_cluster_iceberg_with_spark
    instance = cluster.instances["node1"]
    storage_type = "local"
    table_name = f"test_metadata_log_default_limit_{get_uuid_str()}"

    create_iceberg_table(storage_type, instance, table_name, cluster, "(x Int)")
    instance.query(
        "".join(f"INSERT INTO {table_name} VALUES ({i});" for i in range(1, 102)),
        settings=ICEBERG_SETTINGS,
    )

    latest, log = read_latest_metadata(cluster, storage_type, table_name)
    assert latest == "v102.metadata.json"
    assert log == [f"v{i}.metadata.json" for i in range(2, 102)]
    assert "v1.metadata.json" in list_metadata_files(cluster, storage_type, table_name)

    time.sleep(2)
    older_than = time.strftime("%Y-%m-%d %H:%M:%S")
    result = instance.query(
        f"ALTER TABLE {table_name} EXECUTE remove_orphan_files(older_than = '{older_than}')",
        settings={
            **ICEBERG_SETTINGS,
            "allow_iceberg_remove_orphan_files": 1,
            "iceberg_orphan_files_min_retention_seconds": 0,
        },
    )
    counts = dict(line.split("\t") for line in result.strip().split("\n"))
    assert counts["deleted_metadata_files_count"] == "1", counts

    assert list_metadata_files(cluster, storage_type, table_name) == {latest, *log}
    assert instance.query(f"SELECT count() FROM {table_name}") == "101\n"


@pytest.mark.parametrize("storage_type", ["local", "s3"])
def test_metadata_log_limit_keeps_files(started_cluster_iceberg_with_spark, storage_type):
    """Every writer keeps `write.metadata.previous-versions-max` files in `metadata-log` and deletes none.

    A commit is refused only if the metadata file of its version exists, so a deleted version could be committed
    again by a delayed writer. `write.metadata.delete-after-commit.enabled` is therefore not honored.
    """
    cluster = started_cluster_iceberg_with_spark
    instance = cluster.instances["node1"]
    spark = cluster.spark_session
    table_name = f"test_metadata_log_limit_keeps_files_{storage_type}_{get_uuid_str()}"

    create_table_keeping_two_previous_files(cluster, storage_type, table_name, "(x Int, p Int)", partition_by="p")

    writers = [
        (f"INSERT INTO {table_name} VALUES (1, 1), (2, 2)", ICEBERG_SETTINGS),
        # A second manifest for partition 1 gives `OPTIMIZE ... MANIFEST` something to consolidate.
        (f"INSERT INTO {table_name} VALUES (3, 1)", ICEBERG_SETTINGS),
        (f"INSERT INTO {table_name} VALUES (4, 3)", ICEBERG_SETTINGS),
        (f"ALTER TABLE {table_name} ADD COLUMN y Nullable(Int32)", ICEBERG_SETTINGS),
        (f"ALTER TABLE {table_name} DELETE WHERE x = 1", {**ICEBERG_SETTINGS, "mutations_sync": 2}),
        (f"ALTER TABLE {table_name} DROP PARTITION 3", ICEBERG_SETTINGS),
        (
            f"OPTIMIZE TABLE {table_name} MANIFEST",
            {"allow_experimental_iceberg_compaction": 1, "iceberg_manifest_min_count_to_compact": 1},
        ),
        (
            f"ALTER TABLE {table_name} EXECUTE expire_snapshots(retain_last = 1, retention_period = '1ms')",
            {**ICEBERG_SETTINGS, "allow_experimental_expire_snapshots": 1},
        ),
    ]

    latest, _ = read_latest_metadata(cluster, storage_type, table_name)
    for query, settings in writers:
        previous = latest
        instance.query(query, settings=settings)
        latest, log = read_latest_metadata(cluster, storage_type, table_name)
        assert metadata_version(latest) == metadata_version(previous) + 1, query
        assert len(log) == 2, (query, log)
        all_versions = {f"v{i}.metadata.json" for i in range(1, metadata_version(latest) + 1)}
        assert list_metadata_files(cluster, storage_type, table_name) == all_versions, query

    assert instance.query(f"SELECT x, p FROM {table_name} ORDER BY x") == "2\t2\n3\t1\n"
    if storage_type == "local":
        table_dir = download_for_spark(cluster, storage_type, table_name)
        assert spark.read.format("iceberg").load(table_dir).count() == 2

