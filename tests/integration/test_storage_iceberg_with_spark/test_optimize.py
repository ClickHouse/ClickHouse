import json
import pytest
from datetime import datetime, timezone
import time

from helpers.iceberg_utils import (
    create_iceberg_table,
    default_upload_directory,
    default_download_directory,
    get_uuid_str,
    get_last_snapshot,
    spark_alter_table,
)

@pytest.mark.parametrize("storage_type", ["local", "s3", "azure"])
def test_optimize(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_optimize_" + storage_type + "_" + get_uuid_str()

    spark.sql(
        f"""
        CREATE TABLE {TABLE_NAME} (id long, data string) USING iceberg TBLPROPERTIES ('format-version' = '2', 'write.update.mode'=
        'merge-on-read', 'write.delete.mode'='merge-on-read', 'write.merge.mode'='merge-on-read')
        """
    )
    spark.sql(f"INSERT INTO {TABLE_NAME} select id, char(id + ascii('a')) from range(10, 100)")

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)
    snapshot_id = get_last_snapshot(f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/")
    snapshot_timestamp = datetime.now(timezone.utc)

    time.sleep(0.1)
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 90

    spark.sql(f"DELETE FROM {TABLE_NAME} WHERE id < 20")
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )
    spark.sql(f"INSERT INTO {TABLE_NAME} select id, char(id + ascii('a')) from range(100, 110)")
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 90

    instance.query(f"OPTIMIZE TABLE {TABLE_NAME};", settings={"allow_experimental_iceberg_compaction" : 1})

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 90
    assert instance.query(f"SELECT id FROM {TABLE_NAME} ORDER BY id") == instance.query(
        "SELECT number FROM numbers(20, 90)"
    )

    # check that timetravel works with previous snapshot_ids and timestamps
    assert instance.query(f"SELECT id FROM {TABLE_NAME} ORDER BY id SETTINGS iceberg_snapshot_id = {snapshot_id}") == instance.query(
        "SELECT number FROM numbers(20, 80)"
    )

    assert instance.query(f"SELECT id FROM {TABLE_NAME} ORDER BY id SETTINGS iceberg_timestamp_ms = {int(snapshot_timestamp.timestamp() * 1000)}") == instance.query(
        "SELECT number FROM numbers(20, 80)"
    )
    if storage_type == "azure":
        return

    default_download_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
    )
    df = spark.read.format("iceberg").load(f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}").collect()
    assert len(df) == 90


def test_optimize_manifest_per_file_stats(started_cluster_iceberg_with_spark):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    storage_type = "local"
    TABLE_NAME = "test_optimize_stats_" + storage_type + "_" + get_uuid_str()

    spark.sql(
        f"""
        CREATE TABLE {TABLE_NAME} (id long, data string) USING iceberg TBLPROPERTIES (
            'format-version' = '2',
            'write.update.mode' = 'merge-on-read',
            'write.delete.mode' = 'merge-on-read',
            'write.merge.mode' = 'merge-on-read'
        )
        """
    )
    spark.sql(
        f"INSERT INTO {TABLE_NAME} SELECT id, char(id + ascii('a')) FROM range(10, 100)"
    )

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )
    create_iceberg_table(
        storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark
    )

    spark.sql(f"DELETE FROM {TABLE_NAME} WHERE id < 20")
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    instance.query(
        f"OPTIMIZE TABLE {TABLE_NAME};",
        settings={"allow_experimental_iceberg_compaction": 1},
    )
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 80

    metadata_dir = (
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/metadata"
    )
    manifest_files = (
        instance.exec_in_container(
            [
                "bash",
                "-c",
                f"find '{metadata_dir}' -maxdepth 1 -name '*.avro' "
                f"-not -name 'snap-*.avro' -type f",
            ]
        )
        .strip()
        .splitlines()
    )
    assert manifest_files

    data_entries_checked = 0
    for manifest in manifest_files:
        result = instance.query(
            f"""
            SELECT
                tupleElement(data_file, 'content')             AS content,
                tupleElement(data_file, 'file_path')           AS file_path,
                tupleElement(data_file, 'record_count')        AS record_count,
                tupleElement(data_file, 'file_size_in_bytes')  AS file_size
            FROM file('{manifest}', Avro)
            FORMAT TSV
            """
        ).strip()
        if not result:
            continue
        for line in result.splitlines():
            content, file_path, record_count, file_size = line.split("\t")
            if int(content) != 0:
                continue

            exists = instance.exec_in_container(
                ["bash", "-c", f"test -f '{file_path}' && echo yes || echo no"]
            ).strip()
            if exists != "yes":
                continue

            actual_size = int(
                instance.exec_in_container(
                    ["bash", "-c", f"wc -c < '{file_path}'"]
                ).strip()
            )
            assert int(file_size) == actual_size

            actual_rows = int(
                instance.query(
                    f"SELECT count() FROM file('{file_path}', Parquet)"
                ).strip()
            )
            assert int(record_count) == actual_rows
            data_entries_checked += 1

    assert data_entries_checked > 0


@pytest.mark.parametrize("storage_type", ["local"])
def test_optimize_rejected_when_gc_disabled(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_optimize_gc_disabled_" + get_uuid_str()

    spark.sql(
        f"""
        CREATE TABLE {TABLE_NAME} (id long, data string) USING iceberg TBLPROPERTIES (
            'format-version' = '2',
            'gc.enabled' = 'false',
            'write.update.mode' = 'merge-on-read',
            'write.delete.mode' = 'merge-on-read',
            'write.merge.mode' = 'merge-on-read'
        )
        """
    )
    spark.sql(f"INSERT INTO {TABLE_NAME} SELECT id, char(id + ascii('a')) FROM range(10, 100)")
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)
    spark.sql(f"DELETE FROM {TABLE_NAME} WHERE id < 20")
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    table_dir = f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/"
    files_before = set(default_download_directory(
        started_cluster_iceberg_with_spark, storage_type, table_dir, table_dir,
    ))
    error = instance.query_and_get_error(
        f"OPTIMIZE TABLE {TABLE_NAME};",
        settings={"allow_experimental_iceberg_compaction": 1},
    )

    assert "BAD_ARGUMENTS" in error
    assert "GC is disabled" in error
    assert set(default_download_directory(
        started_cluster_iceberg_with_spark, storage_type, table_dir, table_dir,
    )) == files_before
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 80


@pytest.mark.parametrize("storage_type", ["local"])
def test_optimize_ignores_pinned_metadata_when_gc_disabled(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_optimize_pinned_metadata_" + get_uuid_str()

    spark.sql(
        f"""
        CREATE TABLE {TABLE_NAME} (id long, data string) USING iceberg TBLPROPERTIES (
            'format-version' = '2',
            'gc.enabled' = 'true',
            'write.update.mode' = 'merge-on-read',
            'write.delete.mode' = 'merge-on-read',
            'write.merge.mode' = 'merge-on-read'
        )
        """
    )
    spark.sql(f"INSERT INTO {TABLE_NAME} SELECT id, char(id + ascii('a')) FROM range(0, 100)")
    spark.sql(f"DELETE FROM {TABLE_NAME} WHERE id < 20")

    table_dir = f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/"
    default_upload_directory(
        started_cluster_iceberg_with_spark, storage_type, table_dir, table_dir,
    )
    metadata_dir = f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/metadata"
    old_metadata_file = instance.exec_in_container(
        ["bash", "-c", f"ls -v {metadata_dir}/v*.metadata.json | tail -1"]
    ).strip()
    old_metadata_path = "metadata/" + old_metadata_file.rsplit("/", 1)[-1]
    with open(old_metadata_file) as metadata_handle:
        old_metadata = json.load(metadata_handle)
    assert old_metadata["properties"].get("gc.enabled") == "true"
    assert any(
        int(snapshot.get("summary", {}).get("added-position-delete-files", "0")) > 0
        for snapshot in old_metadata["snapshots"]
    ), "Fixture must contain a positional delete so OPTIMIZE reaches the rewrite path"

    spark_alter_table(
        started_cluster_iceberg_with_spark,
        spark,
        storage_type,
        TABLE_NAME,
        "SET TBLPROPERTIES('gc.enabled' = 'false')",
    )
    latest_metadata_file = instance.exec_in_container(
        ["bash", "-c", f"ls -v {metadata_dir}/v*.metadata.json | tail -1"]
    ).strip()
    assert latest_metadata_file != old_metadata_file
    with open(latest_metadata_file) as metadata_handle:
        latest_metadata = json.load(metadata_handle)
    assert latest_metadata["properties"].get("gc.enabled") == "false"

    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_with_spark,
        explicit_metadata_path=old_metadata_path,
    )
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 80

    files_before = set(default_download_directory(
        started_cluster_iceberg_with_spark, storage_type, table_dir, table_dir,
    ))
    error = instance.query_and_get_error(
        f"OPTIMIZE TABLE {TABLE_NAME};",
        settings={"allow_experimental_iceberg_compaction": 1},
    )

    assert "BAD_ARGUMENTS" in error
    assert "GC is disabled" in error
    assert set(default_download_directory(
        started_cluster_iceberg_with_spark, storage_type, table_dir, table_dir,
    )) == files_before
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 80


@pytest.mark.parametrize("storage_type", ["local"])
def test_optimize_uses_current_schema_with_pinned_metadata(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_optimize_pinned_schema_" + get_uuid_str()

    spark.sql(
        f"""
        CREATE TABLE {TABLE_NAME} (id long, data string) USING iceberg TBLPROPERTIES (
            'format-version' = '2',
            'write.update.mode' = 'merge-on-read',
            'write.delete.mode' = 'merge-on-read',
            'write.merge.mode' = 'merge-on-read'
        )
        """
    )
    spark.sql(f"INSERT INTO {TABLE_NAME} SELECT id, char(id + ascii('a')) FROM range(0, 100)")

    table_dir = f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/"
    default_upload_directory(
        started_cluster_iceberg_with_spark, storage_type, table_dir, table_dir,
    )
    metadata_dir = f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/metadata"
    old_metadata_file = instance.exec_in_container(
        ["bash", "-c", f"ls -v {metadata_dir}/v*.metadata.json | tail -1"]
    ).strip()
    old_metadata_path = "metadata/" + old_metadata_file.rsplit("/", 1)[-1]

    spark_alter_table(
        started_cluster_iceberg_with_spark,
        spark,
        storage_type,
        TABLE_NAME,
        "ADD COLUMN newer_col string",
    )
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (100, 'new-data', 'important')")
    default_upload_directory(
        started_cluster_iceberg_with_spark, storage_type, table_dir, table_dir,
    )
    spark.sql(f"DELETE FROM {TABLE_NAME} WHERE id < 20")
    default_upload_directory(
        started_cluster_iceberg_with_spark, storage_type, table_dir, table_dir,
    )

    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_with_spark,
        explicit_metadata_path=old_metadata_path,
    )
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 100

    instance.query(
        f"OPTIMIZE TABLE {TABLE_NAME};",
        settings={"allow_experimental_iceberg_compaction": 1},
    )

    default_download_directory(
        started_cluster_iceberg_with_spark, storage_type, table_dir, table_dir,
    )
    latest_metadata_file = instance.exec_in_container(
        ["bash", "-c", f"ls -v {metadata_dir}/v*.metadata.json | tail -1"]
    ).strip()
    with open(latest_metadata_file) as metadata_handle:
        latest_metadata = json.load(metadata_handle)
    current_schema_id = latest_metadata["current-schema-id"]
    current_schema = next(
        schema for schema in latest_metadata["schemas"] if schema["schema-id"] == current_schema_id
    )
    assert any(field["name"] == "newer_col" for field in current_schema["fields"])

    current_table = spark.read.format("iceberg").load(table_dir)
    assert current_table.count() == 81
    assert current_table.where("id = 100").select("newer_col").collect()[0][0] == "important"


@pytest.mark.parametrize("storage_type", ["local"])
def test_optimize_uses_latest_metadata_compression(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_optimize_metadata_compression_" + get_uuid_str()

    spark.sql(
        f"""
        CREATE TABLE {TABLE_NAME} (id long, data string) USING iceberg TBLPROPERTIES (
            'format-version' = '2',
            'gc.enabled' = 'true',
            'write.update.mode' = 'merge-on-read',
            'write.delete.mode' = 'merge-on-read',
            'write.merge.mode' = 'merge-on-read'
        )
        """
    )
    spark.sql(f"INSERT INTO {TABLE_NAME} SELECT id, char(id + ascii('a')) FROM range(0, 100)")

    table_dir = f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/"
    default_upload_directory(
        started_cluster_iceberg_with_spark, storage_type, table_dir, table_dir,
    )

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)

    spark.sql(f"DELETE FROM {TABLE_NAME} WHERE id < 20")
    default_upload_directory(
        started_cluster_iceberg_with_spark, storage_type, table_dir, table_dir,
    )

    metadata_dir = f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/metadata"
    latest_metadata_file = instance.exec_in_container(
        ["bash", "-c", f"ls -v {metadata_dir}/v*.metadata.json | tail -1"]
    ).strip()
    compressed_metadata_file = latest_metadata_file.replace(".metadata.json", ".gz.metadata.json")
    instance.exec_in_container(
        [
            "bash",
            "-c",
            f"gzip '{latest_metadata_file}' && mv '{latest_metadata_file}.gz' '{compressed_metadata_file}'",
        ]
    )

    instance.query(
        f"OPTIMIZE TABLE {TABLE_NAME};",
        settings={"allow_experimental_iceberg_compaction": 1},
    )
    compacted_metadata_file = instance.exec_in_container(
        ["bash", "-c", f"ls -v {metadata_dir}/v*.gz.metadata.json | tail -1"]
    ).strip()
    instance.exec_in_container(["gzip", "-t", compacted_metadata_file])
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 80
