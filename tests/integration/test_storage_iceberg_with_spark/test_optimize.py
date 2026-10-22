import pytest
from datetime import datetime, timezone
import time

from helpers.iceberg_utils import (
    create_iceberg_table,
    default_upload_directory,
    default_download_directory,
    get_uuid_str,
    get_last_snapshot
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

    # The summary of the compaction's snapshot describes the compaction, not the Spark write it is built on.
    spark_keys = "arrayFilter(k -> startsWith(k, 'spark.') OR k IN ('engine-name', 'engine-version', 'app-id'), mapKeys(summary))"
    latest = instance.query(
        f"SELECT operation, {spark_keys} FROM system.iceberg_history WHERE table = '{TABLE_NAME}' AND is_current_ancestor "
        "ORDER BY made_current_at DESC LIMIT 2"
    ).splitlines()
    assert latest[0] == "REPLACE\t[]" and latest[1].startswith("APPEND\t['"), latest

    # Earlier snapshots are not changed by the compaction: time travel still returns the rows before the DELETE.
    assert instance.query(f"SELECT id FROM {TABLE_NAME} ORDER BY id SETTINGS iceberg_snapshot_id = {snapshot_id}") == instance.query(
        "SELECT number FROM numbers(10, 90)"
    )

    assert instance.query(f"SELECT id FROM {TABLE_NAME} ORDER BY id SETTINGS iceberg_timestamp_ms = {int(snapshot_timestamp.timestamp() * 1000)}") == instance.query(
        "SELECT number FROM numbers(10, 90)"
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

    metadata_dir = (
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/metadata"
    )

    def list_manifest_files():
        return set(
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

    manifests_before = list_manifest_files()
    instance.query(
        f"OPTIMIZE TABLE {TABLE_NAME};",
        settings={"allow_experimental_iceberg_compaction": 1},
    )
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 80

    # Only the manifests written by OPTIMIZE: the earlier ones stay as they were.
    manifest_files = sorted(list_manifest_files() - manifests_before)
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


def test_optimize_records_replaced_files_as_deleted(started_cluster_iceberg_with_spark):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    storage_type = "local"
    TABLE_NAME = "test_optimize_deleted_entries_" + get_uuid_str()
    table_path = f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}"

    spark.sql(
        f"""
        CREATE TABLE {TABLE_NAME} (id long) USING iceberg TBLPROPERTIES (
            'format-version' = '2',
            'write.delete.mode' = 'merge-on-read'
        )
        """
    )
    spark.sql(f"INSERT INTO {TABLE_NAME} SELECT id FROM range(0, 10)")
    spark.sql(f"INSERT INTO {TABLE_NAME} SELECT id FROM range(10, 20)")
    spark.sql(f"DELETE FROM {TABLE_NAME} WHERE id = 1")
    # A row-level delete of every row of the second file: compaction replaces it with no file.
    spark.sql(f"DELETE FROM {TABLE_NAME} WHERE id >= 10 AND id % 2 >= 0")

    def read_entries():
        return spark.read.format("iceberg").load(f"{table_path}#entries").selectExpr(
            "status", "snapshot_id", "sequence_number", "file_sequence_number",
            "data_file.content AS content", "data_file.file_path AS file_path"
        ).collect()

    entries_before = {row.file_path: row for row in read_entries()}
    replaced_files = {path for path, row in entries_before.items() if row.content == 0}
    removed_delete_files = {path for path, row in entries_before.items() if row.content == 1}
    assert len(replaced_files) == 2 and removed_delete_files, entries_before

    # A concurrent Java writer plans a position delete against the files that OPTIMIZE then replaces.
    jvm = spark._jvm
    table = jvm.org.apache.iceberg.hadoop.HadoopTables(spark._jsc.hadoopConfiguration()).load(table_path)
    start_snapshot_id = table.currentSnapshot().snapshotId()
    staged_delete = (
        jvm.org.apache.iceberg.FileMetadata.deleteFileBuilder(table.spec())
        .ofPositionDeletes()
        .withPath(f"{table_path}/data/staged-position-delete.parquet")
        .withFormat("parquet")
        .withFileSizeInBytes(1)
        .withRecordCount(1)
        .build()
    )

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )
    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)
    instance.query(f"OPTIMIZE TABLE {TABLE_NAME}", settings={"allow_experimental_iceberg_compaction": 1})
    default_download_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"{table_path}/",
        f"{table_path}/",
    )

    table.refresh()
    assert table.currentSnapshot().operation() == "replace"
    replace_snapshot_id = table.currentSnapshot().snapshotId()
    expected_ids = [0] + list(range(2, 10))
    assert sorted(row.id for row in spark.read.format("iceberg").load(table_path).collect()) == expected_ids
    assert instance.query(f"SELECT groupArray(id) FROM (SELECT id FROM {TABLE_NAME} ORDER BY id)") == f"{expected_ids}\n".replace(" ", "")

    # The replaced data files and the applied delete files are DELETED by the compaction, with their sequence numbers kept.
    deleted = {row.file_path: row for row in read_entries() if row.status == 2}
    assert set(deleted) == replaced_files | removed_delete_files, deleted
    for path, row in deleted.items():
        assert row.snapshot_id == replace_snapshot_id
        assert row.content == entries_before[path].content
        assert row.sequence_number == entries_before[path].sequence_number
        assert row.file_sequence_number == entries_before[path].file_sequence_number
    manifests = spark.read.format("iceberg").load(f"{table_path}#manifests").collect()
    assert sum(m.deleted_data_files_count for m in manifests) == len(replaced_files)
    assert sum(m.deleted_delete_files_count for m in manifests) == len(removed_delete_files)

    # Committing the staged delete must fail: the files it references were replaced.
    data_files = jvm.java.util.ArrayList()
    for path in replaced_files:
        data_files.add(path)
    with pytest.raises(Exception, match="Cannot commit, missing data files"):
        (
            table.newRowDelta()
            .addDeletes(staged_delete)
            .validateFromSnapshot(start_snapshot_id)
            .validateDataFilesExist(data_files)
            .validateDeletedFiles()
            .commit()
        )
