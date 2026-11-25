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
    parse_manifest_entry
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


def test_optimize_mixed_format_position_delete_reference_bounds(
    started_cluster_iceberg_with_spark,
):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    storage_type = "local"
    TABLE_NAME = "test_optimize_mixed_format_bounds_" + get_uuid_str()

    # An ORC data file whose sequence number is below a position delete that references only the
    # Parquet data file. Every write is Spark's: ClickHouse refuses its own DELETE while a
    # non-Parquet data file is live in the current snapshot.
    spark.sql(
        f"""
        CREATE TABLE {TABLE_NAME} (id long, data string) USING iceberg TBLPROPERTIES (
            'format-version' = '2',
            'write.update.mode' = 'merge-on-read',
            'write.delete.mode' = 'merge-on-read',
            'write.merge.mode' = 'merge-on-read',
            'write.format.default' = 'orc',
            'write.delete.format.default' = 'parquet'
        )
        """
    )
    spark.sql(f"INSERT INTO {TABLE_NAME} select id, char(id + ascii('a')) from range(0, 10)")
    spark.sql(f"ALTER TABLE {TABLE_NAME} SET TBLPROPERTIES ('write.format.default' = 'parquet')")
    spark.sql(f"INSERT INTO {TABLE_NAME} select id, char(id + ascii('a')) from range(10, 20)")
    spark.sql(f"DELETE FROM {TABLE_NAME} WHERE id = 15")

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )
    create_iceberg_table(
        storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark
    )

    # Arming condition: the current snapshot mixes one ORC and one Parquet data file, and the ORC
    # one is not newer than the position delete. Without both, compaction never reaches the
    # data-file format restriction and this test would pass whatever the planning pass decides.
    files = [
        line.split("\t")
        for line in instance.query(
            f"""
            SELECT content, file_format, sequence_number, file_path FROM system.iceberg_files
            WHERE database = currentDatabase() AND table = '{TABLE_NAME}' FORMAT TSV
            """
        )
        .strip()
        .splitlines()
    ]
    orc = [f for f in files if f[0] == "DATA" and f[1].upper() == "ORC"]
    parquet = [f for f in files if f[0] == "DATA" and f[1].upper() == "PARQUET"]
    deletes = [f for f in files if f[0] == "POSITION_DELETE"]
    assert (
        len(orc) == len(parquet) == len(deletes) == 1
    ), f"not the mixed-format carrier: {files}"
    assert int(orc[0][2]) <= int(
        deletes[0][2]
    ), f"the ORC data file is newer than the position delete: {files}"

    # Arming condition: the position delete records reference-data-file bounds that exclude the
    # ORC data file. With no bounds recorded, a read of this snapshot throws too, and compaction
    # is then right to refuse it.
    query_id = TABLE_NAME + "_manifest_entries"
    instance.query(
        f"SELECT count() FROM {TABLE_NAME} SETTINGS iceberg_metadata_log_level = 'manifest_file_entry'",
        query_id=query_id,
    )
    instance.query("SYSTEM FLUSH LOGS")
    entries = [
        parse_manifest_entry(json.loads(line))
        for line in instance.query(
            f"""
            SELECT DISTINCT content FROM system.iceberg_metadata_log
            WHERE content != '' AND content IS NOT NULL
              AND content_type = 'ManifestFileEntry' AND query_id = '{query_id}'
            """
        )
        .strip()
        .splitlines()
        if line
    ]
    delete_entries = [e for e in entries if e.content_type == 1]
    assert len(delete_entries) == 1, f"expected one position delete manifest entry, got {entries}"
    lower, upper = delete_entries[0].lower_bound, delete_entries[0].upper_bound
    orc_path, parquet_path = orc[0][3], parquet[0][3]
    assert (
        lower is not None and upper is not None
    ), f"the position delete records no reference-data-file bounds: {delete_entries[0].file_path}"
    # The Parquet file being inside the bounds is what makes the ORC file being outside them a
    # real exclusion rather than a difference in how the two sources spell the same path.
    assert lower <= parquet_path <= upper, f"bounds ({lower}, {upper}) miss {parquet_path}"
    assert not lower <= orc_path <= upper, f"bounds ({lower}, {upper}) cover {orc_path}"

    expected_ids = instance.query("SELECT number FROM numbers(20) WHERE number != 15")
    assert instance.query(f"SELECT id FROM {TABLE_NAME} ORDER BY id") == expected_ids

    instance.query(
        f"OPTIMIZE TABLE {TABLE_NAME};",
        settings={"allow_experimental_iceberg_compaction": 1},
    )

    assert instance.query(f"SELECT id FROM {TABLE_NAME} ORDER BY id") == expected_ids
    assert (
        int(
            instance.query(
                f"""
                SELECT count() FROM system.iceberg_files
                WHERE database = currentDatabase() AND table = '{TABLE_NAME}'
                  AND content = 'POSITION_DELETE'
                """
            )
        )
        == 0
    )


@pytest.mark.parametrize("delete_format", ["orc", "avro"])
def test_optimize_refuses_orc_avro_position_deletes(started_cluster_iceberg_with_spark, delete_format):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    storage_type = "local"
    TABLE_NAME = f"test_optimize_{delete_format}_position_delete_" + get_uuid_str()

    # A Parquet data file with an ORC or Avro position delete, and a newer ORC data file that the delete does not apply to.
    # After the rename, compaction would rewrite the ORC data file by column name and lose its `renamed_data` values.
    spark.sql(
        f"""
        CREATE TABLE {TABLE_NAME} (id long, data string) USING iceberg TBLPROPERTIES (
            'format-version' = '2',
            'write.delete.mode' = 'merge-on-read',
            'write.format.default' = 'parquet',
            'write.delete.format.default' = '{delete_format}'
        )
        """
    )
    spark.sql(f"INSERT INTO {TABLE_NAME} select id, char(id + ascii('a')) from range(0, 10)")
    spark.sql(f"DELETE FROM {TABLE_NAME} WHERE id = 5")
    spark.sql(f"ALTER TABLE {TABLE_NAME} SET TBLPROPERTIES ('write.format.default' = 'orc')")
    spark.sql(f"INSERT INTO {TABLE_NAME} select id, char(id + ascii('a')) from range(10, 20)")
    spark.sql(f"ALTER TABLE {TABLE_NAME} RENAME COLUMN data TO renamed_data")

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )
    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)

    files = instance.query(
        f"""
        SELECT content, lower(file_format) FROM system.iceberg_files
        WHERE database = currentDatabase() AND table = '{TABLE_NAME}' ORDER BY ALL FORMAT TSV
        """
    )
    assert files == f"DATA\torc\nDATA\tparquet\nPOSITION_DELETE\t{delete_format}\n", files

    expected = instance.query("SELECT number, char(number + 97) FROM numbers(20) WHERE number != 5")
    assert instance.query(f"SELECT id, renamed_data FROM {TABLE_NAME} ORDER BY id") == expected

    error = instance.query_and_get_error(
        f"OPTIMIZE TABLE {TABLE_NAME}", settings={"allow_experimental_iceberg_compaction": 1}
    )
    assert "NOT_IMPLEMENTED" in error, error
    assert instance.query(f"SELECT id, renamed_data FROM {TABLE_NAME} ORDER BY id") == expected


def get_latest_metadata(instance, table_path):
    latest = instance.exec_in_container(
        ["bash", "-c", f"ls -v {table_path}/metadata/v*.metadata.json | tail -1"]
    ).strip()
    return json.loads(instance.exec_in_container(["cat", latest]))


def test_optimize_position_delete_other_partition_spec(started_cluster_iceberg_with_spark):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_optimize_delete_other_spec_" + get_uuid_str()
    table_path = f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}"

    # OPTIMIZE must not apply a position delete to a data file of another partition spec. An ORC
    # data file under `identity(region)` and a position delete under `truncate(1, region)` share the
    # partition tuple, and `write.data.path` places the ORC file between the two Parquet files the
    # delete references, so only the partition spec tells the ORC file apart.
    spark.sql(
        f"""
        CREATE TABLE {TABLE_NAME} (id long, region string) USING iceberg PARTITIONED BY (region)
        TBLPROPERTIES (
            'format-version' = '2',
            'write.delete.mode' = 'merge-on-read',
            'write.delete.granularity' = 'partition',
            'write.format.default' = 'orc',
            'write.data.path' = '{table_path}/data_b'
        )
        """
    )
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (0, 'a')")
    spark.sql(f"ALTER TABLE {TABLE_NAME} REPLACE PARTITION FIELD region WITH truncate(1, region)")
    for data_dir, rows in [("data_a", "(1, 'ab'), (3, 'ab')"), ("data_c", "(2, 'ac'), (4, 'ac')")]:
        spark.sql(
            f"ALTER TABLE {TABLE_NAME} SET TBLPROPERTIES "
            f"('write.format.default' = 'parquet', 'write.data.path' = '{table_path}/{data_dir}')"
        )
        spark.sql(f"INSERT INTO {TABLE_NAME} VALUES {rows}")
    spark.sql(f"DELETE FROM {TABLE_NAME} WHERE id IN (1, 2)")

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        "local",
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )
    create_iceberg_table("local", instance, TABLE_NAME, started_cluster_iceberg_with_spark)

    # Arming condition: one partition tuple across both specs, and the ORC file sorting between
    # the two Parquet files whose deleted rows the single position delete records.
    files = [
        line.split("\t")
        for line in instance.query(
            f"""
            SELECT content, file_format, partition, file_path FROM system.iceberg_files
            WHERE database = currentDatabase() AND table = '{TABLE_NAME}' FORMAT TSV
            """
        )
        .strip()
        .splitlines()
    ]
    orc = [f[3] for f in files if f[0] == "DATA" and f[1].upper() == "ORC"]
    parquet = sorted(f[3] for f in files if f[0] == "DATA" and f[1].upper() == "PARQUET")
    deletes = [f for f in files if f[0] == "POSITION_DELETE"]
    assert len(orc) == 1 and len(parquet) == 2 and len(deletes) == 1, files
    assert len({f[2] for f in files}) == 1, files
    assert parquet[0] < orc[0] < parquet[1], files

    # OPTIMIZE must keep the partition keys; the default spec is not spec 0, which regenerated metadata gets.
    metadata_before = get_latest_metadata(instance, table_path)
    assert metadata_before["default-spec-id"] != 0, metadata_before["partition-specs"]

    expected_ids = "0\n3\n4\n"
    assert instance.query(f"SELECT id FROM {TABLE_NAME} ORDER BY id") == expected_ids

    instance.query(
        f"OPTIMIZE TABLE {TABLE_NAME};",
        settings={"allow_experimental_iceberg_compaction": 1},
    )

    assert instance.query(f"SELECT id FROM {TABLE_NAME} ORDER BY id") == expected_ids
    assert "POSITION_DELETE" not in instance.query(
        f"SELECT content FROM system.iceberg_files WHERE database = currentDatabase() AND table = '{TABLE_NAME}'"
    )

    metadata_after = get_latest_metadata(instance, table_path)
    changed = {
        key: (metadata_before[key], metadata_after.get(key))
        for key in ("partition-specs", "default-spec-id", "last-partition-id")
        if metadata_after.get(key) != metadata_before[key]
    }
    assert not changed, changed


def test_optimize_keeps_partition_tuple_of_another_arity(started_cluster_iceberg_with_spark):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_optimize_spec_arity_" + get_uuid_str()
    table_path = f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}"

    # A data file written before DROP PARTITION FIELD has a partition tuple of another arity than the
    # default spec. OPTIMIZE rewrites it under its own partition spec, keeping its tuple.
    spark.sql(
        f"""
        CREATE TABLE {TABLE_NAME} (id long, region string) USING iceberg PARTITIONED BY (region)
        TBLPROPERTIES ('format-version' = '2', 'write.delete.mode' = 'merge-on-read')
        """
    )
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (1, 'a'), (2, 'a'), (3, 'b')")
    spark.sql(f"ALTER TABLE {TABLE_NAME} DROP PARTITION FIELD region")
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (4, 'c')")
    spark.sql(f"DELETE FROM {TABLE_NAME} WHERE id = 2")

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        "local",
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )
    create_iceberg_table("local", instance, TABLE_NAME, started_cluster_iceberg_with_spark)

    def data_file_partitions():
        return sorted(
            instance.query(
                f"""
                SELECT partition FROM system.iceberg_files
                WHERE database = currentDatabase() AND table = '{TABLE_NAME}' AND content = 'DATA' FORMAT TSVRaw
                """
            )
            .strip()
            .splitlines()
        )

    # Arming condition: the default spec has no fields, while the old data files carry a one-value tuple.
    metadata_before = get_latest_metadata(instance, table_path)
    default_spec = [s for s in metadata_before["partition-specs"] if s["spec-id"] == metadata_before["default-spec-id"]]
    assert len(default_spec) == 1 and default_spec[0]["fields"] == [], metadata_before["partition-specs"]
    assert data_file_partitions() == ["{'a'}", "{'b'}", "{}"]

    expected_ids = "1\n3\n4\n"
    assert instance.query(f"SELECT id FROM {TABLE_NAME} ORDER BY id") == expected_ids

    instance.query(
        f"OPTIMIZE TABLE {TABLE_NAME};",
        settings={"allow_experimental_iceberg_compaction": 1},
    )

    assert instance.query(f"SELECT id FROM {TABLE_NAME} ORDER BY id") == expected_ids
    assert "POSITION_DELETE" not in instance.query(
        f"SELECT content FROM system.iceberg_files WHERE database = currentDatabase() AND table = '{TABLE_NAME}'"
    )
    assert data_file_partitions() == ["{'a'}", "{'b'}", "{}"]
    metadata_after = get_latest_metadata(instance, table_path)
    changed = {
        key: (metadata_before[key], metadata_after.get(key))
        for key in ("partition-specs", "default-spec-id", "last-partition-id")
        if metadata_after.get(key) != metadata_before[key]
    }
    assert not changed, changed

    # Spark reads the rewritten file under its old partition spec.
    default_download_directory(
        started_cluster_iceberg_with_spark,
        "local",
        f"{table_path}/",
        f"{table_path}/",
    )
    data_files = (
        spark.read.format("iceberg")
        .load(f"{table_path}#files")
        .where("content = 0")
        .selectExpr("spec_id", "coalesce(partition.region, '') AS region")
        .collect()
    )
    assert sorted((row.spec_id, row.region) for row in data_files) == [(0, "a"), (0, "b"), (1, "")], data_files
    assert sorted(row.id for row in spark.read.format("iceberg").load(table_path).collect()) == [1, 3, 4]


def test_optimize_keeps_field_ids_of_reordered_columns(started_cluster_iceberg_with_spark):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_optimize_reordered_columns_" + get_uuid_str()

    # After `ALTER COLUMN region FIRST` the field ids are not 1..N in column order, and the partition spec refers to `region` by its id.
    spark.sql(
        f"""
        CREATE TABLE {TABLE_NAME} (id long, region string) USING iceberg PARTITIONED BY (region)
        TBLPROPERTIES ('format-version' = '2', 'write.delete.mode' = 'merge-on-read')
        """
    )
    spark.sql(f"ALTER TABLE {TABLE_NAME} ALTER COLUMN region FIRST")
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES ('a', 1), ('a', 2), ('b', 3)")
    spark.sql(f"DELETE FROM {TABLE_NAME} WHERE id = 2")

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        "local",
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )
    create_iceberg_table("local", instance, TABLE_NAME, started_cluster_iceberg_with_spark)

    count_position_deletes = (
        f"SELECT count() FROM system.iceberg_files "
        f"WHERE database = currentDatabase() AND table = '{TABLE_NAME}' AND content = 'POSITION_DELETE'"
    )
    assert int(instance.query(count_position_deletes)) > 0
    instance.query(f"OPTIMIZE TABLE {TABLE_NAME};", settings={"allow_experimental_iceberg_compaction": 1})
    assert int(instance.query(count_position_deletes)) == 0
    write_settings = {"allow_insert_into_iceberg": 1, "mutations_sync": 1}
    instance.query(f"INSERT INTO {TABLE_NAME} VALUES ('c', 4)", settings=write_settings)
    instance.query(f"ALTER TABLE {TABLE_NAME} DELETE WHERE id = 3", settings=write_settings)

    assert instance.query(f"SELECT region, id FROM {TABLE_NAME} ORDER BY id") == "a\t1\nc\t4\n"
    data_file_partitions = instance.query(
        f"""
        SELECT partition FROM system.iceberg_files
        WHERE database = currentDatabase() AND table = '{TABLE_NAME}' AND content = 'DATA' ORDER BY partition FORMAT TSVRaw
        """
    )
    assert data_file_partitions == "{'a'}\n{'b'}\n{'c'}\n", data_file_partitions
