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