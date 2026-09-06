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


@pytest.mark.parametrize("storage_type", ["local", "s3"])
def test_optimize_after_nested_rename(started_cluster_iceberg_with_spark, storage_type):
    """
    Compaction rejects genuinely lossy schema evolution (DROP COLUMN, incompatible type change),
    but a nested rename/reorder/add inside a struct/list/map is NOT lossy: the field ids and leaf
    types are preserved and `getSchemaTransformationDagByIds` can still remap old files. The
    lossy-evolution guard must compare schemas semantically (by field id, recursing into complex
    types), not by the textual JSON of the enclosing type (which embeds child names/ordering).

    Spark is used because ClickHouse's own Iceberg writer cannot express a nested-field rename.
    A positional delete makes compaction necessary; OPTIMIZE must succeed (not be rejected as
    lossy), the live rows must survive with correct values under the current schema, and time
    travel to the pre-rename snapshot must still work.

    Note: the struct is read as a whole (`SELECT s`), not by the renamed leaf (`s.b`). Iceberg
    subcolumn pushdown resolves the child by its on-disk name, so reading a renamed nested field
    directly is a pre-existing normal-read-path limitation, independent of compaction.
    """
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_optimize_nested_rename_" + storage_type + "_" + get_uuid_str()

    def upload():
        default_upload_directory(
            started_cluster_iceberg_with_spark,
            storage_type,
            f"/iceberg_data/default/{TABLE_NAME}/",
            f"/iceberg_data/default/{TABLE_NAME}/",
        )

    spark.sql(
        f"""
        CREATE TABLE {TABLE_NAME} (id long, s struct<a: string, n: int>) USING iceberg
        TBLPROPERTIES ('format-version' = '2', 'write.update.mode' = 'merge-on-read',
        'write.delete.mode' = 'merge-on-read', 'write.merge.mode' = 'merge-on-read')
        """
    )
    spark.sql(
        f"INSERT INTO {TABLE_NAME} VALUES (1, named_struct('a', 'x', 'n', 10)), "
        f"(2, named_struct('a', 'y', 'n', 20)), (3, named_struct('a', 'z', 'n', 30))"
    )
    upload()

    create_iceberg_table(
        storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark
    )
    pre_rename_snapshot = get_last_snapshot(
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/"
    )

    # Positional delete (merge-on-read) so compaction has work to do.
    spark.sql(f"DELETE FROM {TABLE_NAME} WHERE id = 2")
    upload()
    # Nested rename: struct field s.a -> s.b (field id and type preserved, only the name changes).
    # Spark's Iceberg RENAME COLUMN target is the new LEAF name only (not the dotted path), so the
    # field s.a becomes s.b via `... TO b`.
    spark.sql(f"ALTER TABLE {TABLE_NAME} RENAME COLUMN s.a TO b")
    upload()

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 2

    # OPTIMIZE must SUCCEED: the nested rename is not lossy, so the semantic guard allows it.
    # (The over-broad textual guard rejected this with NOT_IMPLEMENTED.)
    instance.query(
        f"OPTIMIZE TABLE {TABLE_NAME};",
        settings={
            "allow_experimental_iceberg_compaction": 1,
            "allow_insert_into_iceberg": 1,
        },
    )

    # All live rows survive with their original values under the current (renamed) schema. Read
    # the whole struct `s` rather than the renamed leaf `s.b`: Iceberg subcolumn pushdown extracts
    # the child by its on-disk name, which is a pre-existing normal-read-path limitation for
    # renamed nested fields and is independent of compaction (it fails the same way before OPTIMIZE).
    # The whole-struct read is remapped to the current schema, so it proves the values are intact.
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 2
    assert (
        instance.query(f"SELECT id, s FROM {TABLE_NAME} ORDER BY id")
        == "1\t('x',10)\n3\t('z',30)\n"
    )

    # Time travel to the pre-rename snapshot still works after compaction. Compaction physically
    # rewrites the data files and applies the positional delete, so (as the sibling `test_optimize`
    # also asserts) an old snapshot reads the compacted, delete-applied files: id=2 is gone. The
    # surviving rows keep their original values, remapped from the pre-rename schema.
    assert (
        instance.query(
            f"SELECT id, s FROM {TABLE_NAME} ORDER BY id SETTINGS iceberg_snapshot_id = {pre_rename_snapshot}"
        )
        == "1\t('x',10)\n3\t('z',30)\n"
    )


def test_optimize_multi_partition_manifest(started_cluster_iceberg_with_spark):
    """
    A single Spark manifest can pack data files from several partitions (one INSERT that
    writes multiple partitions groups them into one manifest). Compaction keys partition
    values and statistics per rewritten DATA FILE, not per manifest: otherwise every output
    entry in the manifest would inherit one partition tuple (partition pruning skips live
    files -> wrong results) and one unioned bounds tuple (predicate pruning regresses).

    The table is partitioned by identity(part) with disjoint id ranges per partition, so a
    per-manifest aggregation would be observable. After OPTIMIZE every partition's rows must
    survive with correct values (incl. partition-filtered reads), each data-file manifest
    entry must carry its own partition value, and the two entries' bounds must NOT be unioned.

    storage_type is "local": the manifest inspection below reads the regenerated Avro from
    the node's local user_files (as the sibling per-file-stats test does).
    """
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    storage_type = "local"
    TABLE_NAME = "test_optimize_multi_part_" + storage_type + "_" + get_uuid_str()

    def upload():
        default_upload_directory(
            started_cluster_iceberg_with_spark,
            storage_type,
            f"/iceberg_data/default/{TABLE_NAME}/",
            f"/iceberg_data/default/{TABLE_NAME}/",
        )

    spark.sql(
        f"""
        CREATE TABLE {TABLE_NAME} (part int, id long) USING iceberg PARTITIONED BY (identity(part))
        TBLPROPERTIES ('format-version' = '2', 'write.update.mode' = 'merge-on-read',
        'write.delete.mode' = 'merge-on-read', 'write.merge.mode' = 'merge-on-read')
        """
    )
    # One INSERT writing two disjoint partitions -> one manifest listing two data files,
    # one per partition, with disjoint id ranges (part 0: 10..49, part 1: 100..149).
    spark.sql(
        f"INSERT INTO {TABLE_NAME} SELECT 0, id FROM range(10, 50) "
        f"UNION ALL SELECT 1, id FROM range(100, 150)"
    )
    upload()

    create_iceberg_table(
        storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark
    )

    # Positional delete (merge-on-read) in one partition so compaction has work to do.
    spark.sql(f"DELETE FROM {TABLE_NAME} WHERE part = 0 AND id < 20")
    upload()

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 80

    instance.query(
        f"OPTIMIZE TABLE {TABLE_NAME};",
        settings={"allow_experimental_iceberg_compaction": 1},
    )

    # Correctness after compaction, including partition-filtered reads. If partition values were
    # stamped per manifest (all entries -> last-visited partition), pruning would drop live files.
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 80
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME} WHERE part = 0")) == 30
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME} WHERE part = 1")) == 50
    assert instance.query(f"SELECT id FROM {TABLE_NAME} WHERE part = 0 ORDER BY id") == instance.query(
        "SELECT number FROM numbers(20, 30)"
    )
    assert instance.query(f"SELECT id FROM {TABLE_NAME} WHERE part = 1 ORDER BY id") == instance.query(
        "SELECT number FROM numbers(100, 50)"
    )

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

    # A data-file manifest entry exposes its own partition value and its own bounds. Collect,
    # per manifest, the (partition, lower_bounds) of every DATA entry. Per-manifest aggregation
    # would make every entry in the same manifest identical; per-file accounting keeps them
    # distinct across the two partitions.
    saw_multi_partition_manifest = False
    for manifest in manifest_files:
        rows = instance.query(
            f"""
            SELECT
                tupleElement(tupleElement(data_file, 'partition'), 'part') AS part_val,
                tupleElement(data_file, 'lower_bounds')                    AS lower_bounds
            FROM file('{manifest}', Avro)
            WHERE tupleElement(data_file, 'content') = 0
            ORDER BY part_val
            FORMAT TSV
            """
        ).strip()
        if not rows:
            continue
        lines = rows.splitlines()
        if len(lines) < 2:
            continue
        saw_multi_partition_manifest = True
        parts = [line.split("\t")[0] for line in lines]
        lower_bounds = [line.split("\t")[1] for line in lines]
        # Comment 1: each rewritten file keeps its own partition value (0 and 1), not one shared.
        assert parts == ["0", "1"], f"partition values not per-file: {parts}"
        # Comment 2: bounds are per-file, not unioned across every file in the manifest.
        assert lower_bounds[0] != lower_bounds[1], "bounds were unioned across files"

    assert saw_multi_partition_manifest, "expected a manifest packing both partitions"