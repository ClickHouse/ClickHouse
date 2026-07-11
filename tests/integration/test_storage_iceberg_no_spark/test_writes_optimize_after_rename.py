import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    get_uuid_str,
)

# Force the writer to roll the data over into several files so that a single
# manifest ends up referencing more than one live data file. This exercises the
# multi-file-manifest path that Spark/Flink/Trino output routinely produces.
MULTI_FILE_INSERT_SETTINGS = {
    "allow_insert_into_iceberg": 1,
    "iceberg_insert_max_rows_in_data_file": 2,
    "min_insert_block_size_rows": 2,
    "max_insert_block_size": 2,
    "max_block_size": 2,
}


@pytest.mark.parametrize("storage_type", ["local", "s3"])
def test_optimize_after_rename_column(started_cluster_iceberg_no_spark, storage_type):
    """
    OPTIMIZE (compaction) must read data files written before an `ALTER ... RENAME COLUMN`
    by the file's own schema and remap the columns to the current schema. Otherwise the old
    file is read by the current-schema column names and the renamed column is compacted into
    DEFAULT/NULL values instead of the original data.

    The rows are written across several data files inside a single manifest (rollover), so this
    also covers the multi-file-manifest case: compaction must rewrite every live data file, not
    just the first one referenced by the manifest.

    A DELETE creates a positional delete file (merge-on-read) so compaction has work to do; the
    test then asserts the compaction side effect (the positional delete is gone from
    system.iceberg_files) so that a no-op OPTIMIZE cannot pass.
    """
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    TABLE_NAME = "test_optimize_after_rename_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_no_spark,
        "(id Int32, value Nullable(String))",
        2,
    )

    # Six rows rolled over into three data files inside one manifest.
    instance.query(
        f"INSERT INTO {TABLE_NAME} SELECT number + 1 AS id, char(97 + number) AS value FROM numbers(6);",
        settings=MULTI_FILE_INSERT_SETTINGS,
    )
    # DELETE one row -> produces a positional delete file (merge-on-read),
    # which makes compaction necessary.
    instance.query(
        f"DELETE FROM {TABLE_NAME} WHERE id = 3;",
        settings={"allow_insert_into_iceberg": 1},
    )

    # Schema evolution: the data files above now belong to an older schema id.
    instance.query(f"ALTER TABLE {TABLE_NAME} RENAME COLUMN value TO label;")

    assert (
        instance.query(f"SELECT id, label FROM {TABLE_NAME} ORDER BY id")
        == "1\ta\n2\tb\n4\td\n5\te\n6\tf\n"
    )

    # There must be exactly one positional delete file before compaction.
    assert (
        int(
            instance.query(
                f"SELECT countIf(content = 'POSITION_DELETE') FROM system.iceberg_files WHERE table = '{TABLE_NAME}'"
            )
        )
        == 1
    )

    instance.query(
        f"OPTIMIZE TABLE {TABLE_NAME};",
        settings={"allow_experimental_iceberg_compaction": 1, "allow_insert_into_iceberg": 1},
    )

    # Compaction side effect: the positional delete must be gone. This fails a no-op OPTIMIZE
    # or one that skips metadata regeneration.
    assert (
        int(
            instance.query(
                f"SELECT countIf(content = 'POSITION_DELETE') FROM system.iceberg_files WHERE table = '{TABLE_NAME}'"
            )
        )
        == 0
    )

    # After compaction every live data file must survive (no multi-file-manifest drop), the
    # renamed column must still carry the original values (not DEFAULT/NULL), and the deleted
    # row must stay deleted.
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 5
    assert (
        instance.query(f"SELECT id, label FROM {TABLE_NAME} ORDER BY id")
        == "1\ta\n2\tb\n4\td\n5\te\n6\tf\n"
    )
    assert (
        int(instance.query(f"SELECT count() FROM {TABLE_NAME} WHERE label IS NULL"))
        == 0
    )


@pytest.mark.parametrize("storage_type", ["local", "s3"])
def test_optimize_rejected_after_lossy_schema_evolution(
    started_cluster_iceberg_no_spark, storage_type
):
    """
    Compaction physically rewrites every historical data file into the CURRENT schema, while
    `writeMetadataFiles` keeps the original snapshot ids and their committed schema-ids so old
    snapshots stay reachable for time travel. After a lossy schema evolution (`DROP COLUMN`) the
    rewritten files no longer contain the dropped field, so time travel to a pre-drop snapshot
    would silently return NULL/defaults. OPTIMIZE must be rejected in that case (fail-closed).

    A rename (field id + type preserved) is not lossy and must still be compactable, which the
    companion test above covers.
    """
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    TABLE_NAME = "test_optimize_lossy_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_no_spark,
        "(id Int32, value Nullable(String), extra Nullable(String))",
        2,
    )

    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES (1,'a','x1'),(2,'b','x2'),(3,'c','x3');",
        settings={"allow_insert_into_iceberg": 1},
    )
    pre_drop_snapshot = int(
        instance.query(
            f"SELECT snapshot_id FROM system.iceberg_history WHERE table = '{TABLE_NAME}' ORDER BY made_current_at DESC LIMIT 1"
        )
    )
    # DELETE one row -> positional delete file so compaction has work to do.
    instance.query(
        f"DELETE FROM {TABLE_NAME} WHERE id = 2;",
        settings={"allow_insert_into_iceberg": 1},
    )
    # Lossy schema evolution: the field `extra` disappears from the current schema.
    instance.query(f"ALTER TABLE {TABLE_NAME} DROP COLUMN extra;")

    # OPTIMIZE must be rejected: rewriting historical files into the current schema would drop
    # `extra` and break time travel to the pre-drop snapshot.
    assert "NOT_IMPLEMENTED" in instance.query_and_get_error(
        f"OPTIMIZE TABLE {TABLE_NAME};",
        settings={
            "allow_experimental_iceberg_compaction": 1,
            "allow_insert_into_iceberg": 1,
        },
    )

    # The positional delete is still there (OPTIMIZE did not run).
    assert (
        int(
            instance.query(
                f"SELECT countIf(content = 'POSITION_DELETE') FROM system.iceberg_files WHERE table = '{TABLE_NAME}'"
            )
        )
        == 1
    )

    # Time travel to the pre-drop snapshot must still return the original `extra` values.
    assert (
        instance.query(
            f"SELECT id, value, extra FROM {TABLE_NAME} ORDER BY id SETTINGS iceberg_snapshot_id = {pre_drop_snapshot}"
        )
        == "1\ta\tx1\n2\tb\tx2\n3\tc\tx3\n"
    )
