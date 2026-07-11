import json

import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    get_uuid_str,
)


def _metadata_dir(table_name):
    return f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}/metadata"


def _read_latest_metadata(instance, table_name):
    metadata_dir = _metadata_dir(table_name)
    latest = instance.exec_in_container(
        ["bash", "-c", f"ls -v {metadata_dir}/v*.metadata.json | tail -1"]
    ).strip()
    return json.loads(instance.exec_in_container(["cat", latest])), latest


def _write_metadata(instance, meta, path):
    instance.exec_in_container(
        ["bash", "-c", f"cat > {path} << 'JSONEOF'\n{json.dumps(meta, indent=4)}\nJSONEOF"]
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


@pytest.mark.parametrize("storage_type", ["local", "s3"])
def test_optimize_noop_after_lossy_schema_evolution(
    started_cluster_iceberg_no_spark, storage_type
):
    """
    The lossy-evolution guard must only fire when compaction actually rewrites files. With no
    live positional deletes there is nothing to compact, so a no-op OPTIMIZE on a table with a
    historical `DROP COLUMN` must stay a no-op, not throw NOT_IMPLEMENTED (`need_optimize` is
    known only after the manifest scan). This mirrors the pre-existing behavior where an OPTIMIZE
    with nothing to do returned without touching metadata.
    """
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    TABLE_NAME = "test_optimize_noop_lossy_" + storage_type + "_" + get_uuid_str()

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
    # Lossy schema evolution, but NO delete -> nothing to compact.
    instance.query(f"ALTER TABLE {TABLE_NAME} DROP COLUMN extra;")

    # OPTIMIZE must not throw: there are no positional deletes, so compaction is a no-op and the
    # lossy-evolution guard (which protects time travel across a rewrite) must not trigger.
    instance.query(
        f"OPTIMIZE TABLE {TABLE_NAME};",
        settings={
            "allow_experimental_iceberg_compaction": 1,
            "allow_insert_into_iceberg": 1,
        },
    )

    # Data is unchanged.
    assert (
        instance.query(f"SELECT id, value FROM {TABLE_NAME} ORDER BY id")
        == "1\ta\n2\tb\n3\tc\n"
    )


@pytest.mark.parametrize("storage_type", ["local", "s3"])
def test_optimize_preserves_partition_spec(
    started_cluster_iceberg_no_spark, storage_type
):
    """
    Compaction must start the new metadata from the source table metadata, not from a synthetic
    `createEmptyMetadataFile` object. Otherwise a partitioned table loses its `partition-specs` /
    `default-spec-id` after OPTIMIZE: the next INSERT reads the emptied spec, `partitioner ==
    nullopt`, and writes unpartitioned data into a partitioned table. This test writes a
    partitioned table, forces compaction (positional delete), then INSERTs again and asserts the
    data files are still partitioned and the rows read back correctly.
    """
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    TABLE_NAME = "test_optimize_partition_spec_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_no_spark,
        "(part Int32, id Int32, value Nullable(String))",
        2,
        partition_by="part",
    )

    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES (0,1,'a'),(0,2,'b'),(1,3,'c'),(1,4,'d');",
        settings={"allow_insert_into_iceberg": 1},
    )
    # DELETE one row -> positional delete file so compaction has work to do.
    instance.query(
        f"DELETE FROM {TABLE_NAME} WHERE id = 2;",
        settings={"allow_insert_into_iceberg": 1},
    )

    instance.query(
        f"OPTIMIZE TABLE {TABLE_NAME};",
        settings={
            "allow_experimental_iceberg_compaction": 1,
            "allow_insert_into_iceberg": 1,
        },
    )

    # Compaction consumed the positional delete.
    assert (
        int(
            instance.query(
                f"SELECT countIf(content = 'POSITION_DELETE') FROM system.iceberg_files WHERE table = '{TABLE_NAME}'"
            )
        )
        == 0
    )
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 3

    # A fresh INSERT after compaction must still be partitioned: the partition spec was
    # preserved, so each partition value lands in its own data file. Before the fix the spec was
    # empty and the new rows went into a single unpartitioned file.
    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES (0,5,'e'),(1,6,'f');",
        settings={"allow_insert_into_iceberg": 1},
    )

    # Partition pruning must return exactly the live rows of a single partition. If the metadata
    # had lost its partition spec, the post-OPTIMIZE writes would be unpartitioned and pruning by
    # `part` would drop live rows (or scan everything).
    assert (
        instance.query(f"SELECT id, value FROM {TABLE_NAME} WHERE part = 0 ORDER BY id")
        == "1\ta\n5\te\n"
    )
    assert (
        instance.query(f"SELECT id, value FROM {TABLE_NAME} WHERE part = 1 ORDER BY id")
        == "3\tc\n4\td\n6\tf\n"
    )
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 5


@pytest.mark.parametrize("storage_type", ["local"])
def test_optimize_prunes_refs_to_delete_only_snapshot(
    started_cluster_iceberg_no_spark, storage_type
):
    """
    Compaction preserves table-level `refs`, but the replay regenerates only append-like
    snapshots (`tryGetAppendUpdate` returns nullopt for DELETE / position-delete-only OVERWRITE,
    which are skipped). A tag or branch pinned to a skipped snapshot id would survive in `refs`
    while its target is absent from the rebuilt `snapshots` list, leaving metadata that readers
    resolving refs cannot follow. Such refs must be pruned; `main` is re-pointed to the last
    replayed snapshot and must stay valid.
    """
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    TABLE_NAME = "test_optimize_refs_prune_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_no_spark,
        "(id Int32, value Nullable(String))",
        2,
    )

    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES (1,'a'),(2,'b'),(3,'c');",
        settings={"allow_insert_into_iceberg": 1},
    )
    # Positional delete -> a delete-only OVERWRITE snapshot that compaction will NOT replay.
    instance.query(
        f"DELETE FROM {TABLE_NAME} WHERE id = 2;",
        settings={"allow_insert_into_iceberg": 1},
    )

    # Inject a tag ref pinned to that delete-only snapshot into a fresh metadata version.
    meta, prev_path = _read_latest_metadata(instance, TABLE_NAME)
    delete_snapshot_id = None
    for snapshot in meta["snapshots"]:
        if snapshot.get("summary", {}).get("operation") in ("overwrite", "delete"):
            delete_snapshot_id = snapshot["snapshot-id"]
    assert delete_snapshot_id is not None, "expected a delete-only snapshot"
    meta.setdefault("refs", {})["archive"] = {
        "snapshot-id": delete_snapshot_id,
        "type": "tag",
    }
    new_version = int(prev_path.split("/v")[-1].split(".")[0]) + 1
    new_path = f"{_metadata_dir(TABLE_NAME)}/v{new_version}.metadata.json"
    _write_metadata(instance, meta, new_path)
    version_hint = f"{_metadata_dir(TABLE_NAME)}/version-hint.text"
    instance.exec_in_container(
        ["bash", "-c", f"test -f {version_hint} && echo -n {new_version} > {version_hint} || true"]
    )

    instance.query(
        f"OPTIMIZE TABLE {TABLE_NAME};",
        settings={
            "allow_experimental_iceberg_compaction": 1,
            "allow_insert_into_iceberg": 1,
        },
    )

    meta_after, _ = _read_latest_metadata(instance, TABLE_NAME)
    regenerated_ids = {s["snapshot-id"] for s in meta_after["snapshots"]}
    refs = meta_after.get("refs", {})
    # The tag pinned to the skipped delete-only snapshot must be gone.
    assert "archive" not in refs, f"stale ref survived: {refs}"
    # Every surviving ref must point at a regenerated snapshot (no dangling refs).
    for name, ref in refs.items():
        assert (
            ref["snapshot-id"] in regenerated_ids
        ), f"ref '{name}' points at non-regenerated snapshot {ref['snapshot-id']}"
    # Data still correct after compaction.
    assert (
        instance.query(f"SELECT id, value FROM {TABLE_NAME} ORDER BY id") == "1\ta\n3\tc\n"
    )


@pytest.mark.parametrize("storage_type", ["local"])
def test_optimize_resets_next_row_id_format_v3(
    started_cluster_iceberg_no_spark, storage_type
):
    """
    Format-version 3 row lineage: `MetadataGenerator::generateNextMetadata` uses the table-level
    `next-row-id` as the starting `first-row-id` for each replayed snapshot and increments it by
    that snapshot's added rows. Compaction rebuilds the whole snapshot history from scratch, so
    the deep-copied (already-advanced) `next-row-id` must be reset first. Otherwise the first
    regenerated snapshot starts at the old table tail and the final `next-row-id` becomes the old
    value plus the replayed history (inflated row lineage).
    """
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    TABLE_NAME = "test_optimize_next_row_id_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_no_spark,
        "(id Int32, value Nullable(String))",
        3,
    )

    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES (1,'a'),(2,'b'),(3,'c');",
        settings={"allow_insert_into_iceberg": 1},
    )
    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES (4,'d'),(5,'e');",
        settings={"allow_insert_into_iceberg": 1},
    )

    meta_before, _ = _read_latest_metadata(instance, TABLE_NAME)
    # 5 rows appended across two snapshots -> next-row-id advanced to 5.
    assert meta_before.get("next-row-id") == 5

    # Positional delete so compaction has work to do.
    instance.query(
        f"DELETE FROM {TABLE_NAME} WHERE id = 2;",
        settings={"allow_insert_into_iceberg": 1},
    )
    instance.query(
        f"OPTIMIZE TABLE {TABLE_NAME};",
        settings={
            "allow_experimental_iceberg_compaction": 1,
            "allow_insert_into_iceberg": 1,
        },
    )

    meta_after, _ = _read_latest_metadata(instance, TABLE_NAME)
    # Row lineage must be replayed from 0: the first regenerated snapshot starts at first-row-id 0,
    # and next-row-id equals the sum of the replayed added-rows (self-consistent from 0). Before the
    # fix, the preserved next-row-id (>= 5) was reused as the starting first-row-id, so the first
    # snapshot started at the old table tail and next-row-id ended at old_value + replayed_history.
    replay_snapshots = [
        s for s in meta_after["snapshots"] if s.get("first-row-id") is not None
    ]
    assert replay_snapshots, "format-v3 snapshots must carry first-row-id"
    first_row_ids = [s["first-row-id"] for s in replay_snapshots]
    assert min(first_row_ids) == 0, f"row lineage did not restart at 0: {first_row_ids}"
    replayed_total = sum(s.get("added-rows", 0) for s in replay_snapshots)
    assert (
        meta_after.get("next-row-id") == replayed_total
    ), (
        f"next-row-id inflated after compaction: next-row-id="
        f"{meta_after.get('next-row-id')} vs replayed_total={replayed_total} "
        f"(pre-fix it would be old next-row-id + replayed_total)"
    )
    assert (
        instance.query(f"SELECT id, value FROM {TABLE_NAME} ORDER BY id")
        == "1\ta\n3\tc\n4\td\n5\te\n"
    )
