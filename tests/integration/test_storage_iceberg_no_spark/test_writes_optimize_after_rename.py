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
def test_optimize_rejected_on_format_v3(
    started_cluster_iceberg_no_spark, storage_type
):
    """
    Compaction is fail-closed on format-version 3, matching the MANIFEST-only path: the writer
    does not round-trip the row-lineage `first_row_id`, so a rewrite would drop row ids.
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

    # Positional delete so compaction would otherwise have work to do.
    instance.query(
        f"DELETE FROM {TABLE_NAME} WHERE id = 2;",
        settings={"allow_insert_into_iceberg": 1},
    )
    error = instance.query_and_get_error(
        f"OPTIMIZE TABLE {TABLE_NAME};",
        settings={
            "allow_experimental_iceberg_compaction": 1,
            "allow_insert_into_iceberg": 1,
        },
    )
    assert "Compaction is supported only for format_version 2" in error, error

    # The table is left unchanged: the rejection happens before any file is rewritten.
    meta_after, _ = _read_latest_metadata(instance, TABLE_NAME)
    assert meta_after.get("next-row-id") == meta_before.get("next-row-id")
    assert (
        instance.query(f"SELECT id, value FROM {TABLE_NAME} ORDER BY id")
        == "1\ta\n3\tc\n4\td\n5\te\n"
    )


@pytest.mark.parametrize("storage_type", ["local"])
def test_optimize_clears_partition_statistics(
    started_cluster_iceberg_no_spark, storage_type
):
    """
    `clearOldFiles` removes the entire pre-compaction `metadata/` subtree, and
    SnapshotFilesTraversal treats both `statistics` and `partition-statistics` as reachable files.
    The deep-copy bootstrap therefore must clear `partition-statistics` alongside `statistics`,
    otherwise a non-empty `partition-statistics` array copied from Spark/Flink source metadata
    publishes a dangling `statistics-path` after OPTIMIZE.
    """
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    TABLE_NAME = "test_optimize_part_stats_" + storage_type + "_" + get_uuid_str()

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
    # Positional delete so compaction has work to do.
    instance.query(
        f"DELETE FROM {TABLE_NAME} WHERE id = 2;",
        settings={"allow_insert_into_iceberg": 1},
    )

    # Inject a partition-statistics entry pointing at a metadata-side file (as Spark/Flink emit).
    meta, prev_path = _read_latest_metadata(instance, TABLE_NAME)
    current_snapshot_id = meta["current-snapshot-id"]
    dangling_path = f"{_metadata_dir(TABLE_NAME)}/partition-stats-injected.parquet"
    meta["partition-statistics"] = [
        {
            "snapshot-id": current_snapshot_id,
            "statistics-path": dangling_path,
            "file-size-in-bytes": 123,
        }
    ]
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
    # The compacted metadata must not carry the injected partition-statistics entry (which would
    # point at a statistics-path under the deleted pre-compaction metadata subtree).
    assert not meta_after.get("partition-statistics"), (
        f"partition-statistics survived compaction: {meta_after.get('partition-statistics')}"
    )
    # Data still correct.
    assert (
        instance.query(f"SELECT id, value FROM {TABLE_NAME} ORDER BY id") == "1\ta\n3\tc\n"
    )


@pytest.mark.parametrize("storage_type", ["local"])
def test_optimize_rejected_after_decimal_precision_change(
    started_cluster_iceberg_no_spark, storage_type
):
    """
    The lossy-evolution guard must reject a parameterized-primitive type change (here a decimal
    precision widening) the same way it rejects a plain type change. Iceberg encodes decimal (and
    fixed) as a string primitive ("decimal(P, S)" / "fixed[N]"), so `walkTypeNode` stores the full
    parameterized string in the field-id -> type signature; a precision/scale change therefore
    yields a different signature and must be rejected. Compaction reuses the original snapshot ids
    for time travel, so letting a decimal change through would rewrite historical files into the
    new physical type and misread a pre-change snapshot.
    """
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    TABLE_NAME = "test_optimize_decimal_" + storage_type + "_" + get_uuid_str()

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
    # Positional delete so compaction has work to do (the guard only fires when need_optimize).
    instance.query(
        f"DELETE FROM {TABLE_NAME} WHERE id = 2;",
        settings={"allow_insert_into_iceberg": 1},
    )

    # Inject a decimal precision change into a fresh metadata version: the current schema has the
    # field as decimal(20, 2), while an OLD reachable schema (pinned by the first snapshot) has it
    # as decimal(10, 2). Reading that pre-change snapshot after a rewrite into decimal(20, 2) would
    # use the wrong physical type, so OPTIMIZE must be rejected.
    meta, prev_path = _read_latest_metadata(instance, TABLE_NAME)
    cur_id = meta["current-schema-id"]
    schemas_by_id = {s["schema-id"]: s for s in meta["schemas"]}
    cur_schema = schemas_by_id[cur_id]
    field_id = None
    for f in cur_schema["fields"]:
        if f["name"] == "value":
            f["type"] = "decimal(20, 2)"
            field_id = f["id"]
    assert field_id is not None
    old_schema = json.loads(json.dumps(cur_schema))
    old_schema["schema-id"] = max(schemas_by_id) + 1
    for f in old_schema["fields"]:
        if f["name"] == "value":
            f["type"] = "decimal(10, 2)"
    meta["schemas"].append(old_schema)
    # Make the first snapshot reachable via the old (decimal(10, 2)) schema id.
    meta["snapshots"][0]["schema-id"] = old_schema["schema-id"]

    new_version = int(prev_path.split("/v")[-1].split(".")[0]) + 1
    new_path = f"{_metadata_dir(TABLE_NAME)}/v{new_version}.metadata.json"
    _write_metadata(instance, meta, new_path)
    version_hint = f"{_metadata_dir(TABLE_NAME)}/version-hint.text"
    instance.exec_in_container(
        ["bash", "-c", f"test -f {version_hint} && echo -n {new_version} > {version_hint} || true"]
    )

    assert "NOT_IMPLEMENTED" in instance.query_and_get_error(
        f"OPTIMIZE TABLE {TABLE_NAME};",
        settings={
            "allow_experimental_iceberg_compaction": 1,
            "allow_insert_into_iceberg": 1,
        },
    )


@pytest.mark.parametrize("storage_type", ["local"])
def test_optimize_aborts_on_metadata_commit_conflict(
    started_cluster_iceberg_no_spark, storage_type
):
    """
    `writeMetadataFileAndVersionHint` returns false when the target `vN.metadata.json` already
    exists (a concurrent writer won the commit). Compaction must propagate that and skip
    `clearOldFiles`, otherwise it deletes the pre-compaction data/metadata while the table still
    points at the other writer's snapshot -> data loss. We simulate the race by pre-creating a
    valid next metadata version (the "winner") while keeping the version hint on the current one,
    so compaction reads the current metadata but its commit target already exists. OPTIMIZE must
    fail and the pre-compaction data files must survive.
    """
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    TABLE_NAME = "test_optimize_commit_conflict_" + storage_type + "_" + get_uuid_str()

    # iceberg_use_version_hint=1 so the version hint (not directory listing) selects the metadata
    # compaction reads, letting us keep it on vN while a valid vN+1 already exists on disk.
    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_no_spark,
        "(id Int32, value Nullable(String))",
        2,
        use_version_hint=True,
    )

    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES (1,'a'),(2,'b'),(3,'c');",
        settings={"allow_insert_into_iceberg": 1},
    )
    # Positional delete so compaction has work to do.
    instance.query(
        f"DELETE FROM {TABLE_NAME} WHERE id = 2;",
        settings={"allow_insert_into_iceberg": 1},
    )

    metadata_dir = _metadata_dir(TABLE_NAME)
    _, latest_path = _read_latest_metadata(instance, TABLE_NAME)
    n = int(latest_path.split("/v")[-1].split(".")[0])
    next_path = f"{metadata_dir}/v{n + 1}.metadata.json"
    data_dir = f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/data"

    def data_file_count():
        return int(
            instance.exec_in_container(
                ["bash", "-c", f"ls {data_dir}/*.parquet 2>/dev/null | wc -l"]
            ).strip()
        )

    before = data_file_count()
    # Concurrent winner: a valid vN+1 already committed. Keep the version hint on vN so compaction
    # plans from vN but its commit target vN+1 already exists.
    instance.exec_in_container(["bash", "-c", f"cp {latest_path} {next_path}"])
    instance.exec_in_container(
        ["bash", "-c", f"echo -n {n} > {metadata_dir}/version-hint.text"]
    )

    err = instance.query_and_get_error(
        f"OPTIMIZE TABLE {TABLE_NAME};",
        settings={
            "allow_experimental_iceberg_compaction": 1,
            "allow_insert_into_iceberg": 1,
        },
    )
    assert "CONCURRENT_ACCESS_NOT_SUPPORTED" in err, err
    # The pre-compaction data files must NOT have been deleted (clearOldFiles must be skipped).
    assert data_file_count() >= before, (
        f"pre-compaction data files were deleted after a lost commit: {data_file_count()} < {before}"
    )


@pytest.mark.parametrize("storage_type", ["local"])
def test_optimize_keeps_version_hint(started_cluster_iceberg_no_spark, storage_type):
    """
    `version-hint.text` is a fixed path rewritten in place, so it must survive compaction's
    cleanup of the previous metadata generation. If the pre-compaction listing is deleted after
    the commit, `iceberg_use_version_hint = 1` readers lose their discovery file and the table
    cannot be read at all.
    """
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    TABLE_NAME = "test_optimize_version_hint_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_no_spark,
        "(id Int32, value Nullable(String))",
        2,
        use_version_hint=True,
    )

    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES (1,'a'),(2,'b'),(3,'c');",
        settings={"allow_insert_into_iceberg": 1},
    )
    # Positional delete so compaction has work to do.
    instance.query(
        f"DELETE FROM {TABLE_NAME} WHERE id = 2;",
        settings={"allow_insert_into_iceberg": 1},
    )

    hint_path = f"{_metadata_dir(TABLE_NAME)}/version-hint.text"

    def hint_contents():
        return instance.exec_in_container(
            ["bash", "-c", f"cat {hint_path} 2>/dev/null || true"]
        ).strip()

    before = hint_contents()
    assert before, "version hint must exist before OPTIMIZE"

    instance.query(
        f"OPTIMIZE TABLE {TABLE_NAME};",
        settings={
            "allow_experimental_iceberg_compaction": 1,
            "allow_insert_into_iceberg": 1,
        },
    )

    after = hint_contents()
    assert after, f"version hint was deleted by compaction (was {before})"
    # It points at the compacted metadata, so it must have advanced past the pre-compaction one.
    assert int(after) > int(before), f"version hint did not advance: {before} -> {after}"
    # The table is still discoverable and returns the compacted contents.
    assert (
        instance.query(f"SELECT id, value FROM {TABLE_NAME} ORDER BY id") == "1\ta\n3\tc\n"
    )
