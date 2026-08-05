import json
import pathlib

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


def _read_live_metadata(instance, table_name):
    """Read the live metadata JSON via `system.iceberg_metadata_log`.

    Unlike `_read_latest_metadata` this does not assume the metadata sits on the node's
    local filesystem, so it works for object-store tables too.
    """
    query_id = f"{table_name}_meta_{get_uuid_str()}"
    instance.query(
        f"SELECT count() FROM {table_name}",
        query_id=query_id,
        settings={"iceberg_metadata_log_level": "metadata"},
    )
    instance.query("SYSTEM FLUSH LOGS")
    content = instance.query(
        f"SELECT DISTINCT content FROM system.iceberg_metadata_log "
        f"WHERE query_id = '{query_id}' AND content_type = 'Metadata' FORMAT TSVRaw"
    )
    # The logged JSON is single-line, and one SELECT resolves one metadata version, so
    # anything other than exactly one row means the read is ambiguous.
    versions = [line for line in content.split("\n") if line]
    assert len(versions) == 1, (
        f"expected exactly one logged metadata version for {table_name}, got {len(versions)}"
    )
    return json.loads(versions[0])

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
    # `extra` and break time travel to the pre-drop snapshot. Name the guard's own wording, since
    # several unrelated refusals also raise NOT_IMPLEMENTED.
    err = instance.query_and_get_error(
        f"OPTIMIZE TABLE {TABLE_NAME};",
        settings={
            "allow_experimental_iceberg_compaction": 1,
            "allow_insert_into_iceberg": 1,
        },
    )
    assert "NOT_IMPLEMENTED" in err, err
    assert "was dropped from the current schema" in err, err

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

    # The persisted spec is the contract, and query results cannot stand in for it: `part` is a
    # stored column, so `WHERE part = ...` returns the right rows even out of an unpartitioned file
    # (pruning is an optimization, not a correctness mechanism). Assert the metadata itself.
    meta_after = _read_live_metadata(instance, TABLE_NAME)
    specs = meta_after.get("partition-specs") or []
    default_spec_id = meta_after.get("default-spec-id")
    default_spec = next((s for s in specs if s.get("spec-id") == default_spec_id), None)
    assert default_spec is not None, (
        f"OPTIMIZE lost the default partition spec: default-spec-id={default_spec_id}, specs={specs}"
    )
    assert [f.get("name") for f in default_spec.get("fields") or []] == ["part"], (
        f"partition spec lost its field after OPTIMIZE: {default_spec}"
    )

    # A fresh INSERT after compaction must still be partitioned: the spec survived, so each
    # partition value lands in its own data file.
    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES (0,5,'e'),(1,6,'f');",
        settings={"allow_insert_into_iceberg": 1},
    )

    # Every live data file carries a partition tuple ('{0}' / '{1}'); an unpartitioned write
    # would show up as '{}'.
    assert (
        int(
            instance.query(
                f"SELECT countIf(partition = '{{}}') FROM system.iceberg_files "
                f"WHERE table = '{TABLE_NAME}' AND content = 'DATA'"
            )
        )
        == 0
    ), "a data file was written without a partition tuple after OPTIMIZE"

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

    err = instance.query_and_get_error(
        f"OPTIMIZE TABLE {TABLE_NAME};",
        settings={
            "allow_experimental_iceberg_compaction": 1,
            "allow_insert_into_iceberg": 1,
        },
    )
    # Name the guard, not just the error code: several unrelated refusals also raise
    # NOT_IMPLEMENTED, and this test exists to prove the decimal change was what stopped it.
    assert "NOT_IMPLEMENTED" in err, err
    assert "changed type" in err, err
    assert f"field id {field_id}" in err, err


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

    def object_set():
        """Every data and metadata object, so a leak of any kind is visible."""
        listing = instance.exec_in_container(
            ["bash", "-c", f"ls {data_dir} {metadata_dir} 2>/dev/null | sort"]
        )
        return set(line for line in listing.split("\n") if line.strip())

    def data_file_count():
        return int(
            instance.exec_in_container(
                ["bash", "-c", f"ls {data_dir}/*.parquet 2>/dev/null | wc -l"]
            ).strip()
        )

    before = data_file_count()
    # Snapshot BEFORE the winner exists, which is what compaction's own pre-rewrite listing sees.
    # The winner must therefore be preserved because cleanup only touches names the rewrite itself
    # generated, not merely because it happened to be in that listing.
    objects_before = object_set()
    # Concurrent winner: a valid vN+1 already committed. Keep the version hint on vN so compaction
    # plans from vN but its commit target vN+1 already exists.
    instance.exec_in_container(["bash", "-c", f"cp {latest_path} {next_path}"])
    instance.exec_in_container(
        ["bash", "-c", f"echo -n {n} > {metadata_dir}/version-hint.text"]
    )
    expected_after = objects_before | {next_path.rsplit("/", 1)[-1]}

    err = instance.query_and_get_error(
        f"OPTIMIZE TABLE {TABLE_NAME};",
        settings={
            "allow_experimental_iceberg_compaction": 1,
            "allow_insert_into_iceberg": 1,
        },
    )
    assert "CONCURRENT_ACCESS_NOT_SUPPORTED" in err, err
    # Compare the FULL object set, not just the data files: an implementation could remove the
    # rewritten data files while leaking manifests, manifest lists or a partially written metadata
    # JSON, and a data-file-only count would not notice. The pre-compaction objects must all still
    # be there (clearOldFiles skipped) and nothing new may remain, since nothing references the
    # rewritten files once the commit is lost.
    objects_after = object_set()
    assert objects_after == expected_after, (
        f"objects changed after a lost commit;\n"
        f"  leaked (orphaned rewrite output): {sorted(objects_after - expected_after)}\n"
        f"  missing (deleted pre-compaction file or the concurrent winner): "
        f"{sorted(expected_after - objects_after)}"
    )
    assert data_file_count() == before
    # The winning writer's metadata must survive the cleanup (deleting it would be worse than the
    # leak). It is part of objects_before, so the set comparison covers it; asserted separately to
    # name the failure.
    assert (
        instance.exec_in_container(["bash", "-c", f"test -f {next_path} && echo yes || echo no"]).strip()
        == "yes"
    ), "cleanup after a lost commit removed the concurrent winner's metadata"
    # The table still reads its pre-compaction contents.
    assert (
        instance.query(f"SELECT id, value FROM {TABLE_NAME} ORDER BY id") == "1\ta\n3\tc\n"
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


@pytest.mark.parametrize("storage_type", ["local"])
def test_optimize_rejected_after_added_column(
    started_cluster_iceberg_no_spark, storage_type
):
    """
    An ADD COLUMN must also block compaction. The rewrite materializes the current schema into the
    data file, so the added field id lands in a file that an older, still-reachable snapshot
    resolves against its own schema, where that id does not exist. The reader then refuses the file
    ("field_id N that is not in datalake metadata") and the rows become unreadable through the
    current snapshot too, not only through time travel.
    """
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    TABLE_NAME = "test_optimize_added_column_" + storage_type + "_" + get_uuid_str()

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
    # Positional delete so compaction would otherwise have work to do.
    instance.query(
        f"DELETE FROM {TABLE_NAME} WHERE id = 2;",
        settings={"allow_insert_into_iceberg": 1},
    )
    instance.query(f"ALTER TABLE {TABLE_NAME} ADD COLUMN extra Nullable(String);")

    err = instance.query_and_get_error(
        f"OPTIMIZE TABLE {TABLE_NAME};",
        settings={
            "allow_experimental_iceberg_compaction": 1,
            "allow_insert_into_iceberg": 1,
        },
    )
    assert "NOT_IMPLEMENTED" in err, err
    assert "added field id" in err, err

    # The table must still be readable: the rejection has to happen before anything is rewritten.
    assert (
        instance.query(f"SELECT id, value, extra FROM {TABLE_NAME} ORDER BY id")
        == "1\ta\t\\N\n3\tc\t\\N\n"
    )


@pytest.mark.parametrize("storage_type", ["local"])
def test_optimize_rejected_after_requiredness_change(
    started_cluster_iceberg_no_spark, storage_type
):
    """
    Making a required field optional changes only `required` in the schema, not the type string, so
    the guard has to look at requiredness as well. Without that the rewrite reaches the Parquet
    writer, which casts the column to ColumnNullable for a now-optional field and aborts on the
    still-required data.
    """
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    TABLE_NAME = "test_optimize_requiredness_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_no_spark,
        "(id Int32, value String)",
        2,
    )

    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES (1,'a'),(2,'b'),(3,'c');",
        settings={"allow_insert_into_iceberg": 1},
    )
    # Positional delete so compaction would otherwise have work to do.
    instance.query(
        f"DELETE FROM {TABLE_NAME} WHERE id = 2;",
        settings={"allow_insert_into_iceberg": 1},
    )
    instance.query(f"ALTER TABLE {TABLE_NAME} MODIFY COLUMN value Nullable(String);")

    err = instance.query_and_get_error(
        f"OPTIMIZE TABLE {TABLE_NAME};",
        settings={
            "allow_experimental_iceberg_compaction": 1,
            "allow_insert_into_iceberg": 1,
        },
    )
    assert "NOT_IMPLEMENTED" in err, err
    # The guard's own wording, so an unrelated broad refusal cannot satisfy this.
    assert "changed type" in err, err

    # Rejected before anything was rewritten, so the table still reads.
    assert (
        instance.query(f"SELECT id, value FROM {TABLE_NAME} ORDER BY id") == "1\ta\n3\tc\n"
    )


@pytest.mark.parametrize("storage_type", ["local"])
def test_optimize_rejected_after_nested_requiredness_change(
    started_cluster_iceberg_no_spark, storage_type
):
    """
    Requiredness matters at every level, not just the top. A nested struct field going from
    optional to required changes only its `required` flag, so a guard that inspects only top-level
    fields lets the rewrite through and the Parquet writer aborts on the mismatched column.

    ClickHouse DDL cannot express a nested requiredness change, so the evolved schema is injected
    into the metadata directly, which is enough because the guard runs before any file is written.
    """
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    TABLE_NAME = "test_optimize_nested_required_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_no_spark,
        "(id Int32, s Tuple(a Nullable(String), n Nullable(Int32)))",
        2,
    )

    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES (1,('x',10)),(2,('y',20)),(3,('z',30));",
        settings={"allow_insert_into_iceberg": 1},
    )
    # Positional delete so compaction would otherwise have work to do.
    instance.query(
        f"DELETE FROM {TABLE_NAME} WHERE id = 2;",
        settings={"allow_insert_into_iceberg": 1},
    )

    meta, latest_path = _read_latest_metadata(instance, TABLE_NAME)
    n = int(latest_path.split("/v")[-1].split(".")[0])
    evolved = json.loads(json.dumps(meta["schemas"][0]))
    evolved["schema-id"] = 1
    nested_id = None
    for field in evolved["fields"]:
        if isinstance(field.get("type"), dict) and field["type"].get("type") == "struct":
            nested = field["type"]["fields"][0]
            nested["required"] = not nested.get("required", False)
            nested_id = nested["id"]
    assert nested_id is not None, "expected a nested struct field to flip"
    meta["schemas"].append(evolved)
    meta["current-schema-id"] = 1
    _write_metadata(instance, meta, f"{_metadata_dir(TABLE_NAME)}/v{n + 1}.metadata.json")

    err = instance.query_and_get_error(
        f"OPTIMIZE TABLE {TABLE_NAME};",
        settings={
            "allow_experimental_iceberg_compaction": 1,
            "allow_insert_into_iceberg": 1,
        },
    )
    assert "NOT_IMPLEMENTED" in err, err
    # The nested field must be the one named, which a top-level-only guard could never report.
    assert f"field id {nested_id}" in err, err


def test_compaction_keeps_everything_if_the_commit_throws():
    """
    The commit helper publishes the metadata before it inspects the version hint, and those hint
    operations are unguarded, so an exception can escape after the table was already committed. The
    call must therefore be wrapped and keep every file, or the cleanup would delete the rewritten
    data that the published metadata references.

    Asserted statically: making the helper throw after publishing needs fault injection inside
    object storage, which an integration test cannot arrange.
    """
    source = pathlib.Path(__file__).resolve().parents[3] / (
        "src/Storages/ObjectStorage/DataLakes/Iceberg/Compaction.cpp"
    )
    statements = " ".join(source.read_text().split())
    assert "writeMetadataFileAndVersionHint" in statements, (
        f"{source} no longer commits through writeMetadataFileAndVersionHint; update this guard"
    )
    # The `try {` must be the last thing before the call, and its handler must keep everything.
    # Checking only for "a try somewhere earlier" would pass even with the wrapper deleted.
    call = statements.index("Iceberg::writeMetadataFileAndVersionHint")
    window = statements[max(0, call - 120) : call]
    assert "try {" in window, (
        "the metadata commit is no longer directly inside a try block, so a throw after publication "
        f"would reach the cleanup handler; preceding text: {window!r}"
    )
    handler = statements[call : call + 1200]
    assert "CommitResult::KeepEverything" in handler, (
        "the metadata commit's exception handler no longer keeps every file, so a throw after "
        "publication would delete data the committed metadata references"
    )


def test_compaction_never_cleans_up_the_commit_target():
    """
    The metadata name compaction commits to must never join the cleanup set. On a lost commit that
    name belongs to the winning writer, so removing it would leave the table pointing at missing
    metadata: strictly worse than the leak the cleanup exists to prevent.

    This is asserted statically because a single-process test cannot reach the window. The winner
    has to appear AFTER compaction's own initial listing, and any in-process test plants it before
    OPTIMIZE is called, which puts it in that listing and masks the defect.
    """
    source = pathlib.Path(__file__).resolve().parents[3] / (
        "src/Storages/ObjectStorage/DataLakes/Iceberg/Compaction.cpp"
    )
    text = source.read_text()
    assert "generated_metadata_paths" in text, (
        f"{source} no longer tracks generated paths; this guard needs updating"
    )
    # Scan whitespace-collapsed statements rather than single lines, so an alias, a second call
    # site or a wrapped statement cannot slip past. This is a proxy for a race a single-process
    # test cannot stage, so it is deliberately broad: any insertion of the commit target into the
    # cleanup list trips it.
    statements = " ".join(text.split()).split(";")
    offenders = [
        st.strip()
        for st in statements
        if "generated_metadata_paths" in st
        and "generated_metadata_info" in st
        and any(op in st for op in ("push_back", "emplace_back", "insert", "assign", "="))
    ]
    assert not offenders, (
        "the metadata commit target was added to the compaction cleanup set, which deletes the "
        f"concurrent winner's metadata on a lost commit: {offenders}"
    )
