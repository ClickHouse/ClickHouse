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
    `writeMetadataFile` returns false when the target `vN.metadata.json` already exists (a
    concurrent writer won the commit). Compaction must propagate that and skip `clearOldFiles`,
    otherwise it deletes the pre-compaction data/metadata while the table still points at the
    other writer's snapshot -> data loss. We simulate the race by pre-creating a valid next
    metadata version (the "winner") that ranks below the current one by `last-updated-ms`, so
    compaction reads the current metadata but its commit target already exists. OPTIMIZE must
    fail and the pre-compaction data files must survive.
    """
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    TABLE_NAME = "test_optimize_commit_conflict_" + storage_type + "_" + get_uuid_str()

    # Rank metadata by `last-updated-ms` rather than by version number, so a valid vN+1 that is
    # older by that field leaves compaction reading vN.
    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_no_spark,
        "(id Int32, value Nullable(String))",
        2,
        additional_settings=["iceberg_recent_metadata_file_by_last_updated_ms_field = true"],
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
    meta, latest_path = _read_latest_metadata(instance, TABLE_NAME)
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
    # Concurrent winner: a valid vN+1 that ranks below vN, so compaction plans from vN but its
    # commit target vN+1 already exists.
    meta["last-updated-ms"] -= 1
    _write_metadata(instance, meta, next_path)
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


@pytest.mark.parametrize("hint", ["unreadable", "ahead"])
def test_optimize_keeps_both_generations_if_the_hint_is_not_confirmed(
    started_cluster_iceberg_no_spark, hint
):
    """
    The compacted metadata is published before `version-hint.text` is updated. When that update
    throws (`unreadable`) or the hint cannot be confirmed to name the new version (`ahead`), a
    reader may still resolve the previous generation, so OPTIMIZE must succeed and delete neither
    the pre-compaction files nor the rewritten ones.
    """
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    TABLE_NAME = "test_optimize_unconfirmed_hint_" + hint + "_" + get_uuid_str()
    table_dir = f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}"
    metadata_dir = _metadata_dir(TABLE_NAME)

    create_iceberg_table(
        "local",
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

    _, pre_path = _read_latest_metadata(instance, TABLE_NAME)
    n = int(pre_path.split("/v")[-1].split(".")[0])
    hint_path = f"{metadata_dir}/version-hint.text"
    if hint == "unreadable":
        # Reading a directory fails, so the hint update throws after the metadata is written.
        instance.exec_in_container(["bash", "-c", f"rm -f {hint_path} && mkdir {hint_path}"])
    else:
        instance.exec_in_container(["bash", "-c", f"echo {n + 100} > {hint_path}"])

    def object_set():
        listing = instance.exec_in_container(
            ["bash", "-c", f"find {table_dir}/data {metadata_dir} -type f | sort"]
        )
        return set(line for line in listing.split("\n") if line.strip())

    objects_before = object_set()
    instance.query(
        f"OPTIMIZE TABLE {TABLE_NAME};",
        settings={
            "allow_experimental_iceberg_compaction": 1,
            "allow_insert_into_iceberg": 1,
        },
    )
    objects_after = object_set()

    assert objects_before <= objects_after, (
        f"pre-compaction files were removed: {sorted(objects_before - objects_after)}"
    )
    written = objects_after - objects_before
    assert f"{metadata_dir}/v{n + 1}.metadata.json" in written, sorted(written)
    assert any(f.startswith(f"{table_dir}/data/") for f in written), (
        f"OPTIMIZE rewrote no data file: {sorted(written)}"
    )

    # Both generations stay readable: the compacted one and the one an unconfirmed hint may name.
    assert instance.query(f"SELECT id, value FROM {TABLE_NAME} ORDER BY id") == "1\ta\n3\tc\n"
    instance.query(
        f"CREATE TABLE {TABLE_NAME}_pre ENGINE = IcebergLocal(local, path = '{table_dir}', format = Parquet) "
        f"SETTINGS iceberg_metadata_file_path = 'metadata/{pre_path.rsplit('/', 1)[-1]}'"
    )
    assert instance.query(f"SELECT id, value FROM {TABLE_NAME}_pre ORDER BY id") == "1\ta\n3\tc\n"


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
