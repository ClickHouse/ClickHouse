import uuid

from helpers.export_partition_helpers import (
    export_transaction_id,
    make_source,
    wait_for_export_status,
    wait_for_new_export_transaction,
)

from .common import (
    create_s3_table,
    create_tables_and_insert_data,
    source_engine_clause,
)

CLUSTER_INSTANCES = ["replica1", "replica_with_export_disabled"]

# The happy paths and the user-facing guards of `EXPORT PARTITION` into a plain object-storage
# destination: exporting one partition or all of them, the already-exists policies, permissions,
# and the pending mutation / patch part gates.


def test_export_partition_file_already_exists_policy(cluster, source_engine):
    node = cluster.instances["replica1"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"export_partition_file_already_exists_policy_mt_table_{postfix}"
    s3_table = f"export_partition_file_already_exists_policy_s3_table_{postfix}"

    create_tables_and_insert_data(node, mt_table, s3_table, "replica1", engine=source_engine)

    # stop merges so part names remain stable. it is important for the test.
    node.query(f"SYSTEM STOP MERGES {mt_table}")

    # Export all parts
    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table}",
    )

    # check system.partition_exports for the export
    assert node.query(
        f"""
        SELECT status FROM system.partition_exports
        WHERE source_table = '{mt_table}'
          AND destination_table = '{s3_table}'
          AND partition_id = '2020'
        """
    ) == "COMPLETED\n", "Export should be marked as COMPLETED"

    # wait for the exports to finish
    wait_for_export_status(node, mt_table, s3_table, "2020", "COMPLETED")

    # plain object storage destinations surface the commit marker file path via
    # system.partition_exports.committed_marker_file
    committed_marker_file = node.query(
        f"""
        SELECT committed_marker_file FROM system.partition_exports
        WHERE source_table = '{mt_table}'
          AND destination_table = '{s3_table}'
          AND partition_id = '2020'
        """
    ).strip()
    # `committed_marker_file` is the absolute key in the bucket (same convention as
    # `destination_file_paths`); it may carry the s3_conn URL's in-bucket prefix on
    # top of the table's `filename` argument, so use a "contains" check that does
    # not depend on knowing that prefix.
    assert f"{s3_table}/commit_2020_" in committed_marker_file, \
        f"Expected committed_marker_file under {s3_table}/, got: {committed_marker_file!r}"
    # Path relative to the `s3_conn` URL, derived from the absolute key without
    # assuming a particular URL prefix.
    marker_relative_path = committed_marker_file[committed_marker_file.index(f"{s3_table}/"):]
    assert node.query(
        f"SELECT count() FROM s3(s3_conn, filename='{marker_relative_path}', format=LineAsString)"
    ) == '1\n', f"Commit marker file does not exist at {committed_marker_file!r}"

    # try to export the partition
    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table} SETTINGS export_merge_tree_partition_force_export=1"
    )

    wait_for_export_status(node, mt_table, s3_table, "2020", "COMPLETED")

    assert node.query(
        f"""
        SELECT count() FROM system.partition_exports
        WHERE source_table = '{mt_table}'
          AND destination_table = '{s3_table}'
          AND partition_id = '2020'
          AND status = 'COMPLETED'
        """
    ) == '1\n', "Expected the export to be marked as COMPLETED"

    # overwrite policy
    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table} SETTINGS export_merge_tree_partition_force_export=1, export_merge_tree_part_file_already_exists_policy='overwrite'"
    )

    # wait for the export to finish
    wait_for_export_status(node, mt_table, s3_table, "2020", "COMPLETED")

    # check system.partition_exports for the export
    # ideally we would make sure the transaction id is different, but I do not have the time to do that now
    assert node.query(
        f"""
        SELECT count() FROM system.partition_exports
        WHERE source_table = '{mt_table}'
          AND destination_table = '{s3_table}'
          AND partition_id = '2020'
          AND status = 'COMPLETED'
        """
    ) == '1\n', "Expected the export to be marked as COMPLETED"

    # last but not least, the error policy. The `overwrite` export above finished every part and
    # left a per-part commit marker proving it, so there is nothing for this export to write and
    # it completes by reusing those files. `error` only refuses destination files that no commit
    # marker covers -- see test_export_partition_error_policy_rejects_incomplete_part.
    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table} SETTINGS export_merge_tree_partition_force_export=1, export_merge_tree_part_file_already_exists_policy='error'",
    )

    # wait for the export to finish
    wait_for_export_status(node, mt_table, s3_table, "2020", "COMPLETED")

    # check system.partition_exports for the export
    assert node.query(
        f"""
        SELECT count() FROM system.partition_exports
        WHERE source_table = '{mt_table}'
          AND destination_table = '{s3_table}'
          AND partition_id = '2020'
          AND status = 'COMPLETED'
        """
    ) == '1\n', "Expected the export to be marked as COMPLETED"


def create_split_export_tables(node, mt_table, s3_table, replica_name, engine):
    """Create a source table whose part splits into one destination file per row on export.

    `export_merge_tree_part_max_rows_per_file` is evaluated once per chunk rather than per row
    (see `MultiFileStorageObjectStorageSink::consume`), and `MergeTreeSequentialSource` emits one
    chunk per index granule, so a part can only split at granule boundaries. With the default
    granularity a small part is a single granule and never splits at all, hence
    `index_granularity = 1`. `index_granularity_bytes = 0` disables adaptive granularity, which
    would otherwise choose the granule size itself.
    """
    node.query(f"DROP TABLE IF EXISTS {mt_table} SYNC")
    make_source(
        node, mt_table, "id UInt64, year UInt16", "year",
        engine=engine, replica_name=replica_name,
        extra_settings="index_granularity = 1, index_granularity_bytes = 0",
    )
    node.query(f"INSERT INTO {mt_table} VALUES (1, 2020), (2, 2020), (3, 2020), (4, 2021)")
    create_s3_table(node, s3_table)


def export_partition_split_into_files(
    node, mt_table, s3_table, force=False, policy=None, previous_transaction_id=None,
    expected_status="COMPLETED",
):
    """Export partition 2020 with one row per destination file and wait for *expected_status*.

    Only splits per row for a table built by `create_split_export_tables`.
    """
    settings = ["export_merge_tree_part_max_rows_per_file = 1"]
    if force:
        settings.append("export_merge_tree_partition_force_export = 1")
    if policy:
        settings.append(f"export_merge_tree_part_file_already_exists_policy = '{policy}'")

    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table} "
        f"SETTINGS {', '.join(settings)}"
    )

    if previous_transaction_id is not None:
        wait_for_new_export_transaction(node, mt_table, s3_table, "2020", previous_transaction_id)

    wait_for_export_status(node, mt_table, s3_table, "2020", expected_status)


def recorded_export_paths(node, mt_table, s3_table):
    """Destination file paths recorded for the exported parts, in the order the sink wrote them.

    This is what the commit phase turns into the partition commit marker.
    """
    paths = node.query(
        f"""
        SELECT arrayJoin(arrayFlatten(mapValues(destination_file_paths)))
        FROM system.partition_exports
        WHERE source_table = '{mt_table}'
          AND destination_table = '{s3_table}'
          AND partition_id = '2020'
        """
    )
    return [path for path in paths.splitlines() if path]


def partition_commit_marker_lines(node, mt_table, s3_table):
    """Data-file paths listed inside the partition-level commit marker."""
    committed_marker_file = node.query(
        f"""
        SELECT committed_marker_file FROM system.partition_exports
        WHERE source_table = '{mt_table}'
          AND destination_table = '{s3_table}'
          AND partition_id = '2020'
        """
    ).strip()

    assert f"{s3_table}/commit_2020_" in committed_marker_file, \
        f"Expected committed_marker_file under {s3_table}/, got: {committed_marker_file!r}"
    marker_relative_path = committed_marker_file[committed_marker_file.index(f"{s3_table}/"):]

    lines = node.query(
        f"SELECT * FROM s3(s3_conn, filename='{marker_relative_path}', format=LineAsString)"
    )
    return [line for line in lines.splitlines() if line]


def list_partition_directory(cluster, data_path):
    """Object keys sitting next to *data_path*, split into data files and commit markers.

    The per-part commit marker is written by `MultiFileStorageObjectStorageSink::commit` in the
    same directory as the data files, named `commit_<destination file name>`.
    """
    directory = data_path.rsplit("/", 1)[0] + "/"
    object_names = sorted(
        obj.object_name
        for obj in cluster.minio_client.list_objects(
            cluster.minio_bucket, prefix=directory, recursive=True
        )
    )
    data_files = [n for n in object_names if not n.rsplit("/", 1)[-1].startswith("commit_")]
    markers = [n for n in object_names if n.rsplit("/", 1)[-1].startswith("commit_")]
    return data_files, markers


def test_export_partition_skip_policy_reports_every_split_file(cluster, source_engine):
    """A `skip` re-export of an already-exported multi-file part must record every destination
    file, not just the first one.

    The recorded list is what the commit phase turns into the partition commit marker, so
    dropping the later split files from it misrepresents the export even though the data is all
    there.
    """
    node = cluster.instances["replica1"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"skip_reports_all_files_mt_table_{postfix}"
    s3_table = f"skip_reports_all_files_s3_table_{postfix}"

    create_split_export_tables(node, mt_table, s3_table, "replica1", engine=source_engine)
    # The destination file name is derived from the part name, so part names have to stay stable
    # across the two exports, otherwise the second one writes to fresh paths and skips nothing.
    node.query(f"SYSTEM STOP MERGES {mt_table}")

    export_partition_split_into_files(node, mt_table, s3_table)
    first_transaction_id = export_transaction_id(node, mt_table, s3_table, "2020")

    exported_paths = recorded_export_paths(node, mt_table, s3_table)
    assert len(exported_paths) == 3, \
        f"Expected the 3-row partition to split into 3 files, got {exported_paths}"
    assert len(partition_commit_marker_lines(node, mt_table, s3_table)) == 3

    # Re-export. Every destination file is already there, so `skip` short-circuits the part --
    # but it must do so with the complete file list.
    export_partition_split_into_files(
        node, mt_table, s3_table, force=True, policy="skip",
        previous_transaction_id=first_transaction_id,
    )

    skipped_paths = recorded_export_paths(node, mt_table, s3_table)
    assert sorted(skipped_paths) == sorted(exported_paths), (
        f"Skipped re-export recorded {skipped_paths} instead of all 3 split files {exported_paths}"
    )

    committed = partition_commit_marker_lines(node, mt_table, s3_table)
    assert len(committed) == 3, \
        f"Skipped re-export committed {len(committed)} path(s) instead of all 3 split files: {committed}"


def test_export_partition_skip_policy_reexports_incomplete_part(cluster, source_engine):
    """A part whose multi-file export was interrupted must be re-exported in full under `skip`.

    The first split file existing proves nothing on its own: only the per-part commit marker,
    written after the last file is finalized, proves the part was fully exported. Removing the
    trailing files together with the marker reproduces what an attempt that died mid-part leaves
    behind, and the retry has to rewrite them -- the rows in those files are produced by no other
    attempt.
    """
    node = cluster.instances["replica1"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"skip_reexports_partial_mt_table_{postfix}"
    s3_table = f"skip_reexports_partial_s3_table_{postfix}"

    create_split_export_tables(node, mt_table, s3_table, "replica1", engine=source_engine)
    node.query(f"SYSTEM STOP MERGES {mt_table}")

    export_partition_split_into_files(node, mt_table, s3_table)
    first_transaction_id = export_transaction_id(node, mt_table, s3_table, "2020")

    written_in_order = recorded_export_paths(node, mt_table, s3_table)
    assert len(written_in_order) == 3, \
        f"Expected the 3-row partition to split into 3 files, got {written_in_order}"

    data_files, markers = list_partition_directory(cluster, written_in_order[0])
    assert data_files == sorted(written_in_order), \
        f"Objects in the partition directory {data_files} do not match the recorded paths {written_in_order}"
    assert len(markers) == 1, f"Expected one per-part commit marker, got {markers}"

    # Roll the destination back to "first file finalized, nothing else": drop the trailing files
    # and the marker that would otherwise prove the part complete.
    for key in written_in_order[1:] + markers:
        cluster.minio_client.remove_object(cluster.minio_bucket, key)

    surviving_data_files, surviving_markers = list_partition_directory(cluster, written_in_order[0])
    assert surviving_data_files == [written_in_order[0]], \
        f"Expected only the first split file to remain, got {surviving_data_files}"
    assert surviving_markers == [], \
        f"Expected the per-part commit marker to be gone, got {surviving_markers}"

    export_partition_split_into_files(
        node, mt_table, s3_table, force=True, policy="skip",
        previous_transaction_id=first_transaction_id,
    )

    data_files_after, markers_after = list_partition_directory(cluster, written_in_order[0])
    assert len(data_files_after) == 3, (
        f"Retry left the part partially exported: {data_files_after} "
        f"(the interrupted attempt's missing files were never rewritten)"
    )
    assert len(markers_after) == 1, \
        f"Retry did not rewrite the per-part commit marker: {markers_after}"
    assert node.query(f"SELECT count() FROM {s3_table} WHERE year = 2020") == "3\n", \
        "Rows from the split files the interrupted attempt never wrote are missing from the destination"
    assert len(partition_commit_marker_lines(node, mt_table, s3_table)) == 3


def test_export_partition_error_policy_adopts_completed_part(cluster, source_engine):
    """Under `error`, a part an earlier attempt already finished must be adopted, not failed.

    A part export is retried whenever the destination write succeeded but the outcome never
    became durable: the descriptor write failing, the server going down between the two, or a
    Keeper hiccup on the replicated path. The per-part commit marker proves the earlier attempt
    produced the whole file set, so the retry has nothing left to write. Treating that as a
    conflict makes a transient bookkeeping failure permanent while the exported data is intact.
    """
    node = cluster.instances["replica1"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"error_adopts_completed_mt_table_{postfix}"
    s3_table = f"error_adopts_completed_s3_table_{postfix}"

    create_split_export_tables(node, mt_table, s3_table, "replica1", engine=source_engine)
    # The destination file name is derived from the part name, so part names have to stay stable
    # across the two exports, otherwise the second one writes to fresh paths and finds no marker.
    node.query(f"SYSTEM STOP MERGES {mt_table}")

    export_partition_split_into_files(node, mt_table, s3_table)
    first_transaction_id = export_transaction_id(node, mt_table, s3_table, "2020")

    exported_paths = recorded_export_paths(node, mt_table, s3_table)
    assert len(exported_paths) == 3, \
        f"Expected the 3-row partition to split into 3 files, got {exported_paths}"

    # Stands in for the retry of a part whose success was never recorded: same part, same
    # destination paths, same commit marker, `error` policy.
    export_partition_split_into_files(
        node, mt_table, s3_table, force=True, policy="error",
        previous_transaction_id=first_transaction_id,
    )

    adopted_paths = recorded_export_paths(node, mt_table, s3_table)
    assert sorted(adopted_paths) == sorted(exported_paths), (
        f"Re-export under `error` recorded {adopted_paths} instead of the committed file set "
        f"{exported_paths}"
    )
    assert len(partition_commit_marker_lines(node, mt_table, s3_table)) == 3

    data_files_after, markers_after = list_partition_directory(cluster, exported_paths[0])
    assert sorted(data_files_after) == sorted(exported_paths), \
        f"Adopting the committed file set must not write anything new: {data_files_after}"
    assert len(markers_after) == 1, f"Expected one per-part commit marker, got {markers_after}"
    assert node.query(f"SELECT count() FROM {s3_table} WHERE year = 2020") == "3\n", \
        "Adopting the committed file set must not duplicate rows at the destination"


def test_export_partition_error_policy_rejects_incomplete_part(cluster, source_engine):
    """`error` must still refuse destination files that no commit marker covers.

    Only the per-part commit marker, written after the last file is finalized, proves a previous
    attempt produced the whole set. Files left by an attempt that died mid-part say nothing about
    how many files the part needs, so adopting them would record a truncated list as the part's
    export result and publish a fraction of its rows.
    """
    node = cluster.instances["replica1"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"error_rejects_partial_mt_table_{postfix}"
    s3_table = f"error_rejects_partial_s3_table_{postfix}"

    create_split_export_tables(node, mt_table, s3_table, "replica1", engine=source_engine)
    node.query(f"SYSTEM STOP MERGES {mt_table}")

    export_partition_split_into_files(node, mt_table, s3_table)
    first_transaction_id = export_transaction_id(node, mt_table, s3_table, "2020")

    written_in_order = recorded_export_paths(node, mt_table, s3_table)
    assert len(written_in_order) == 3, \
        f"Expected the 3-row partition to split into 3 files, got {written_in_order}"

    _, markers = list_partition_directory(cluster, written_in_order[0])
    assert len(markers) == 1, f"Expected one per-part commit marker, got {markers}"

    # Roll the destination back to "first file finalized, nothing else".
    for key in written_in_order[1:] + markers:
        cluster.minio_client.remove_object(cluster.minio_bucket, key)

    export_partition_split_into_files(
        node, mt_table, s3_table, force=True, policy="error",
        previous_transaction_id=first_transaction_id,
        expected_status="FAILED",
    )

    data_files_after, markers_after = list_partition_directory(cluster, written_in_order[0])
    assert data_files_after == [written_in_order[0]], \
        f"A rejected part must be left untouched, got {data_files_after}"
    assert markers_after == [], \
        f"A rejected part must not be marked complete, got {markers_after}"


def test_export_partition_feature_is_disabled(cluster, source_engine):
    replica_with_export_disabled = cluster.instances["replica_with_export_disabled"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"export_partition_feature_is_disabled_mt_table_{postfix}"
    s3_table = f"export_partition_feature_is_disabled_s3_table_{postfix}"

    create_tables_and_insert_data(replica_with_export_disabled, mt_table, s3_table, "replica1", engine=source_engine)

    error = replica_with_export_disabled.query_and_get_error(f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table};")
    assert "experimental" in error, "Expected error about disabled feature"

    # make sure kill operation also throws
    error = replica_with_export_disabled.query_and_get_error(f"KILL EXPORT PARTITION WHERE partition_id = '2020' and source_table = '{mt_table}' and destination_table = '{s3_table}'")
    assert "experimental" in error, "Expected error about disabled feature"


def test_export_partition_permissions(cluster, source_engine):
    """Test that export partition validates permissions correctly:
    - User needs ALTER permission on source table
    - User needs INSERT permission on destination table
    """
    node = cluster.instances["replica1"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"permissions_mt_table_{postfix}"
    s3_table = f"permissions_s3_table_{postfix}"

    # Create tables as default user
    create_tables_and_insert_data(node, mt_table, s3_table, "replica1", engine=source_engine)

    # Create test users with specific permissions
    node.query("CREATE USER IF NOT EXISTS user_no_alter IDENTIFIED WITH no_password")
    node.query("CREATE USER IF NOT EXISTS user_no_insert IDENTIFIED WITH no_password")
    node.query("CREATE USER IF NOT EXISTS user_with_permissions IDENTIFIED WITH no_password")

    # Grant basic access to all users
    node.query(f"GRANT SELECT ON {mt_table} TO user_no_alter")
    node.query(f"GRANT SELECT ON {s3_table} TO user_no_alter")

    # user_no_insert has ALTER on source but no INSERT on destination
    node.query(f"GRANT ALTER ON {mt_table} TO user_no_insert")
    node.query(f"GRANT SELECT ON {s3_table} TO user_no_insert")

    # user_with_permissions has both ALTER and INSERT
    node.query(f"GRANT ALTER ON {mt_table} TO user_with_permissions")
    node.query(f"GRANT INSERT ON {s3_table} TO user_with_permissions")

    # Test 1: User without ALTER permission should fail
    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table}",
        user="user_no_alter"
    )

    assert "ACCESS_DENIED" in error or "Not enough privileges" in error, \
        f"Expected ACCESS_DENIED error for user without ALTER, got: {error}"

    # Test 2: User with ALTER but without INSERT permission should fail
    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table}",
        user="user_no_insert"
    )

    assert "ACCESS_DENIED" in error or "Not enough privileges" in error, \
        f"Expected ACCESS_DENIED error for user without INSERT, got: {error}"

    # Test 3: User with both ALTER and INSERT should succeed
    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table}",
        user="user_with_permissions"
    )

    # Wait for export to complete
    wait_for_export_status(node, mt_table, s3_table, "2020", "COMPLETED")

    # Verify the export succeeded
    result = node.query(f"SELECT count() FROM {s3_table} WHERE year = 2020")
    assert result.strip() == "3", f"Expected 3 rows exported, got: {result}"

    # Verify system table shows COMPLETED status
    status = node.query(
        f"""
        SELECT status FROM system.partition_exports
        WHERE source_table = '{mt_table}'
            AND destination_table = '{s3_table}'
            AND partition_id = '2020'
        """
    )
    assert status.strip() == "COMPLETED", f"Expected COMPLETED status, got: {status}"


# assert multiple exports within a single query are executed. They all share the same query id
# and previously the transaction id was the query id, which would cause problems
def test_multiple_exports_within_a_single_query(cluster, source_engine):
    node = cluster.instances["replica1"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"multiple_exports_within_a_single_query_mt_table_{postfix}"
    s3_table = f"multiple_exports_within_a_single_query_s3_table_{postfix}"

    create_tables_and_insert_data(node, mt_table, s3_table, "replica1", engine=source_engine)

    node.query(f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table}, EXPORT PARTITION ID '2021' TO TABLE {s3_table};")

    wait_for_export_status(node, mt_table, s3_table, "2020", "COMPLETED")
    wait_for_export_status(node, mt_table, s3_table, "2021", "COMPLETED")

    # assert the exports have been executed
    assert node.query(f"SELECT count() FROM {s3_table} WHERE year = 2020") == '3\n', "Export did not succeed"
    assert node.query(f"SELECT count() FROM {s3_table} WHERE year = 2021") == '1\n', "Export did not succeed"

    # check system.partition_exports for the exports
    assert node.query(
        f"""
        SELECT status FROM system.partition_exports
        WHERE source_table = '{mt_table}'
          AND destination_table = '{s3_table}'
          AND partition_id = '2020'
        """
    ) == "COMPLETED\n", "Export should be marked as COMPLETED"

    assert node.query(
        f"""
        SELECT status FROM system.partition_exports
        WHERE source_table = '{mt_table}'
          AND destination_table = '{s3_table}'
          AND partition_id = '2021'
        """
    ) == "COMPLETED\n", "Export should be marked as COMPLETED"


def test_pending_mutations_throw_before_export_partition(cluster, source_engine):
    """Test that pending mutations before export partition throw an error."""
    node = cluster.instances["replica1"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"pending_mutations_throw_partition_mt_table_{postfix}"
    s3_table = f"pending_mutations_throw_partition_s3_table_{postfix}"

    create_tables_and_insert_data(node, mt_table, s3_table, "replica1", engine=source_engine)

    node.query(f"SYSTEM STOP MERGES {mt_table}")

    node.query(f"ALTER TABLE {mt_table} UPDATE id = id + 100 WHERE year = 2020")

    mutations = node.query(f"SELECT count() FROM system.mutations WHERE table = '{mt_table}' AND is_done = 0")
    assert mutations.strip() != '0', "Mutation should be pending"

    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table} "
        f"SETTINGS export_merge_tree_part_throw_on_pending_mutations=true"
    )

    assert "PENDING_MUTATIONS_NOT_ALLOWED" in error, f"Expected error about pending mutations, got: {error}"


def test_pending_mutations_skip_before_export_partition(cluster, source_engine):
    """Test that pending mutations before export partition are skipped with throw_on_pending_mutations=false."""
    node = cluster.instances["replica1"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"pending_mutations_skip_partition_mt_table_{postfix}"
    s3_table = f"pending_mutations_skip_partition_s3_table_{postfix}"

    create_tables_and_insert_data(node, mt_table, s3_table, "replica1", engine=source_engine)

    node.query(f"SYSTEM STOP MERGES {mt_table}")

    node.query(f"ALTER TABLE {mt_table} UPDATE id = id + 100 WHERE year = 2020")

    mutations = node.query(f"SELECT count() FROM system.mutations WHERE table = '{mt_table}' AND is_done = 0")
    assert mutations.strip() != '0', "Mutation should be pending"

    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table} "
        f"SETTINGS export_merge_tree_part_throw_on_pending_mutations=false"
    )

    wait_for_export_status(node, mt_table, s3_table, "2020", "COMPLETED")

    result = node.query(f"SELECT id FROM {s3_table} WHERE year = 2020 ORDER BY id")
    assert "101" not in result and "102" not in result and "103" not in result, \
        "Export should contain original data before mutation"
    assert "1\n2\n3" in result, "Export should contain original data"


def test_pending_patch_parts_throw_before_export_partition(cluster, source_engine):
    """Test that pending patch parts before export partition throw an error with default settings."""
    node = cluster.instances["replica1"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"pending_patches_throw_partition_mt_table_{postfix}"
    s3_table = f"pending_patches_throw_partition_s3_table_{postfix}"

    create_tables_and_insert_data(node, mt_table, s3_table, "replica1", engine=source_engine)

    node.query(f"SYSTEM STOP MERGES {mt_table}")

    node.query(f"UPDATE {mt_table} SET id = id + 100 WHERE year = 2020")

    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table}"
    )

    node.query(f"DROP TABLE {mt_table}")

    assert "PENDING_MUTATIONS_NOT_ALLOWED" in error or "pending patch parts" in error.lower(), \
        f"Expected error about pending patch parts, got: {error}"


def test_pending_patch_parts_skip_before_export_partition(cluster, source_engine):
    """Test that pending patch parts before export partition are skipped with throw_on_pending_patch_parts=false."""
    node = cluster.instances["replica1"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"pending_patches_skip_partition_mt_table_{postfix}"
    s3_table = f"pending_patches_skip_partition_s3_table_{postfix}"

    create_tables_and_insert_data(node, mt_table, s3_table, "replica1", engine=source_engine)

    node.query(f"SYSTEM STOP MERGES {mt_table}")

    node.query(f"UPDATE {mt_table} SET id = id + 100 WHERE year = 2020")

    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table} "
        f"SETTINGS export_merge_tree_part_throw_on_pending_patch_parts=false"
    )

    wait_for_export_status(node, mt_table, s3_table, "2020", "COMPLETED")

    result = node.query(f"SELECT id FROM {s3_table} WHERE year = 2020 ORDER BY id")
    assert "1\n2\n3" in result, "Export should contain original data before patch"

    node.query(f"DROP TABLE {mt_table}")


def test_mutation_in_partition_clause(cluster):
    """Test that mutations limited to specific partitions using IN PARTITION clause
    allow exports of unaffected partitions to succeed.

    Replicated-only: a plain MergeTree's mutations snapshot is not partition-scoped, so a
    mutation confined to one partition still marks parts of every other partition as having
    pending mutations and the export of an unaffected partition is refused. See "Pending
    mutations" under Plain (non-replicated) MergeTree in docs/en/antalya/partition_export.md.
    """
    node = cluster.instances["replica1"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"mutation_in_partition_clause_mt_table_{postfix}"
    s3_table = f"mutation_in_partition_clause_s3_table_{postfix}"

    create_tables_and_insert_data(node, mt_table, s3_table, "replica1")

    node.query(f"SYSTEM STOP MERGES {mt_table}")

    # Issue a mutation that uses IN PARTITION to limit it to partition 2020
    node.query(f"ALTER TABLE {mt_table} UPDATE id = id + 100 IN PARTITION '2020' WHERE year = 2020")

    # Verify mutation is pending for 2020
    mutations = node.query(
        f"SELECT count() FROM system.mutations WHERE table = '{mt_table}' AND is_done = 0"
    )
    assert mutations.strip() != '0', "Mutation should be pending"

    # Export of 2020 should fail (it has pending mutations)
    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table} "
        f"SETTINGS export_merge_tree_part_throw_on_pending_mutations=true"
    )
    assert "PENDING_MUTATIONS_NOT_ALLOWED" in error, f"Expected error about pending mutations for partition 2020, got: {error}"

    # Export of 2021 should succeed (no mutations affecting it)
    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2021' TO TABLE {s3_table} "
        f"SETTINGS export_merge_tree_part_throw_on_pending_mutations=true"
    )

    wait_for_export_status(node, mt_table, s3_table, "2021", "COMPLETED")

    result = node.query(f"SELECT id FROM {s3_table} WHERE year = 2021 ORDER BY id")
    assert "4" in result, "Export of partition 2021 should contain original data"


def test_export_partition_with_mixed_computed_columns(cluster, source_engine):
    """Test export partition with ALIAS, MATERIALIZED, and EPHEMERAL columns."""
    node = cluster.instances["replica1"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"mixed_computed_mt_table_{postfix}"
    s3_table = f"mixed_computed_s3_table_{postfix}"

    node.query(f"""
        CREATE TABLE {mt_table} (
            id UInt32,
            value UInt32,
            tag_input String EPHEMERAL,
            doubled UInt64 ALIAS value * 2,
            tripled UInt64 MATERIALIZED value * 3,
            tag String DEFAULT upper(tag_input)
        ) ENGINE = {source_engine_clause(source_engine, mt_table)}
        PARTITION BY id
        ORDER BY id
        SETTINGS index_granularity = 1
    """)

    # Create S3 destination table with regular columns (no EPHEMERAL)
    node.query(f"""
        CREATE TABLE {s3_table} (
            id UInt32,
            value UInt32,
            doubled UInt64,
            tripled UInt64,
            tag String
        ) ENGINE = S3(s3_conn, filename='{s3_table}', format=Parquet, partition_strategy='hive')
        PARTITION BY id
    """)

    node.query(f"INSERT INTO {mt_table} (id, value, tag_input) VALUES (1, 5, 'test'), (1, 10, 'prod')")

    node.query(f"ALTER TABLE {mt_table} EXPORT PARTITION ID '1' TO TABLE {s3_table}")

    wait_for_export_status(node, mt_table, s3_table, "1", "COMPLETED")

    # Verify source data (ALIAS computed, EPHEMERAL not stored)
    source_result = node.query(f"SELECT id, value, doubled, tripled, tag FROM {mt_table} ORDER BY value")
    expected = "1\t5\t10\t15\tTEST\n1\t10\t20\t30\tPROD\n"
    assert source_result == expected, f"Source table data mismatch. Expected:\n{expected}\nGot:\n{source_result}"

    dest_result = node.query(f"SELECT id, value, doubled, tripled, tag FROM {s3_table} ORDER BY value")
    assert dest_result == expected, f"Exported data mismatch. Expected:\n{expected}\nGot:\n{dest_result}"

    status = node.query(f"""
        SELECT status FROM system.partition_exports
        WHERE source_table = '{mt_table}'
            AND destination_table = '{s3_table}'
            AND partition_id = '1'
    """)
    assert status.strip() == "COMPLETED", f"Expected COMPLETED status, got: {status}"


def test_export_partition_all(cluster, source_engine):
    """Happy path for `ALTER TABLE ... EXPORT PARTITION ALL TO TABLE ...`.

    Schedules one export task per active partition in a single ALTER, then
    verifies every partition lands in the destination S3 table.
    """
    node = cluster.instances["replica1"]

    uid = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"export_all_mt_{uid}"
    s3_table = f"export_all_s3_{uid}"

    node.query(
        f"CREATE TABLE {mt_table} (id UInt64, year UInt16)"
        f" ENGINE = {source_engine_clause(source_engine, mt_table)}"
        f" PARTITION BY year ORDER BY tuple()"
    )
    node.query(f"INSERT INTO {mt_table} VALUES (1, 2020), (2, 2021), (3, 2022)")
    create_s3_table(node, s3_table)

    node.query(f"ALTER TABLE {mt_table} EXPORT PARTITION ALL TO TABLE {s3_table}")

    for partition_id in ("2020", "2021", "2022"):
        wait_for_export_status(node, mt_table, s3_table, partition_id, "COMPLETED", timeout=60)

    row_count = int(node.query(f"SELECT count() FROM {s3_table}").strip())
    assert row_count == 3, f"Expected 3 rows in S3 after EXPORT PARTITION ALL, got {row_count}"


def test_export_partition_all_failure_modes(cluster, source_engine):
    """Cover the three values of `export_merge_tree_partition_all_on_error`.

    Set up an already-fully-exported source table, then re-run EXPORT PARTITION ALL
    with each failure mode and assert the documented behavior.
    """
    node = cluster.instances["replica1"]

    uid = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"export_all_modes_mt_{uid}"
    s3_table = f"export_all_modes_s3_{uid}"
    empty_mt = f"export_all_empty_mt_{uid}"

    node.query(
        f"CREATE TABLE {mt_table} (id UInt64, year UInt16)"
        f" ENGINE = {source_engine_clause(source_engine, mt_table)}"
        f" PARTITION BY year ORDER BY tuple()"
    )
    node.query(f"INSERT INTO {mt_table} VALUES (1, 2020), (2, 2021), (3, 2022)")
    create_s3_table(node, s3_table)

    # First run: schedule + wait for all partitions to complete.
    node.query(f"ALTER TABLE {mt_table} EXPORT PARTITION ALL TO TABLE {s3_table}")
    for partition_id in ("2020", "2021", "2022"):
        wait_for_export_status(node, mt_table, s3_table, partition_id, "COMPLETED", timeout=60)

    # Empty table: throws BAD_ARGUMENTS (no active partitions).
    node.query(
        f"CREATE TABLE {empty_mt} (id UInt64, year UInt16)"
        f" ENGINE = {source_engine_clause(source_engine, empty_mt)}"
        f" PARTITION BY year ORDER BY tuple()"
    )
    error = node.query_and_get_error(
        f"ALTER TABLE {empty_mt} EXPORT PARTITION ALL TO TABLE {s3_table}"
    )
    assert "no active partitions to export" in error, (
        f"Expected 'no active partitions' error, got: {error}"
    )

    # throw_first (default): re-run aborts on the first conflicting partition.
    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ALL TO TABLE {s3_table}"
        f" SETTINGS export_merge_tree_partition_all_on_error = 'throw_first'"
    )
    assert "EXPORT_PARTITION_ALREADY_EXPORTED" in error, (
        f"Expected EXPORT_PARTITION_ALREADY_EXPORTED in error, got: {error}"
    )

    # collect: aggregated PARTITION_EXPORT_FAILED message lists every conflicting partition.
    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ALL TO TABLE {s3_table}"
        f" SETTINGS export_merge_tree_partition_all_on_error = 'collect'"
    )
    assert "PARTITION_EXPORT_FAILED" in error, (
        f"Expected PARTITION_EXPORT_FAILED in error, got: {error}"
    )
    for partition_id in ("2020", "2021", "2022"):
        assert partition_id in error, (
            f"Expected aggregated error to mention partition {partition_id}, got: {error}"
        )

    # skip_conflicts: succeeds silently because every partition conflicts and is skipped.
    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ALL TO TABLE {s3_table}"
        f" SETTINGS export_merge_tree_partition_all_on_error = 'skip_conflicts'"
    )


def test_export_partition_with_a_fully_deleted_part(cluster, source_engine):
    """
    A part whose rows were all removed by a lightweight delete exports successfully without
    writing a file. The export must still commit, carrying only what the other parts produced.
    """
    node = cluster.instances["replica1"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"deleted_part_mt_{postfix}"
    s3_table = f"deleted_part_s3_{postfix}"

    # Merges are disabled because a merge applies the deleted mask: it would rewrite the two
    # parts below into one without the deleted rows, removing the case under test.
    make_source(
        node, mt_table, "id UInt64, year UInt16", "year",
        engine=source_engine, replica_name="replica1",
        extra_settings="max_bytes_to_merge_at_max_space_in_pool = 1",
    )
    node.query(f"INSERT INTO {mt_table} VALUES (1, 2020), (2, 2020)")
    node.query(f"INSERT INTO {mt_table} VALUES (3, 2020)")
    create_s3_table(node, s3_table)

    node.query(f"DELETE FROM {mt_table} WHERE id IN (1, 2)", settings={"mutations_sync": 2})

    node.query(f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table}")
    wait_for_export_status(node, mt_table, s3_table, "2020", "COMPLETED")

    result = node.query(f"SELECT id, year FROM {s3_table} ORDER BY id").strip()
    assert result == "3\t2020", f"Unexpected data in the destination table:\n{result}"

    exported_files = node.query(
        f"""
        SELECT length(arrayFlatten(mapValues(destination_file_paths)))
        FROM system.partition_exports
        WHERE source_table = '{mt_table}'
          AND destination_table = '{s3_table}'
          AND partition_id = '2020'
        """
    ).strip()
    assert exported_files == "1", (
        f"Expected only the surviving part to write a file, got {exported_files}"
    )


def test_export_partition_where_every_row_is_deleted(cluster, source_engine):
    """
    When no part of the partition has a surviving row the export produces no files at all.
    An empty export is not corrupted state: the task must reach COMPLETED and leave the
    destination untouched.
    """
    node = cluster.instances["replica1"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"deleted_all_mt_{postfix}"
    s3_table = f"deleted_all_s3_{postfix}"

    make_source(
        node, mt_table, "id UInt64, year UInt16", "year",
        engine=source_engine, replica_name="replica1",
        extra_settings="max_bytes_to_merge_at_max_space_in_pool = 1",
    )
    node.query(f"INSERT INTO {mt_table} VALUES (1, 2020), (2, 2020)")
    node.query(f"INSERT INTO {mt_table} VALUES (3, 2020)")
    create_s3_table(node, s3_table)

    node.query(f"DELETE FROM {mt_table} WHERE year = 2020", settings={"mutations_sync": 2})

    node.query(f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table}")
    wait_for_export_status(node, mt_table, s3_table, "2020", "COMPLETED")

    count = int(node.query(f"SELECT count() FROM {s3_table}").strip())
    assert count == 0, f"Expected the destination table to stay empty, got {count} rows"

    exported_files = node.query(
        f"""
        SELECT length(arrayFlatten(mapValues(destination_file_paths)))
        FROM system.partition_exports
        WHERE source_table = '{mt_table}'
          AND destination_table = '{s3_table}'
          AND partition_id = '2020'
        """
    ).strip()
    assert exported_files == "0", (
        f"Expected no files to be written for an entirely deleted partition, got {exported_files}"
    )
