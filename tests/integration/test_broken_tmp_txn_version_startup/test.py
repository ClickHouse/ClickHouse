import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/storage_conf.xml", "configs/transactions.xml"],
    with_zookeeper=True,
    stay_alive=True,
    # `ATTACH TABLE ... AS REPLICATED` expands the `{shard}` and `{replica}` macros in the
    # default replica path/name (`/clickhouse/tables/{uuid}/{shard}`), so they must be defined.
    macros={"shard": "s1", "replica": "r1"},
    # Transactions refuse to start unless Keeper advertises these.
    keeper_required_feature_flags=[
        "filtered_list",
        "multi_read",
        "list_with_stat_and_data",
        "check_stat",
    ],
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_broken_tmp_txn_version_file_does_not_prevent_startup(started_cluster):
    # A leftover `txn_version.txt.tmp` on a part is an artifact of an interrupted write, so both its
    # content and - on an object storage disk - even the local metadata file describing it can be
    # arbitrary garbage. Loading a part that carries a committed `txn_version.txt` must clean the
    # leftover up (`VersionMetadataOnDisk` documents its content as expendable) instead of failing
    # the table load and with it the server startup: the diagnostic dump of the file in
    # `removeTmpMetadataFile` used to call `getFileSize`, which for an object storage disk
    # deserializes the broken metadata file and throws.
    node.query("DROP TABLE IF EXISTS t_broken_tmp_txn SYNC")
    node.query(
        "CREATE TABLE t_broken_tmp_txn (n Int64) ENGINE = MergeTree ORDER BY n"
        " SETTINGS storage_policy = 'local_object'"
    )
    # A transactional insert persists `txn_version.txt` on the part, so the part is committed and
    # must survive the cleanup of the broken temporary file.
    node.query(
        "INSERT INTO t_broken_tmp_txn VALUES (42)",
        settings={"implicit_transaction": 1, "async_insert": 0},
    )

    part_path = node.query(
        "SELECT path FROM system.parts"
        " WHERE database = 'default' AND table = 't_broken_tmp_txn' AND active LIMIT 1"
    ).strip()
    assert part_path

    ls = node.exec_in_container(["bash", "-c", f"ls {part_path}"])
    assert "txn_version.txt" in ls

    # For a `metadata_type = local` object storage disk, `system.parts.path` is the directory with
    # the metadata files, so the raw write creates a `txn_version.txt.tmp` metadata file that is not
    # even deserializable - like after a hard restart in the middle of writing it.
    node.exec_in_container(
        ["bash", "-c", f"echo incomplete > {part_path}txn_version.txt.tmp"]
    )

    node.restart_clickhouse(kill=True)

    # The server must start, the table must load, and the committed data must survive.
    assert node.query("SELECT count() FROM t_broken_tmp_txn").strip() == "1"
    assert node.query("SELECT n FROM t_broken_tmp_txn").strip() == "42"

    # The leftover file was cleaned up during loading.
    ls = node.exec_in_container(["bash", "-c", f"ls {part_path}"])
    assert "txn_version.txt.tmp" not in ls

    node.query("DROP TABLE t_broken_tmp_txn SYNC")


def test_dummy_tid_load_with_stale_tmp_txn_version(started_cluster):
    # Converted from stateless test 04104_transaction_version_metadata_dummy_tid_load.sh.
    #
    # Regression test for https://github.com/ClickHouse/ClickHouse/pull/92141
    # (STID 3547-447e):
    #
    # When a part on disk has `txn_version.txt.tmp` but no `txn_version.txt` (an
    # incomplete write that was not atomically renamed), `VersionMetadataOnDisk::loadMetadata`
    # returns a `VersionInfo` with `creation_tid = Tx::DummyTID` and
    # `creation_csn = Tx::RolledBackCSN`. `DummyTID` has `start_csn == NonTransactionalCSN`
    # but `local_tid == DummyLocalTID`, which must not trip the assertion inside
    # `TransactionID::isNonTransactional` called by `VersionMetadata::validateInfo` and
    # `VersionInfo::wasInvolvedInTransaction`. Before the fix, the server aborted
    # with signal 6 during part loading in debug and sanitizer builds.
    #
    # This test creates a part with no `txn_version.txt` (deferred persist is the
    # default for non-transactional inserts), drops a bogus `txn_version.txt.tmp`
    # into the part directory, and runs `DETACH` + `ATTACH`. The server must stay
    # alive and the rolled-back part must be invisible.
    #
    # The stateless original triggered part loading via `DETACH` + `ATTACH` and asserted the
    # server stays alive; the same trigger is kept here (no server restart), it exercises
    # exactly the `VersionMetadataOnDisk::loadMetadata` path under test. The original also
    # ran the client with `send_logs_level=error` only to suppress the expected
    # `removeTmpMetadataFile` warning in the stateless runner; that is not needed here.
    node.query("DROP TABLE IF EXISTS t_txn_tmp_leftover SYNC")
    node.query(
        "CREATE TABLE t_txn_tmp_leftover (n Int64) ENGINE = MergeTree ORDER BY n"
    )
    node.query("INSERT INTO t_txn_tmp_leftover VALUES (42)")

    part_path = node.query(
        "SELECT path FROM system.parts"
        " WHERE database = 'default' AND table = 't_txn_tmp_leftover' AND active LIMIT 1"
    ).strip()
    assert part_path

    # Simulate an incomplete write: drop a bogus `txn_version.txt.tmp` file into the
    # part directory while keeping `txn_version.txt` absent.
    node.exec_in_container(
        ["bash", "-c", f"echo incomplete > {part_path}txn_version.txt.tmp"],
        privileged=True,
        user="root",
    )

    # DETACH + ATTACH triggers `VersionMetadataOnDisk::loadMetadata` on the stale tmp
    # file. Before the fix this aborted the server with signal 6 (assertion inside
    # `TransactionID::isNonTransactional`). With the fix the part is loaded with
    # `creation_tid == Tx::DummyTID` / `creation_csn == Tx::RolledBackCSN`, marked
    # `Outdated` by `MergeTreeData::loadDataPart`, and safely cleaned up on `DROP`.
    node.query("DETACH TABLE t_txn_tmp_leftover")
    node.query("ATTACH TABLE t_txn_tmp_leftover")

    # `SELECT count()` is 0 because the rolled-back part is inactive.
    assert (
        node.query("SELECT 'select_ok', count() FROM t_txn_tmp_leftover")
        == "select_ok\t0\n"
    )

    # The rolled-back part is present in `system.parts` with `active = 0`,
    # `is_dummy_tid = 1`, `is_rolled_back_csn = 1`.
    assert (
        node.query(
            "SELECT"
            " 'rolled_back_part',"
            " active,"
            # Compare the TID components rather than the whole tuple: downstream builds
            # extend the `creation_tid` tuple with extra elements, and comparing tuples
            # of different sizes throws `ILLEGAL_TYPE_OF_ARGUMENT`.
            " creation_tid.1 = 1 AND creation_tid.2 = 2"
            " AND creation_tid.3 = toUUID('00000000-0000-0000-0000-000000000000')"
            " AS is_dummy_tid,"
            " creation_csn = 18446744073709551615 AS is_rolled_back_csn"
            " FROM system.parts"
            " WHERE database = 'default'"
            " AND table = 't_txn_tmp_leftover'"
            " AND creation_csn = 18446744073709551615"
        )
        == "rolled_back_part\t0\t1\t1\n"
    )

    # `DROP TABLE` completes — the fix in `VersionMetadata::hasValidMetadata`
    # lets the part be removed without trying to read the missing on-disk metadata.
    node.query("DROP TABLE t_txn_tmp_leftover SYNC")


def test_attach_as_replicated_clears_tmp_txn_version(started_cluster):
    # Converted from stateless test 04492_attach_as_replicated_clears_tmp_txn_version.sh.
    #
    # Regression test for ATTACH TABLE ... AS REPLICATED discarding committed data when a stale
    # `txn_version.txt.tmp` is left on a part.
    #
    # `InterpreterCreateQuery::clearTransactionMetadata` (run by ATTACH AS REPLICATED) used to
    # remove only `txn_version.txt`, not `txn_version.txt.tmp`. A `.tmp` file can legitimately
    # linger on a part (for example, hardlinked onto a mutated part from its source part during a
    # merge/mutation race on object storage). With `txn_version.txt` gone but the `.tmp` present,
    # `VersionMetadataOnDisk::loadMetadata` treats the part as a rolled-back transaction
    # (`creation_csn == Tx::RolledBackCSN`), so it is marked `Outdated` and discarded. The
    # committed data is then lost - here the whole table would wrongly become empty.
    #
    # `ATTACH ... AS REPLICATED` derives the ZooKeeper path from `default_replica_path`
    # (`/clickhouse/tables/{uuid}/{shard}`), so the path is unique per table UUID.
    node.query("DROP TABLE IF EXISTS t_attach_repl_tmp_txn SYNC")
    node.query(
        "CREATE TABLE t_attach_repl_tmp_txn (n Int64) ENGINE = MergeTree ORDER BY n"
    )
    node.query("INSERT INTO t_attach_repl_tmp_txn VALUES (42)")

    part_path = node.query(
        "SELECT path FROM system.parts"
        " WHERE database = 'default' AND table = 't_attach_repl_tmp_txn' AND active LIMIT 1"
    ).strip()
    assert part_path

    # Simulate a leftover temporary version file: drop a `txn_version.txt.tmp` into the part
    # directory while keeping `txn_version.txt` absent.
    node.exec_in_container(
        ["bash", "-c", f"echo incomplete > {part_path}txn_version.txt.tmp"],
        privileged=True,
        user="root",
    )

    # ATTACH AS REPLICATED must strip the leftover tmp file together with `txn_version.txt`, so
    # the part loads as plain committed data and the row is preserved (count 1, not 0).
    node.query("DETACH TABLE t_attach_repl_tmp_txn SYNC")
    node.query("ATTACH TABLE t_attach_repl_tmp_txn AS REPLICATED")

    assert (
        node.query("SELECT 'count_after_attach', count() FROM t_attach_repl_tmp_txn")
        == "count_after_attach\t1\n"
    )
    assert (
        node.query("SELECT 'value_after_attach', n FROM t_attach_repl_tmp_txn ORDER BY n")
        == "value_after_attach\t42\n"
    )

    node.query("DETACH TABLE t_attach_repl_tmp_txn SYNC")
    node.query("ATTACH TABLE t_attach_repl_tmp_txn AS NOT REPLICATED")

    node.query("DROP TABLE t_attach_repl_tmp_txn SYNC")


def test_attach_part_clears_tmp_txn_version(started_cluster):
    # Converted from stateless test 04493_attach_part_clears_tmp_txn_version.sh.
    #
    # Regression test for ALTER TABLE ... ATTACH PART discarding committed data when a stale
    # `txn_version.txt.tmp` is left on the attached part.
    #
    # ATTACH PART strips transaction metadata before reloading the part
    # (`MergeTreeData::loadPartAndFixMetadataImpl` -> `IMergeTreeDataPart::removeVersionMetadata`).
    # It used to remove only `txn_version.txt`, not `txn_version.txt.tmp`. A `.tmp` file can
    # legitimately linger on a part (for example, hardlinked onto a mutated part from its source
    # part during a merge/mutation race on object storage). With the main file stripped but the
    # `.tmp` left behind, the next full table load (`MergeTreeData::loadDataPart` ->
    # `VersionMetadataOnDisk::loadMetadata`) treats the part as a rolled-back transaction
    # (`creation_csn == Tx::RolledBackCSN`), marks it `Outdated` and discards it, so the committed
    # row would be lost.
    node.query("DROP TABLE IF EXISTS t_attach_part_tmp_txn SYNC")
    node.query(
        "CREATE TABLE t_attach_part_tmp_txn (n Int64) ENGINE = MergeTree ORDER BY n"
    )
    node.query("INSERT INTO t_attach_part_tmp_txn VALUES (42)")

    part_name = node.query(
        "SELECT name FROM system.parts"
        " WHERE database = 'default' AND table = 't_attach_part_tmp_txn' AND active LIMIT 1"
    ).strip()
    assert part_name

    # Detach the part so its on-disk directory can be tampered with.
    node.query(f"ALTER TABLE t_attach_part_tmp_txn DETACH PART '{part_name}'")

    # Read the detached directory name and path directly, without assuming the exact naming.
    detached = node.query(
        "SELECT name, path FROM system.detached_parts"
        " WHERE database = 'default' AND table = 't_attach_part_tmp_txn' LIMIT 1"
    ).strip()
    assert detached
    detached_name, detached_path = detached.split("\t")

    # Simulate a leftover temporary version file on the detached part.
    node.exec_in_container(
        ["bash", "-c", f"echo incomplete > {detached_path}/txn_version.txt.tmp"],
        privileged=True,
        user="root",
    )

    # ATTACH PART must strip the leftover tmp file, so a later full reload of the table keeps the
    # part active and the committed row survives (count 1, not 0).
    node.query(f"ALTER TABLE t_attach_part_tmp_txn ATTACH PART '{detached_name}'")

    # Force a full reload from disk, which is where a leftover tmp file would resurrect as a
    # rolled-back transaction and discard the part.
    node.query("DETACH TABLE t_attach_part_tmp_txn SYNC")
    node.query("ATTACH TABLE t_attach_part_tmp_txn")

    assert (
        node.query("SELECT 'count_after_attach', count() FROM t_attach_part_tmp_txn")
        == "count_after_attach\t1\n"
    )
    assert (
        node.query("SELECT 'value_after_attach', n FROM t_attach_part_tmp_txn ORDER BY n")
        == "value_after_attach\t42\n"
    )

    node.query("DROP TABLE t_attach_part_tmp_txn SYNC")


def test_packed_part_stale_txn_version_guard(started_cluster):
    # Converted from stateless test 04507_packed_part_stale_txn_version_guard.sh.
    #
    # Regression test for packed part storage losing the stale `txn_version.txt.tmp` guard.
    #
    # When a transaction stores version metadata, `VersionMetadataOnDisk::storeInfoToDataPartStorage`
    # calls `createFile` for `txn_version.txt.tmp` first, relying on it to throw if a stale tmp file
    # is already there (a leftover from a crashed write). For full part storage `createFile` maps to
    # `DiskLocal::createFile` and throws `CANNOT_CREATE_FILE`. For packed storage `createFile` used
    # to be a silent no-op, so the stale tmp was overwritten and the guard was lost. This test
    # forces packed storage, plants a stale tmp file, then triggers a version-metadata store via
    # part removal: the removal must be rejected by the guard, exactly as it is for full storage.
    node.query("DROP TABLE IF EXISTS packed_stale_tmp SYNC")

    # min_bytes_for_full_part_storage forces packed storage (the whole part in a single data.packed).
    node.query(
        "CREATE TABLE packed_stale_tmp (a UInt64) ENGINE = MergeTree ORDER BY a"
        " SETTINGS min_bytes_for_full_part_storage = '1G', min_bytes_for_wide_part = 0,"
        " old_parts_lifetime = 100000"
    )

    # Create the part inside a transaction so txn_version.txt is persisted on disk (not deferred).
    # The statements share one clickhouse-client invocation, i.e. one session, as a transaction
    # requires. Transactions reject async inserts, so async_insert is pinned off.
    node.query(
        "BEGIN TRANSACTION; INSERT INTO packed_stale_tmp VALUES (1); COMMIT;",
        settings={"async_insert": 0},
    )

    # Sanity check: the part must actually be packed for this test to be meaningful.
    assert (
        node.query(
            "SELECT part_storage_type FROM system.parts"
            " WHERE database = 'default' AND table = 'packed_stale_tmp' AND active"
        )
        == "Packed\n"
    )

    part_path = node.query(
        "SELECT path FROM system.parts"
        " WHERE database = 'default' AND table = 'packed_stale_tmp' AND active"
    ).strip()
    assert part_path

    # Plant a stale temporary version file, as a crashed metadata write would leave behind.
    node.exec_in_container(
        ["bash", "-c", f"echo incomplete > {part_path}txn_version.txt.tmp"],
        privileged=True,
        user="root",
    )

    # Removing the part stores a removal TID, which goes through the createFile stale-tmp guard.
    # With the stale tmp present the removal must be rejected (as it is for full storage), not
    # silently accepted.
    error = node.query_and_get_error(
        "ALTER TABLE packed_stale_tmp DROP PART 'all_1_1_0'"
    )
    assert "File exists" in error

    # The rejected removal must leave the part and its row intact.
    assert (
        node.query(
            "SELECT count() FROM system.parts"
            " WHERE database = 'default' AND table = 'packed_stale_tmp' AND active"
        )
        == "1\n"
    )
    assert node.query("SELECT count() FROM packed_stale_tmp") == "1\n"

    node.query("DROP TABLE packed_stale_tmp SYNC")
