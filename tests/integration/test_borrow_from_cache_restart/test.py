import os.path

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

CACHE_DISK_CONFIG = os.path.join(os.path.dirname(__file__), "configs/cache_disk.xml")
CACHE_DISK_CONFIG_IN_CONTAINER = "/etc/clickhouse-server/config.d/cache_disk.xml"

node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/base.xml",
        "configs/backups.xml",
        "configs/cache_disk.xml",
    ],
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_borrow_from_cache_atomic_db_creates_no_host_root_symlink(started_cluster):
    # Regression: a `MergeTree` table on a `borrow_from_cache` disk has no real path on the local
    # filesystem -- the `memory` metadata storage returns only the placeholder root ("/") from
    # `getPath()`, so `getDataPaths()` produces a host-looking `/store/...` path built from that
    # placeholder. An `Atomic` database (the default) used to trust that path and create a dangling
    # symlink `data/<db>/<table>` -> `/store/...` pointing into the container's real filesystem
    # root (and `system.tables.data_paths` reported the same bogus path). `tryCreateSymlink` must
    # skip symlink creation for disks that are not on the local filesystem.
    node.query("DROP TABLE IF EXISTS borrowed_symlink SYNC")
    node.query(
        """
        CREATE TABLE borrowed_symlink (key UInt64)
        ENGINE = MergeTree ORDER BY key
        SETTINGS disk = disk(
            type = object_storage,
            object_storage_type = 'borrow_from_cache',
            cache_name = 'borrowed_cache',
            name = 'borrowed_symlink_disk')
        """
    )
    node.query("INSERT INTO borrowed_symlink VALUES (1), (2), (3)")
    assert node.query("SELECT count() FROM borrowed_symlink").strip() == "3"

    # No symlink under the data directory may point into the (non-existent) host filesystem root.
    dangling = node.exec_in_container(
        [
            "bash",
            "-c",
            "find /var/lib/clickhouse/data -maxdepth 3 -xtype l -printf '%p -> %l\\n' || true",
        ]
    ).strip()
    assert (
        dangling == ""
    ), f"borrow_from_cache table created a host-root symlink: {dangling}"

    # The table still drops cleanly (releasing the borrowed cache segments).
    node.query("DROP TABLE borrowed_symlink SYNC")


def test_borrow_from_cache_atomic_db_no_symlink_for_direct_disk_engine(started_cluster):
    # Regression: the `tryCreateSymlink` guard originally only inspected `tryGetStoragePolicy`, which
    # is populated for `MergeTree`-family engines but empty for engines that take a `DiskPtr` directly
    # (`Log`, `StripeLog`, `TinyLog`, `Set`, `Join`). Such a table on a `borrow_from_cache` disk would
    # therefore still fall through and create the same dangling `data/<db>/<table>` -> `/store/...`
    # symlink. `tryCreateSymlink` now consults `IStorage::getDataDisks`, which reports the disk for
    # direct-disk engines too, so no host-root symlink must be created for them either.
    #
    # Log-family engines cannot take an inline `disk(...)` definition (their `disk` setting must be a
    # named disk), so first register the named borrow disk via a throwaway `MergeTree` CREATE, then
    # reference it by name from a `StripeLog` table.
    node.query("DROP TABLE IF EXISTS borrowed_disk_register SYNC")
    node.query("DROP TABLE IF EXISTS borrowed_stripelog SYNC")
    node.query(
        """
        CREATE TABLE borrowed_disk_register (key UInt64)
        ENGINE = MergeTree ORDER BY key
        SETTINGS disk = disk(
            type = object_storage,
            object_storage_type = 'borrow_from_cache',
            cache_name = 'borrowed_cache',
            name = 'borrowed_directdisk')
        """
    )
    node.query(
        "CREATE TABLE borrowed_stripelog (key UInt64) ENGINE = StripeLog SETTINGS disk = 'borrowed_directdisk'"
    )
    node.query("INSERT INTO borrowed_stripelog VALUES (1), (2), (3)")
    assert node.query("SELECT count() FROM borrowed_stripelog").strip() == "3"

    # No symlink under the data directory may point into the (non-existent) host filesystem root.
    dangling = node.exec_in_container(
        [
            "bash",
            "-c",
            "find /var/lib/clickhouse/data -maxdepth 3 -xtype l -printf '%p -> %l\\n' || true",
        ]
    ).strip()
    assert (
        dangling == ""
    ), f"borrow_from_cache direct-disk engine created a host-root symlink: {dangling}"

    node.query("DROP TABLE borrowed_stripelog SYNC")
    node.query("DROP TABLE borrowed_disk_register SYNC")


def test_borrow_from_cache_freeze_and_detach_part(started_cluster):
    # Regression: `FREEZE` and detached-part cloning of a `MergeTree` table on a `borrow_from_cache`
    # disk go through `BackupImpl` with `make_source_readonly`, which calls
    # `IMetadataTransaction::setReadOnly` on each part file. The `memory` metadata backend used to
    # leave `setReadOnly` (and the string metadata read/write helpers) unimplemented, so
    # `ALTER TABLE ... FREEZE`, `DETACH PART`, and broken-part quarantine failed with `NOT_IMPLEMENTED`
    # even though the table itself is supported. These operations must now succeed.
    node.query("DROP TABLE IF EXISTS borrowed_freeze SYNC")
    node.query(
        """
        CREATE TABLE borrowed_freeze (key UInt64)
        ENGINE = MergeTree ORDER BY key
        SETTINGS disk = disk(
            type = object_storage,
            object_storage_type = 'borrow_from_cache',
            cache_name = 'borrowed_cache',
            name = 'borrowed_freeze_disk')
        """
    )
    node.query("INSERT INTO borrowed_freeze VALUES (1), (2), (3)")

    # FREEZE marks each source part file read-only and hardlinks the part into `shadow/`.
    node.query("ALTER TABLE borrowed_freeze FREEZE WITH NAME 'borrow_backup'")

    # Freezing leaves the table itself untouched and still readable.
    assert node.query("SELECT count() FROM borrowed_freeze").strip() == "3"

    # Detaching and re-attaching a part exercises `makeCloneInDetached` (also `make_source_readonly`).
    part = node.query(
        "SELECT name FROM system.parts WHERE table = 'borrowed_freeze' AND active ORDER BY name LIMIT 1"
    ).strip()
    node.query(f"ALTER TABLE borrowed_freeze DETACH PART '{part}'")
    node.query(f"ALTER TABLE borrowed_freeze ATTACH PART '{part}'")
    assert node.query("SELECT count() FROM borrowed_freeze").strip() == "3"

    # Releasing the frozen snapshot and dropping the table both clean up without error.
    node.query("SYSTEM UNFREEZE WITH NAME 'borrow_backup'")
    node.query("DROP TABLE borrowed_freeze SYNC")


def test_borrow_from_cache_no_symlink_for_lazy_loaded_table(started_cluster):
    # Regression: in a database with `lazy_load_tables = 1` every table is attached as a
    # `StorageTableProxy`, which reports `storesDataOnDisk() == true` but deliberately does not
    # expose the nested storage policy. `IStorage::getDataDisks` derives the disks from that policy,
    # so the proxy used to report an empty disk list while `getDataPaths` still materialized the
    # nested `/store/...` path -- the `tryCreateSymlink` guard saw nothing to skip and recreated the
    # dangling `data/<db>/<table>` -> `/store/...` symlink for a `borrow_from_cache` table.
    # `StorageProxy` now forwards `getDataDisks` to the nested storage.
    node.query("DROP DATABASE IF EXISTS lazy_borrow SYNC")
    node.query("CREATE DATABASE lazy_borrow ENGINE = Atomic SETTINGS lazy_load_tables = 1")
    node.query(
        """
        CREATE TABLE lazy_borrow.borrowed_lazy (key UInt64)
        ENGINE = MergeTree ORDER BY key
        SETTINGS disk = disk(
            type = object_storage,
            object_storage_type = 'borrow_from_cache',
            cache_name = 'borrowed_cache',
            name = 'borrowed_lazy_disk')
        """
    )
    node.query("INSERT INTO lazy_borrow.borrowed_lazy VALUES (1), (2), (3)")

    # After a restart the table is attached as a lazy proxy (data itself is node-local cache, so it
    # is legitimately empty again). Renaming it goes through `DatabaseAtomic`'s attach path, which
    # creates the table symlink -- on the proxy, not on the materialized `MergeTree`.
    node.restart_clickhouse()
    assert (
        node.query(
            "SELECT engine FROM system.tables WHERE database = 'lazy_borrow' AND name = 'borrowed_lazy'"
        ).strip()
        == "TableProxy"
    )
    node.query(
        "RENAME TABLE lazy_borrow.borrowed_lazy TO lazy_borrow.borrowed_lazy_renamed"
    )

    dangling = node.exec_in_container(
        [
            "bash",
            "-c",
            "find /var/lib/clickhouse/data -maxdepth 3 -xtype l -printf '%p -> %l\\n' || true",
        ]
    ).strip()
    assert (
        dangling == ""
    ), f"lazy-loaded borrow_from_cache table created a dangling symlink: {dangling}"

    node.query("DROP DATABASE lazy_borrow SYNC")


def test_borrow_from_cache_log_family_writable_after_restart(started_cluster):
    # Regression: the `memory` metadata of a `borrow_from_cache` disk does not survive a restart, so a
    # reattached table has no directory on the disk any more. `MergeTree` recreates its directories
    # when it is attached, but `Log` / `TinyLog` / `StripeLog` only repaired their file checker, so the
    # first `INSERT` into a reattached log-family table failed with
    # `DIRECTORY_DOESNT_EXIST: Cannot create file .../tmp_sizes.json: parent directory does not exist`.
    # The table is legitimately empty after a restart, but it must be writable again.
    #
    # The log family can only reference a disk by name, and a custom DDL disk defined inline by
    # another table is not yet registered when such a table is attached (the attach fails with
    # `UNKNOWN_DISK`), so this uses the configuration-defined `borrowed_cfg` disk.
    for table in ["borrowed_cfg_stripelog", "borrowed_cfg_log", "borrowed_cfg_mt"]:
        node.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node.query(
        "CREATE TABLE borrowed_cfg_stripelog (key UInt64) ENGINE = StripeLog SETTINGS disk = 'borrowed_cfg'"
    )
    node.query(
        "CREATE TABLE borrowed_cfg_log (key UInt64) ENGINE = Log SETTINGS disk = 'borrowed_cfg'"
    )
    node.query(
        "CREATE TABLE borrowed_cfg_mt (key UInt64) ENGINE = MergeTree ORDER BY key SETTINGS disk = 'borrowed_cfg'"
    )
    for table in ["borrowed_cfg_stripelog", "borrowed_cfg_log", "borrowed_cfg_mt"]:
        node.query(f"INSERT INTO {table} VALUES (1), (2), (3)")
        assert node.query(f"SELECT count() FROM {table}").strip() == "3"

    node.restart_clickhouse()

    for table in ["borrowed_cfg_stripelog", "borrowed_cfg_log", "borrowed_cfg_mt"]:
        # The data lived only in the cache, so the reattached table is empty ...
        assert node.query(f"SELECT count() FROM {table}").strip() == "0"
        # ... but writes must work again.
        node.query(f"INSERT INTO {table} VALUES (4), (5)")
        assert node.query(f"SELECT count() FROM {table}").strip() == "2"

    # Drop the tables before the destructive test below removes the cache's configuration file.
    for table in ["borrowed_cfg_stripelog", "borrowed_cfg_log", "borrowed_cfg_mt"]:
        node.query(f"DROP TABLE {table} SYNC")


def test_borrow_from_cache_restart_with_absent_cache(started_cluster):
    # A `borrow_from_cache` table stores its data only in node-local cache segments, so its data
    # does not survive a restart. The named cache is registered by a *separate* disk, and on restart
    # the borrow disk can be reconstructed before that cache exists (or it may have been dropped).
    # The server must still start and bring the (necessarily empty) table up, instead of aborting
    # metadata loading with `There is no cache by name ...`.
    node.query(
        """
        CREATE TABLE borrowed (key UInt64)
        ENGINE = MergeTree ORDER BY key
        SETTINGS disk = disk(
            type = object_storage,
            object_storage_type = 'borrow_from_cache',
            cache_name = 'borrowed_cache',
            name = 'borrowed_disk')
        """
    )
    node.query("INSERT INTO borrowed VALUES (1), (2), (3)")
    assert node.query("SELECT count() FROM borrowed").strip() == "3"

    # A fresh CREATE referencing a non-existent cache must still fail loudly (only ATTACH tolerates it).
    assert "BAD_ARGUMENTS" in node.query_and_get_error(
        """
        CREATE TABLE bad (key UInt64)
        ENGINE = MergeTree ORDER BY key
        SETTINGS disk = disk(
            type = object_storage,
            object_storage_type = 'borrow_from_cache',
            cache_name = 'does_not_exist',
            name = 'bad_disk')
        """
    )

    # The configuration-defined `borrowed_cfg` disk (in the persistent base.xml) stays configured
    # while the cache it references disappears. `DiskSelector` creates configuration disks with
    # `attach = false` even at startup, so without the startup leniency in the borrow_from_cache
    # factory the server would abort on the missing cache instead of bringing the disk up with data
    # writes blocked. Keep a table on it across the restart to cover that path too.
    node.query(
        "CREATE TABLE borrowed_cfg_survivor (key UInt64) ENGINE = StripeLog SETTINGS disk = 'borrowed_cfg'"
    )
    node.query("INSERT INTO borrowed_cfg_survivor VALUES (1), (2)")
    assert node.query("SELECT count() FROM borrowed_cfg_survivor").strip() == "2"

    # Remove the cache-defining disk so the cache is not registered after the restart.
    node.exec_in_container(
        ["bash", "-c", "rm /etc/clickhouse-server/config.d/cache_disk.xml"]
    )
    node.restart_clickhouse()

    # The server came back up (the regression aborted startup here) and the table is empty.
    assert node.query("SELECT 1").strip() == "1"
    assert node.query("SELECT count() FROM borrowed").strip() == "0"

    # The configuration-defined disk was registered despite the missing cache: the server started,
    # the disk is up with data writes blocked, and the table on it is attached and readable (empty).
    assert (
        node.query(
            "SELECT is_read_only FROM system.disks WHERE name = 'borrowed_cfg'"
        ).strip()
        == "1"
    )
    assert node.query("SELECT count() FROM borrowed_cfg_survivor").strip() == "0"
    # Writing data is rejected while the cache is absent.
    assert "read-only" in node.query_and_get_error(
        "INSERT INTO borrowed_cfg_survivor VALUES (3)"
    )
    node.query("DROP TABLE borrowed_cfg_survivor SYNC")

    # While the cache is absent the disk is read-only, so writes are rejected with a clear error
    # rather than crashing or silently succeeding.
    assert node.query(
        "SELECT is_read_only FROM system.disks WHERE name = 'borrowed_disk'"
    ).strip() == "1"
    assert "READ_ONLY" in node.query_and_get_error("INSERT INTO borrowed VALUES (4)")

    # A *new* CREATE that reuses the already-registered, read-only `borrowed_disk` (the same inline
    # `disk(...)` definition, so `getOrCreateDisk` returns the existing disk without re-validating
    # its now-absent cache) is NOT rejected up front. `MergeTreeData::initializeDirectoriesAndFormatVersion`
    # skips directory creation and the `format_version.txt` write on a read-only disk, so the empty
    # table is created and the failure is deferred to the first `INSERT`. This pins the documented
    # "Restart robustness" behavior in `docs/en/operations/storing-data.md`.
    node.query(
        """
        CREATE TABLE borrowed_reuse (key UInt64)
        ENGINE = MergeTree ORDER BY key
        SETTINGS disk = disk(
            type = object_storage,
            object_storage_type = 'borrow_from_cache',
            cache_name = 'borrowed_cache',
            name = 'borrowed_disk')
        """
    )
    assert node.query("SELECT count() FROM borrowed_reuse").strip() == "0"
    assert "READ_ONLY" in node.query_and_get_error(
        "INSERT INTO borrowed_reuse VALUES (7)"
    )
    node.query("DROP TABLE borrowed_reuse")

    # Conversely, registering a *fresh* borrow disk (a name not seen before) does re-validate the
    # cache, so it is rejected outright while the cache is absent -- the immediate-failure guarantee
    # only holds for disk creation, not for reuse of an already read-only disk.
    assert "BAD_ARGUMENTS" in node.query_and_get_error(
        """
        CREATE TABLE borrowed_fresh (key UInt64)
        ENGINE = MergeTree ORDER BY key
        SETTINGS disk = disk(
            type = object_storage,
            object_storage_type = 'borrow_from_cache',
            cache_name = 'borrowed_cache',
            name = 'brand_new_disk')
        """
    )

    # Referencing the custom disk by bare name (rather than its full inline definition) is rejected
    # outright: custom DDL disks cannot be shared between tables by name, so this path cannot create
    # a table on the read-only disk either.
    assert "BAD_ARGUMENTS" in node.query_and_get_error(
        "CREATE TABLE borrowed_byname (key UInt64) ENGINE = MergeTree ORDER BY key "
        "SETTINGS disk = 'borrowed_disk'"
    )

    # The leftover table can still be dropped.
    node.query("DROP TABLE borrowed")


def test_borrow_from_cache_restore_into_table_attached_while_cache_was_absent(
    started_cluster,
):
    # Regression: `Log` / `TinyLog` / `StripeLog` recreate their missing table directory before the
    # first `INSERT` (see `createTableDirectoryIfNeeded`), but `restoreDataImpl` skipped that step,
    # so `RESTORE` into a table that attached while the cache was absent -- and whose directory
    # therefore could not be recreated at attach time -- failed with `DIRECTORY_DOESNT_EXIST` until
    # a regular `INSERT` happened first.
    #
    # The previous test leaves the cache configuration removed; bring it back first.
    node.copy_file_to_container(CACHE_DISK_CONFIG, CACHE_DISK_CONFIG_IN_CONTAINER)
    node.restart_clickhouse()

    for table in ["borrowed_restore_log", "borrowed_restore_stripelog"]:
        node.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node.query(
        "CREATE TABLE borrowed_restore_log (key UInt64) ENGINE = Log SETTINGS disk = 'borrowed_cfg'"
    )
    node.query(
        "CREATE TABLE borrowed_restore_stripelog (key UInt64) ENGINE = StripeLog SETTINGS disk = 'borrowed_cfg'"
    )
    node.query("INSERT INTO borrowed_restore_log VALUES (1), (2), (3)")
    node.query("INSERT INTO borrowed_restore_stripelog VALUES (1), (2), (3)")
    node.query(
        "BACKUP TABLE borrowed_restore_log, TABLE borrowed_restore_stripelog "
        "TO File('/backups/restore_after_read_only_attach/')"
    )

    # Remove the cache and restart: the tables reattach (empty) while the disk is read-only, so
    # their directories cannot be recreated at attach time.
    node.exec_in_container(["bash", "-c", f"rm {CACHE_DISK_CONFIG_IN_CONTAINER}"])
    node.restart_clickhouse()
    assert (
        node.query(
            "SELECT is_read_only FROM system.disks WHERE name = 'borrowed_cfg'"
        ).strip()
        == "1"
    )

    # Bring the cache back at runtime: registering the cache makes the borrow disk writable again,
    # but the table directories are still missing.
    node.copy_file_to_container(CACHE_DISK_CONFIG, CACHE_DISK_CONFIG_IN_CONTAINER)
    node.query("SYSTEM RELOAD CONFIG")
    assert (
        node.query(
            "SELECT is_read_only FROM system.disks WHERE name = 'borrowed_cfg'"
        ).strip()
        == "0"
    )

    # RESTORE appends into the existing (empty) tables, so it must recreate the directories too.
    node.query(
        "RESTORE TABLE borrowed_restore_log, TABLE borrowed_restore_stripelog "
        "FROM File('/backups/restore_after_read_only_attach/')"
    )
    assert node.query("SELECT count() FROM borrowed_restore_log").strip() == "3"
    assert node.query("SELECT count() FROM borrowed_restore_stripelog").strip() == "3"

    for table in ["borrowed_restore_log", "borrowed_restore_stripelog"]:
        node.query(f"DROP TABLE {table} SYNC")


def test_borrow_from_cache_rename_of_table_attached_while_cache_was_absent(
    started_cluster,
):
    # Regression: `write()` and `RESTORE` recreate a missing table directory lazily (see
    # `createTableDirectoryIfNeeded`), but `RENAME TABLE` went straight to `moveDirectory`. For a
    # `Log` / `TinyLog` / `StripeLog` table that attached while its `borrow_from_cache` disk was
    # read-only, the first rename after the cache reappeared -- before any write had recreated the
    # directory -- failed with `DIRECTORY_DOESNT_EXIST`. A missing directory means an empty table,
    # so there is nothing to move: the rename just adopts the new path, and the directory is
    # created there before the first write.
    #
    # Only an `Ordinary` database moves data on `RENAME TABLE` (`Atomic` keeps UUID-based paths
    # that a rename does not change), so the tables live in a dedicated `Ordinary` database.
    node.copy_file_to_container(CACHE_DISK_CONFIG, CACHE_DISK_CONFIG_IN_CONTAINER)
    node.restart_clickhouse()

    node.query("DROP DATABASE IF EXISTS borrow_rename_ord")
    node.query(
        "CREATE DATABASE borrow_rename_ord ENGINE = Ordinary",
        settings={"allow_deprecated_database_ordinary": 1},
    )
    node.query(
        "CREATE TABLE borrow_rename_ord.renamed_log (key UInt64) ENGINE = Log "
        "SETTINGS disk = 'borrowed_cfg'"
    )
    node.query(
        "CREATE TABLE borrow_rename_ord.renamed_stripelog (key UInt64) ENGINE = StripeLog "
        "SETTINGS disk = 'borrowed_cfg'"
    )
    node.query("INSERT INTO borrow_rename_ord.renamed_log VALUES (1)")
    node.query("INSERT INTO borrow_rename_ord.renamed_stripelog VALUES (1)")

    # Remove the cache and restart: the tables reattach (empty) while the disk is read-only, so
    # their directories cannot be recreated at attach time.
    node.exec_in_container(["bash", "-c", f"rm {CACHE_DISK_CONFIG_IN_CONTAINER}"])
    node.restart_clickhouse()
    assert (
        node.query(
            "SELECT is_read_only FROM system.disks WHERE name = 'borrowed_cfg'"
        ).strip()
        == "1"
    )

    # Bring the cache back at runtime: the disk becomes writable again, but the table directories
    # are still missing because no write has happened yet.
    node.copy_file_to_container(CACHE_DISK_CONFIG, CACHE_DISK_CONFIG_IN_CONTAINER)
    node.query("SYSTEM RELOAD CONFIG")
    assert (
        node.query(
            "SELECT is_read_only FROM system.disks WHERE name = 'borrowed_cfg'"
        ).strip()
        == "0"
    )

    # Rename before the first post-restart INSERT; the renamed tables must then accept writes.
    node.query(
        "RENAME TABLE borrow_rename_ord.renamed_log TO borrow_rename_ord.renamed_log2"
    )
    node.query(
        "RENAME TABLE borrow_rename_ord.renamed_stripelog TO borrow_rename_ord.renamed_stripelog2"
    )
    node.query("INSERT INTO borrow_rename_ord.renamed_log2 VALUES (10), (20)")
    node.query("INSERT INTO borrow_rename_ord.renamed_stripelog2 VALUES (10), (20)")
    assert (
        node.query("SELECT count() FROM borrow_rename_ord.renamed_log2").strip() == "2"
    )
    assert (
        node.query("SELECT count() FROM borrow_rename_ord.renamed_stripelog2").strip()
        == "2"
    )

    node.query("DROP DATABASE borrow_rename_ord")


def test_borrow_from_cache_set_join_failed_insert_leaves_no_rows_in_memory(
    started_cluster,
):
    # Regression: the sink shared by persistent `Set` / `Join` tables inserted the block into the
    # in-memory state before opening the persistent backup file. When the disk rejects the write
    # (here: a `borrow_from_cache` disk whose cache is not registered), the `INSERT` failed but the
    # rows stayed visible in memory until restart. A failed `INSERT` must leave no trace.
    #
    # Make sure the cache configuration is in place regardless of what ran before.
    node.copy_file_to_container(CACHE_DISK_CONFIG, CACHE_DISK_CONFIG_IN_CONTAINER)
    node.restart_clickhouse()

    for table in ["borrowed_set", "borrowed_join"]:
        node.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node.query(
        "CREATE TABLE borrowed_set (key UInt64) ENGINE = Set "
        "SETTINGS disk = 'borrowed_cfg', persistent = 1"
    )
    node.query(
        "CREATE TABLE borrowed_join (key UInt64, value String) ENGINE = Join(ANY, LEFT, key) "
        "SETTINGS disk = 'borrowed_cfg', persistent = 1"
    )
    node.query("INSERT INTO borrowed_set VALUES (1)")
    node.query("INSERT INTO borrowed_join VALUES (1, 'a')")
    assert (
        node.query(
            "SELECT count() FROM numbers(10) WHERE number IN borrowed_set"
        ).strip()
        == "1"
    )
    assert node.query("SELECT count() FROM borrowed_join").strip() == "1"

    # Remove the cache and restart: the tables reattach empty and the disk is read-only.
    node.exec_in_container(["bash", "-c", f"rm {CACHE_DISK_CONFIG_IN_CONTAINER}"])
    node.restart_clickhouse()
    assert (
        node.query(
            "SELECT count() FROM numbers(10) WHERE number IN borrowed_set"
        ).strip()
        == "0"
    )
    assert node.query("SELECT count() FROM borrowed_join").strip() == "0"

    # The INSERT fails because the backup file cannot be written, and the rows must not remain
    # visible in the in-memory state of the table.
    assert "read-only" in node.query_and_get_error(
        "INSERT INTO borrowed_set VALUES (7)"
    )
    assert (
        node.query(
            "SELECT count() FROM numbers(10) WHERE number IN borrowed_set"
        ).strip()
        == "0"
    )
    assert "read-only" in node.query_and_get_error(
        "INSERT INTO borrowed_join VALUES (7, 'x')"
    )
    assert node.query("SELECT count() FROM borrowed_join").strip() == "0"

    for table in ["borrowed_set", "borrowed_join"]:
        node.query(f"DROP TABLE {table} SYNC")

    # Restore the cache configuration so the module ends in the same state it started in.
    node.copy_file_to_container(CACHE_DISK_CONFIG, CACHE_DISK_CONFIG_IN_CONTAINER)
