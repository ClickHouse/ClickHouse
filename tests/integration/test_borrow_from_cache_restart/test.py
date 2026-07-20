import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/base.xml",
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
            "find /var/lib/clickhouse/data -maxdepth 3 -type l -lname '/store/*' -print || true",
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
            "find /var/lib/clickhouse/data -maxdepth 3 -type l -lname '/store/*' -print || true",
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

    # Remove the cache-defining disk so the cache is not registered after the restart.
    node.exec_in_container(
        ["bash", "-c", "rm /etc/clickhouse-server/config.d/cache_disk.xml"]
    )
    node.restart_clickhouse()

    # The server came back up (the regression aborted startup here) and the table is empty.
    assert node.query("SELECT 1").strip() == "1"
    assert node.query("SELECT count() FROM borrowed").strip() == "0"

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
