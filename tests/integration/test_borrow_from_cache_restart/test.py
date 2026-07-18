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
