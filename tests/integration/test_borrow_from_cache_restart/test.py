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

    # The leftover table can still be dropped.
    node.query("DROP TABLE borrowed")
