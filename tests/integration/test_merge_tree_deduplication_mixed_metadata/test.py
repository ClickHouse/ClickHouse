import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/storage_conf.xml"],
    with_minio=True,
    stay_alive=True,
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_deduplication_with_mixed_metadata_storage_policy(start_cluster):
    # Exercises the storage policy shape from
    # https://github.com/ClickHouse/ClickHouse/issues/86189: the deduplication log
    # always lives on the first disk of the policy, which here has
    # `metadata_type=plain` and does not support writing with append. Data parts are
    # routed to the second (`local`-metadata) disk, since the plain disk cannot host
    # them. This used to be associated with the logical error 'current_writer != nullptr';
    # the test verifies that INSERT with deduplication works and that the deduplication
    # log is opened using a write mode appropriate for the disk capabilities.
    node.query("DROP TABLE IF EXISTS t0 SYNC")
    node.query(
        """
        CREATE TABLE t0 (c0 Int) ENGINE = MergeTree() ORDER BY tuple()
        SETTINGS non_replicated_deduplication_window = 2, storage_policy = 'mixed_metadata_policy'
        """
    )

    # This INSERT used to fail with Logical error: 'current_writer != nullptr'.
    node.query("INSERT INTO t0 (c0) VALUES (1)")
    # The same block is deduplicated.
    node.query("INSERT INTO t0 (c0) VALUES (1)")
    # A different block is inserted.
    node.query("INSERT INTO t0 (c0) VALUES (2)")

    assert node.query("SELECT count() FROM t0") == "2\n"
    assert node.query("SELECT sum(c0) FROM t0") == "3\n"

    node.query("DROP TABLE t0 SYNC")


def test_deduplication_drop_part_with_mixed_metadata_storage_policy(start_cluster):
    # Companion to the test above, covering the `dropPart()` half of the same fix:
    # `MergeTreeDeduplicationLog::dropPart` used to assert `current_writer != nullptr`
    # too, and https://github.com/ClickHouse/ClickHouse/issues/86189 reports the
    # assertion from that path as well.
    #
    # `TRUNCATE` is used to reach `dropPart` because on this storage policy the other
    # routes to it are rejected before they get there: partition operations are not
    # supported for a `plain`-metadata disk, and neither are mutations (which is how
    # `clearEmptyParts` would otherwise be triggered).
    node.query("DROP TABLE IF EXISTS t1 SYNC")
    node.query(
        """
        CREATE TABLE t1 (c0 Int) ENGINE = MergeTree() ORDER BY tuple()
        SETTINGS non_replicated_deduplication_window = 2, storage_policy = 'mixed_metadata_policy'
        """
    )

    node.query("INSERT INTO t1 (c0) VALUES (1)")
    node.query("INSERT INTO t1 (c0) VALUES (2)")
    assert node.query("SELECT count() FROM t1") == "2\n"

    # Restart so that the deduplication log is reopened from scratch and a drop, not an
    # insert, is the first operation writing to it.
    node.restart_clickhouse()

    # This used to fail with Logical error: 'current_writer != nullptr'.
    node.query("TRUNCATE TABLE t1")
    assert node.query("SELECT count() FROM t1") == "0\n"

    # `dropPart` wrote the drop records, so the previously seen blocks are no longer
    # deduplicated away and can be inserted again.
    node.query("INSERT INTO t1 (c0) VALUES (1)")
    node.query("INSERT INTO t1 (c0) VALUES (2)")
    assert node.query("SELECT count() FROM t1") == "2\n"
    assert node.query("SELECT sum(c0) FROM t1") == "3\n"

    # Deduplication still works after the drop.
    node.query("INSERT INTO t1 (c0) VALUES (1)")
    assert node.query("SELECT count() FROM t1") == "2\n"

    node.query("DROP TABLE t1 SYNC")
