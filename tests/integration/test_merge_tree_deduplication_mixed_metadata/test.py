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
