import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

main_node = cluster.add_instance(
    "main_node",
    main_configs=["configs/config.xml"],
    user_configs=["configs/settings.xml"],
    with_zookeeper=True,
    stay_alive=True,
    macros={"shard": 1, "replica": 1},
)
dummy_node = cluster.add_instance(
    "dummy_node",
    main_configs=["configs/config.xml"],
    user_configs=["configs/settings.xml"],
    with_zookeeper=True,
    stay_alive=True,
    macros={"shard": 1, "replica": 2},
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def _comment(node, db):
    return node.query(
        f"SELECT comment FROM system.tables WHERE database = '{db}' AND name = 't'"
    )


def test_alter_comment_not_published_before_commit(started_cluster):
    # A Replicated database ALTER whose coordinator transaction commits but whose local metadata
    # rename then fails must NOT leave the replica's in-memory metadata ahead of its durable .sql.
    # WP4 reorders the ALTER so settings/comment are published only after the commit, so a failure
    # before the rename is a clean "lagging replica" (in-memory == .sql, both old) rather than the
    # in-memory-ahead-of-disk divergence behind the metadata-clobber family of issues.
    db = "alter_converge"
    main_node.query(
        f"CREATE DATABASE {db} ENGINE = Replicated('/test/{db}', 'shard1', 'replica1')"
    )
    dummy_node.query(
        f"CREATE DATABASE {db} ENGINE = Replicated('/test/{db}', 'shard1', 'replica2')"
    )
    main_node.query(
        f"CREATE TABLE {db}.t (a UInt64) ENGINE = ReplicatedMergeTree ORDER BY a"
    )
    dummy_node.query(f"SYSTEM SYNC DATABASE REPLICA {db}")

    # main_node's ALTER commits the coordinator transaction (the table's digest and metadata znode
    # advance in ZooKeeper) but then fails before renaming the durable .sql.
    main_node.query(
        "SYSTEM ENABLE FAILPOINT atomic_db_fail_after_txn_commit_before_rename"
    )
    error = main_node.query_and_get_error(
        f"ALTER TABLE {db}.t MODIFY COMMENT 'converged'"
    )
    assert error != ""

    # The comment was published only after the (failed) commit, so main_node's in-memory metadata is
    # still the old, empty comment -- not ahead of its unchanged .sql. Under the pre-WP4 order the
    # comment was set in memory before the commit, so this would already read 'converged'.
    assert _comment(main_node, db) == "\n"

    main_node.query(
        "SYSTEM DISABLE FAILPOINT atomic_db_fail_after_txn_commit_before_rename"
    )

    # dummy_node executed the same entry without the fault and applied the comment normally.
    dummy_node.query(f"SYSTEM SYNC DATABASE REPLICA {db}")
    assert _comment(dummy_node, db) == "converged\n"

    # Converging main_node forward to the committed comment is handled by DatabaseReplicated's
    # digest recovery (pre-existing machinery, orthogonal to this reorder and not exercised here).
    main_node.query(f"DROP DATABASE {db} SYNC")
    dummy_node.query(f"DROP DATABASE {db} SYNC")
