"""
Test that ALTER TABLE ... COMMENT COLUMN correctly persists column comments
to ZooKeeper and replicates them for ReplicatedMergeTree tables.

Verifies that:
1. The ZooKeeper /columns node is updated with the new comment.
2. The comment is replicated to other replicas.
3. A new replica joining after COMMENT COLUMN gets the updated comment from ZK.
4. A subsequent real ALTER from another replica does not overwrite the comment.
"""

import uuid

import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

node_1 = cluster.add_instance(
    "node_1",
    main_configs=["configs/clusters.xml"],
    with_zookeeper=True,
    macros={"shard": 1, "replica": 1},
)

node_2 = cluster.add_instance(
    "node_2",
    main_configs=["configs/clusters.xml"],
    with_zookeeper=True,
    macros={"shard": 1, "replica": 2},
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def get_column_comment(node, table, column, database="default"):
    return node.query(
        f"SELECT comment FROM system.columns "
        f"WHERE database='{database}' AND table='{table}' AND name='{column}'"
    ).strip()


def get_zk_columns(node, zk_table_path):
    """Read the raw /columns node from ZooKeeper via system.zookeeper."""
    return node.query(
        f"SELECT value FROM system.zookeeper "
        f"WHERE path='{zk_table_path}' AND name='columns'"
    ).strip()


def get_zk_replica_columns(node, zk_table_path, replica):
    """Read the raw /replicas/{replica}/columns node from ZooKeeper."""
    return node.query(
        f"SELECT value FROM system.zookeeper "
        f"WHERE path='{zk_table_path}/replicas/{replica}' AND name='columns'"
    ).strip()


def get_zk_table_path(node, table, database="default"):
    """Get the zookeeper path for a ReplicatedMergeTree table."""
    return node.query(
        f"SELECT zookeeper_path FROM system.replicas "
        f"WHERE database='{database}' AND table='{table}'"
    ).strip()


def test_comment_column_persisted_to_zk(started_cluster):
    """COMMENT COLUMN updates both local metadata and the ZooKeeper /columns node."""

    node_1.query(
        "CREATE TABLE test_comment_zk (id Int64 COMMENT 'original') "
        "ENGINE=ReplicatedMergeTree('/clickhouse/test_comment_zk', '{replica}') "
        "ORDER BY id"
    )
    node_2.query(
        "CREATE TABLE test_comment_zk (id Int64 COMMENT 'original') "
        "ENGINE=ReplicatedMergeTree('/clickhouse/test_comment_zk', '{replica}') "
        "ORDER BY id"
    )

    zk_path = get_zk_table_path(node_1, "test_comment_zk")

    # Verify initial comment is in ZK
    zk_columns_before = get_zk_columns(node_1, zk_path)
    assert "original" in zk_columns_before

    # Now alter the column comment on node_1
    node_1.query("ALTER TABLE test_comment_zk COMMENT COLUMN id 'updated'")

    # Local metadata on node_1 is updated
    assert get_column_comment(node_1, "test_comment_zk", "id") == "updated"

    # ZooKeeper table-level /columns node is also updated
    zk_columns_after = get_zk_columns(node_1, zk_path)
    assert "updated" in zk_columns_after
    assert "original" not in zk_columns_after

    # Per-replica /columns nodes are also updated
    replica_1_columns = get_zk_replica_columns(node_1, zk_path, "1")
    assert "updated" in replica_1_columns
    assert "original" not in replica_1_columns

    node_1.query("DROP TABLE test_comment_zk SYNC")
    node_2.query("DROP TABLE test_comment_zk SYNC")


def test_comment_column_replicated_to_other_replica(started_cluster):
    """COMMENT COLUMN on one replica is replicated to the other replica."""

    node_1.query(
        "CREATE TABLE test_comment_repl (id Int64 COMMENT 'original') "
        "ENGINE=ReplicatedMergeTree('/clickhouse/test_comment_repl', '{replica}') "
        "ORDER BY id"
    )
    node_2.query(
        "CREATE TABLE test_comment_repl (id Int64 COMMENT 'original') "
        "ENGINE=ReplicatedMergeTree('/clickhouse/test_comment_repl', '{replica}') "
        "ORDER BY id"
    )

    # Update comment on node_1
    node_1.query("ALTER TABLE test_comment_repl COMMENT COLUMN id 'updated'")

    # node_1 sees the new comment
    assert get_column_comment(node_1, "test_comment_repl", "id") == "updated"

    # node_2 also sees the new comment via replication
    assert_eq_with_retry(
        node_2,
        "SELECT comment FROM system.columns WHERE database='default' AND table='test_comment_repl' AND name='id'",
        "updated",
    )

    # Both per-replica /columns nodes in ZK are updated
    zk_path = get_zk_table_path(node_1, "test_comment_repl")
    for replica in ("1", "2"):
        replica_columns = get_zk_replica_columns(node_1, zk_path, replica)
        assert "updated" in replica_columns
        assert "original" not in replica_columns

    node_1.query("DROP TABLE test_comment_repl SYNC")
    node_2.query("DROP TABLE test_comment_repl SYNC")


def test_new_replica_gets_comment_from_zk(started_cluster):
    """A new replica that joins after COMMENT COLUMN was issued gets the
    updated comment from ZooKeeper via cloneMetadataIfNeeded()."""

    node_1.query(
        "CREATE TABLE test_comment_clone (id Int64 COMMENT 'original') "
        "ENGINE=ReplicatedMergeTree('/clickhouse/test_comment_clone', '{replica}') "
        "ORDER BY id"
    )
    node_2.query(
        "CREATE TABLE test_comment_clone (id Int64 COMMENT 'original') "
        "ENGINE=ReplicatedMergeTree('/clickhouse/test_comment_clone', '{replica}') "
        "ORDER BY id"
    )

    # Update comment on node_1 — replicated through ZK
    node_1.query("ALTER TABLE test_comment_clone COMMENT COLUMN id 'updated'")

    # Wait for node_2 to pick up the change
    assert_eq_with_retry(
        node_2,
        "SELECT comment FROM system.columns WHERE database='default' AND table='test_comment_clone' AND name='id'",
        "updated",
    )

    # Drop table on node_2 only (keeping ZK state), then recreate it.
    # The new replica will initialize from ZK via cloneMetadataIfNeeded().
    node_2.query("DROP TABLE test_comment_clone SYNC")
    node_2.query(
        "CREATE TABLE test_comment_clone (id Int64) "
        "ENGINE=ReplicatedMergeTree('/clickhouse/test_comment_clone', '{replica}') "
        "ORDER BY id"
    )

    # The new replica should get the updated comment from ZK
    assert_eq_with_retry(
        node_2,
        "SELECT comment FROM system.columns WHERE database='default' AND table='test_comment_clone' AND name='id'",
        "updated",
    )

    # The new replica's per-replica /columns node should also have the updated comment
    zk_path = get_zk_table_path(node_1, "test_comment_clone")
    replica_2_columns = get_zk_replica_columns(node_1, zk_path, "2")
    assert "updated" in replica_2_columns
    assert "original" not in replica_2_columns

    node_1.query("DROP TABLE test_comment_clone SYNC")
    node_2.query("DROP TABLE test_comment_clone SYNC")


def test_new_replica_persists_comments_from_zk(started_cluster):
    """A new replica created with stale column comments adopts the comments from
    ZooKeeper not only in memory, but also in its persisted metadata, so
    SHOW CREATE TABLE and the table after DETACH/ATTACH match the replicated /columns."""

    node_1.query(
        "CREATE TABLE test_comment_persist (id Int64 COMMENT 'original', v String COMMENT 'v-original') "
        "ENGINE=ReplicatedMergeTree('/clickhouse/test_comment_persist', '{replica}') "
        "ORDER BY id"
    )
    node_1.query("ALTER TABLE test_comment_persist COMMENT COLUMN id 'updated'")
    node_1.query("ALTER TABLE test_comment_persist COMMENT COLUMN v ''")

    node_2.query(
        "CREATE TABLE test_comment_persist (id Int64 COMMENT 'stale', v String COMMENT 'stale-v') "
        "ENGINE=ReplicatedMergeTree('/clickhouse/test_comment_persist', '{replica}') "
        "ORDER BY id"
    )

    def check_comments():
        assert get_column_comment(node_2, "test_comment_persist", "id") == "updated"
        assert get_column_comment(node_2, "test_comment_persist", "v") == ""
        show_create = node_2.query(
            "SHOW CREATE TABLE test_comment_persist FORMAT TSVRaw"
        )
        assert "COMMENT 'updated'" in show_create, show_create
        assert "stale" not in show_create, show_create
        assert "original" not in show_create, show_create

    check_comments()

    # ATTACH loads the table from its persisted metadata (wherever the database
    # disk keeps it) and does not adopt comments from ZooKeeper, so stale
    # persisted comments would show up here.
    node_2.query("DETACH TABLE test_comment_persist")
    node_2.query("ATTACH TABLE test_comment_persist")
    check_comments()

    node_1.query("DROP TABLE test_comment_persist SYNC")
    node_2.query("DROP TABLE test_comment_persist SYNC")


def test_full_attach_keeps_comments_consistent(started_cluster):
    """A full-definition ATTACH of an existing replica does not adopt column
    comments from ZooKeeper: the in-memory metadata and the persisted definition
    both keep the comments from the ATTACH query, before and after a reattach."""

    for node in [node_1, node_2]:
        node.query(
            "CREATE TABLE test_comment_attach (id Int64 COMMENT 'original') "
            "ENGINE=ReplicatedMergeTree('/clickhouse/test_comment_attach', '{replica}') "
            "ORDER BY id"
        )

    node_1.query("ALTER TABLE test_comment_attach COMMENT COLUMN id 'updated'")
    assert_eq_with_retry(
        node_2,
        "SELECT comment FROM system.columns WHERE database='default' AND table='test_comment_attach' AND name='id'",
        "updated",
    )

    # Attach the existing replica again with a full definition carrying a different
    # comment. The detached table keeps its UUID, so the new one gets a fresh UUID
    # and name, but it uses the same ZooKeeper path and replica name.
    node_2.query("DETACH TABLE test_comment_attach SYNC")
    node_2.query(
        f"ATTACH TABLE test_comment_attach_full UUID '{uuid.uuid4()}' (id Int64 COMMENT 'attached') "
        "ENGINE=ReplicatedMergeTree('/clickhouse/test_comment_attach', '{replica}') "
        "ORDER BY id"
    )

    def check_comments():
        # Wait for the attach thread to finish initialization, so its checks against
        # ZooKeeper have run before the comments are compared.
        assert_eq_with_retry(
            node_2,
            "SELECT is_readonly FROM system.replicas WHERE database='default' AND table='test_comment_attach_full'",
            "0",
        )
        assert get_column_comment(node_2, "test_comment_attach_full", "id") == "attached"
        show_create = node_2.query(
            "SHOW CREATE TABLE test_comment_attach_full FORMAT TSVRaw"
        )
        assert "COMMENT 'attached'" in show_create, show_create
        assert "updated" not in show_create, show_create

    check_comments()

    # A short ATTACH loads the table from the persisted definition.
    node_2.query("DETACH TABLE test_comment_attach_full")
    node_2.query("ATTACH TABLE test_comment_attach_full")
    check_comments()

    # The other replica and ZooKeeper are not affected by the local ATTACH.
    assert get_column_comment(node_1, "test_comment_attach", "id") == "updated"
    assert "updated" in get_zk_columns(
        node_1, get_zk_table_path(node_1, "test_comment_attach")
    )

    node_1.query("DROP TABLE test_comment_attach SYNC")
    node_2.query("DROP TABLE test_comment_attach_full SYNC")
    # The original table is still detached and its replica is gone from ZooKeeper,
    # so it is attached read-only only to drop it.
    node_2.query("ATTACH TABLE test_comment_attach")
    node_2.query("DROP TABLE test_comment_attach SYNC")


def test_comment_preserved_after_add_column(started_cluster):
    """When COMMENT COLUMN is followed by ADD COLUMN on the same replica,
    the comment is preserved in ZK and on both replicas."""

    node_1.query(
        "CREATE TABLE test_comment_overwrite (id Int64 COMMENT 'original') "
        "ENGINE=ReplicatedMergeTree('/clickhouse/test_comment_overwrite', '{replica}') "
        "ORDER BY id"
    )
    node_2.query(
        "CREATE TABLE test_comment_overwrite (id Int64 COMMENT 'original') "
        "ENGINE=ReplicatedMergeTree('/clickhouse/test_comment_overwrite', '{replica}') "
        "ORDER BY id"
    )

    # Update comment on node_1 — now replicated through ZK
    node_1.query("ALTER TABLE test_comment_overwrite COMMENT COLUMN id 'updated'")

    # Wait for node_2 to pick up the comment change
    assert_eq_with_retry(
        node_2,
        "SELECT comment FROM system.columns WHERE database='default' AND table='test_comment_overwrite' AND name='id'",
        "updated",
    )

    # Now node_1 performs another real ALTER (ADD COLUMN).
    # The comment should be preserved because it was already in ZK.
    node_1.query("ALTER TABLE test_comment_overwrite ADD COLUMN val String")

    # Wait for node_2 to process the ADD COLUMN
    assert_eq_with_retry(
        node_2,
        "SELECT count() FROM system.columns WHERE database='default' AND table='test_comment_overwrite' AND name='val'",
        "1",
    )

    # Comment is preserved on both replicas after ADD COLUMN
    assert get_column_comment(node_1, "test_comment_overwrite", "id") == "updated"
    assert get_column_comment(node_2, "test_comment_overwrite", "id") == "updated"

    # ZK table-level and per-replica /columns nodes have the updated comment
    zk_path = get_zk_table_path(node_1, "test_comment_overwrite")
    zk_columns = get_zk_columns(node_1, zk_path)
    assert "updated" in zk_columns
    assert "original" not in zk_columns
    for replica in ("1", "2"):
        replica_columns = get_zk_replica_columns(node_1, zk_path, replica)
        assert "updated" in replica_columns
        assert "original" not in replica_columns

    node_1.query("DROP TABLE test_comment_overwrite SYNC")
    node_2.query("DROP TABLE test_comment_overwrite SYNC")
