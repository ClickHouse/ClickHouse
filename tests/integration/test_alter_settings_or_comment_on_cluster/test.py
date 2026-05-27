import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
ch1 = cluster.add_instance(
    "ch1",
    main_configs=[
        "configs/config.d/clusters.xml",
        "configs/config.d/distributed_ddl.xml",
    ],
    with_zookeeper=True,
)
ch2 = cluster.add_instance(
    "ch2",
    main_configs=[
        "configs/config.d/clusters.xml",
        "configs/config.d/distributed_ddl.xml",
    ],
    with_zookeeper=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        ch1.query("CREATE DATABASE test_db ON CLUSTER 'cluster'")
        yield cluster
    finally:
        cluster.shutdown()


def test_mixed_settings_and_comment_alter_on_cluster(started_cluster):
    # Regression test for ON CLUSTER ALTER batches that mix MODIFY SETTING /
    # RESET SETTING with column or table comments. The storage-side fast path
    # in StorageReplicatedMergeTree::alter for "settings or comment" ALTERs
    # skips the replicated log entry, so the DDL routing must execute the
    # query on every replica rather than only on the leader. Without the
    # routing fix, the leader applies the change and the followers diverge.
    ch1.query(
        database="test_db",
        sql="CREATE TABLE mixed_alter (x UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/tables/mixed_alter', 'r1') ORDER BY tuple()",
    )
    ch2.query(
        database="test_db",
        sql="CREATE TABLE mixed_alter (x UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/tables/mixed_alter', 'r2') ORDER BY tuple()",
    )

    # MODIFY COMMENT + MODIFY SETTING in a single ON CLUSTER ALTER.
    ch1.query(
        database="test_db",
        sql="ALTER TABLE mixed_alter ON CLUSTER 'cluster' MODIFY COMMENT 'mixed-on-cluster', MODIFY SETTING old_parts_lifetime = 123",
    )

    # Both replicas must see the new comment and the new setting value.
    for node in [ch1, ch2]:
        show_create = node.query(
            database="test_db",
            sql="SHOW CREATE mixed_alter FORMAT TSVRaw",
        )
        assert "old_parts_lifetime = 123" in show_create, (node.name, show_create)
        assert "mixed-on-cluster" in show_create, (node.name, show_create)

    # MODIFY COMMENT + RESET SETTING - same shape, mixed setting / comment kinds.
    ch1.query(
        database="test_db",
        sql="ALTER TABLE mixed_alter ON CLUSTER 'cluster' MODIFY COMMENT 'second-mixed', RESET SETTING old_parts_lifetime",
    )

    for node in [ch1, ch2]:
        show_create = node.query(
            database="test_db",
            sql="SHOW CREATE mixed_alter FORMAT TSVRaw",
        )
        assert "old_parts_lifetime = 123" not in show_create, (node.name, show_create)
        assert "second-mixed" in show_create, (node.name, show_create)

    # COMMENT COLUMN + MODIFY SETTING - column-comment variant.
    ch1.query(
        database="test_db",
        sql="ALTER TABLE mixed_alter ON CLUSTER 'cluster' COMMENT COLUMN x 'x-col-comment', MODIFY SETTING old_parts_lifetime = 234",
    )

    for node in [ch1, ch2]:
        show_create = node.query(
            database="test_db",
            sql="SHOW CREATE mixed_alter FORMAT TSVRaw",
        )
        assert "old_parts_lifetime = 234" in show_create, (node.name, show_create)
        assert "x-col-comment" in show_create, (node.name, show_create)

    ch1.query(
        database="test_db",
        sql="DROP TABLE mixed_alter ON CLUSTER 'cluster' SYNC",
    )
