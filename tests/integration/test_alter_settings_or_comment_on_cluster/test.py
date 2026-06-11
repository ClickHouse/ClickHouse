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


def get_zk_metadata_version(node, zookeeper_path):
    # The ZooKeeper `/metadata` node version is only bumped by the replicated
    # ALTER_METADATA path. A local settings/comment fast path leaves it
    # untouched, so it is a precise signal for "this ALTER did not write a
    # replicated log entry".
    return int(
        node.query(
            f"SELECT version FROM system.zookeeper WHERE path = '{zookeeper_path}' AND name = 'metadata'"
        ).strip()
    )


def test_mixed_settings_and_comment_alter_on_cluster(started_cluster):
    # ON CLUSTER ALTER batches mixing MODIFY SETTING / RESET SETTING with
    # column or table comments must converge on every replica. The storage
    # layer applies settings/comments as local metadata (never via the
    # replicated log), so convergence relies on DDLWorker forwarding the
    # query to all replicas rather than just the leader.
    zookeeper_path = "/clickhouse/tables/mixed_alter"
    ch1.query(
        database="test_db",
        sql=f"CREATE TABLE mixed_alter (x UInt64) ENGINE=ReplicatedMergeTree('{zookeeper_path}', 'r1') ORDER BY tuple()",
    )
    ch2.query(
        database="test_db",
        sql=f"CREATE TABLE mixed_alter (x UInt64) ENGINE=ReplicatedMergeTree('{zookeeper_path}', 'r2') ORDER BY tuple()",
    )

    # MODIFY COMMENT + MODIFY SETTING in a single ON CLUSTER ALTER.
    version_before = get_zk_metadata_version(ch1, zookeeper_path)
    ch1.query(
        database="test_db",
        sql="ALTER TABLE mixed_alter ON CLUSTER 'cluster' MODIFY COMMENT 'mixed-on-cluster', MODIFY SETTING old_parts_lifetime = 123",
    )

    # The mixed batch must take the local fast path on every replica, not the
    # replicated ALTER_METADATA path. Otherwise both replicas race for the same
    # /metadata version, one wins and the others retry with CANNOT_ASSIGN_ALTER
    # while appending no-op log entries. The ZK metadata version must not move.
    assert get_zk_metadata_version(ch1, zookeeper_path) == version_before

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


def test_modify_column_comment_only_on_cluster(started_cluster):
    # `ALTER ... MODIFY COLUMN c COMMENT 'x'` parses as `MODIFY_COLUMN` but the
    # storage layer recognises it as comment-only and applies it as local
    # metadata, so it must also be routed to every replica. A MODIFY COLUMN with
    # a real type change instead takes the full replicated-log path (negative
    # case below). Both must converge across replicas.
    ch1.query(
        database="test_db",
        sql="CREATE TABLE modcol_comment (id UInt64, x String) ENGINE=ReplicatedMergeTree('/clickhouse/tables/modcol_comment', 'r1') ORDER BY id",
    )
    ch2.query(
        database="test_db",
        sql="CREATE TABLE modcol_comment (id UInt64, x String) ENGINE=ReplicatedMergeTree('/clickhouse/tables/modcol_comment', 'r2') ORDER BY id",
    )

    # `MODIFY COLUMN ... COMMENT '...'` only.
    ch1.query(
        database="test_db",
        sql="ALTER TABLE modcol_comment ON CLUSTER 'cluster' MODIFY COLUMN x COMMENT 'modcol-comment-v1'",
    )

    for node in [ch1, ch2]:
        show_create = node.query(
            database="test_db",
            sql="SHOW CREATE modcol_comment FORMAT TSVRaw",
        )
        assert "modcol-comment-v1" in show_create, (node.name, show_create)

    # Mixed: `MODIFY COLUMN ... COMMENT '...'` + `MODIFY SETTING`.
    ch1.query(
        database="test_db",
        sql="ALTER TABLE modcol_comment ON CLUSTER 'cluster' MODIFY COLUMN x COMMENT 'modcol-comment-v2', MODIFY SETTING old_parts_lifetime = 345",
    )

    for node in [ch1, ch2]:
        show_create = node.query(
            database="test_db",
            sql="SHOW CREATE modcol_comment FORMAT TSVRaw",
        )
        assert "modcol-comment-v2" in show_create, (node.name, show_create)
        assert "old_parts_lifetime = 345" in show_create, (node.name, show_create)

    # Mixed: `MODIFY COLUMN ... COMMENT '...'` + `MODIFY COMMENT '...'`.
    ch1.query(
        database="test_db",
        sql="ALTER TABLE modcol_comment ON CLUSTER 'cluster' MODIFY COLUMN x COMMENT 'modcol-comment-v3', MODIFY COMMENT 'table-comment-modcol'",
    )

    for node in [ch1, ch2]:
        show_create = node.query(
            database="test_db",
            sql="SHOW CREATE modcol_comment FORMAT TSVRaw",
        )
        assert "modcol-comment-v3" in show_create, (node.name, show_create)
        assert "table-comment-modcol" in show_create, (node.name, show_create)

    # Comment-only `MODIFY COLUMN ... FIRST`: a positional modifier still parses
    # as comment-only (no type change), so the storage layer applies it as local
    # metadata via the comment fast path. The column comment and the new column
    # order must both converge on every replica.
    ch1.query(
        database="test_db",
        sql="ALTER TABLE modcol_comment ON CLUSTER 'cluster' MODIFY COLUMN x COMMENT 'modcol-placement-comment' FIRST",
    )

    for node in [ch1, ch2]:
        show_create = node.query(
            database="test_db",
            sql="SHOW CREATE modcol_comment FORMAT TSVRaw",
        )
        assert "modcol-placement-comment" in show_create, (node.name, show_create)
        order = node.query(
            database="test_db",
            sql="SELECT name FROM system.columns WHERE database = 'test_db' AND table = 'modcol_comment' ORDER BY position FORMAT TSVRaw",
        ).split()
        assert order == ["x", "id"], (node.name, order)

    # Negative case: `MODIFY COLUMN` with a real type change must still route as a
    # full replicated ALTER and converge across replicas through the replication log.
    ch1.query(
        database="test_db",
        sql="ALTER TABLE modcol_comment ON CLUSTER 'cluster' MODIFY COLUMN x String COMMENT 'modcol-with-type'",
    )

    for node in [ch1, ch2]:
        show_create = node.query(
            database="test_db",
            sql="SHOW CREATE modcol_comment FORMAT TSVRaw",
        )
        assert "modcol-with-type" in show_create, (node.name, show_create)

    ch1.query(
        database="test_db",
        sql="DROP TABLE modcol_comment ON CLUSTER 'cluster' SYNC",
    )
