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
    # RESET SETTING with column or table comments. Such mixed batches must
    # converge across all replicas. They take the standard replicated-log
    # path in StorageReplicatedMergeTree::alter (the local-only fast paths
    # only apply to pure-settings or pure-comment ALTERs), so the DDL
    # routing forwards them as normal replicated ALTERs.
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


def test_modify_column_comment_only_on_cluster(started_cluster):
    # `ALTER ... MODIFY COLUMN c COMMENT 'x'` parses as `ASTAlterCommand::MODIFY_COLUMN`.
    # It does not change the resolved sorting-key data types, so the gating in
    # StorageMergeTree::alter / StorageReplicatedMergeTree::alter skips the
    # suspicious-primary-key check. It still goes through the replicated-log
    # path, so all replicas converge.
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
