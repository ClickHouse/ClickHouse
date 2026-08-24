import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/remote_servers.xml"],
    with_zookeeper=True,
    stay_alive=True,
    macros={"shard": "s1", "replica": "r1"},
)

node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/remote_servers.xml"],
    with_zookeeper=True,
    macros={"shard": "s1", "replica": "r2"},
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


@pytest.mark.parametrize(
    "before_removing_keeper_path",
    [False, True],
)
def test_drop_database_failed_on_secondary_node_before_removing_keeper_path(
    started_cluster,
    before_removing_keeper_path: bool,
):
    node1.query("DROP DATABASE IF EXISTS test SYNC")
    node2.query("DROP DATABASE IF EXISTS test SYNC")

    failpoint_name = (
        "database_replicated_drop_before_removing_keeper_failed"
        if before_removing_keeper_path
        else "database_replicated_drop_after_removing_keeper_failed"
    )
    node2.query(f"SYSTEM ENABLE FAILPOINT {failpoint_name}")

    node1.query(
        r"CREATE DATABASE test ON CLUSTER 'test_cluster' ENGINE=Replicated('/clickhouse/db/test','{shard}','{replica}')"
    )

    node1.query("CREATE TABLE test.t (x INT, y String) ENGINE=MergeTree ORDER BY x")
    assert (
        node2.query(
            "SELECT count() FROM system.tables WHERE database='test' AND name='t'"
        ).strip()
        == "1"
    )

    node2.query(f"SYSTEM DISABLE FAILPOINT {failpoint_name}")

    node1.query("DROP DATABASE test ON CLUSTER 'test_cluster' SYNC")
    node2.query(f"SYSTEM DISABLE FAILPOINT {failpoint_name}")
