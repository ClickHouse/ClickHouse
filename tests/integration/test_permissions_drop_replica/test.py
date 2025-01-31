import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__, zookeeper_config_path="configs/zookeeper.xml")

main_configs = [
    "configs/remote_servers.xml",
]

node1 = cluster.add_instance(
    "node1",
    main_configs=main_configs,
    macros={"replica": "node1", "shard": "shard1"},
    with_zookeeper=True,
)

node2 = cluster.add_instance(
    "node2",
    main_configs=main_configs,
    macros={"replica": "node2", "shard": "shard2"},
    with_zookeeper=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_drop_permissions(start_cluster):
    node1.query("DROP DATABASE IF EXISTS r")
    node1.query(
        f"CREATE DATABASE r ENGINE=Replicated('/clickhouse/databases/r', '{{shard}}', '{{replica}}')"
    )
    node1.query(
        "CREATE TABLE r.t1 (x UInt8, y String) ENGINE=ReplicatedMergeTree ORDER BY x"
    )
    # create user without any permissions
    node1.query("CREATE USER foo;")
    node1.query("REVOKE ALL ON *.* FROM foo;")
    # try dropping replica using this user without any permissions
    got_error = node1.query_and_get_error("SYSTEM DROP REPLICA 'node2'", user="foo")
    # this operation should not fail silently
    assert (
        "DB::Exception: Access denied for SYSTEM DROP REPLICA. Not enough permissions to drop these databases:"
        in got_error
    )
    # ensure that the replica still exists
    assert (
        node1.query("SELECT host_name FROM system.clusters WHERE replica_num=1") != ""
    )
    # now query using default user (should have necessary permissions)
    node1.query("SYSTEM DROP REPLICA 'node2'", user="default")
    # assert that the replica was dropped successfully
    assert (
        node1.query("SELECT host_name FROM system.clusters WHERE replica_num=3").strip()
        == ""
    )
    node1.query("DROP DATABASE r")
