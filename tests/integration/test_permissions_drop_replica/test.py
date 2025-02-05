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
    stay_alive=True,
)


def setup_nodes(nodes, shard):
    for node in nodes:
        node.query(
            """
                DROP TABLE IF EXISTS test.test_table;
                DROP DATABASE IF EXISTS test;
                DROP USER IF EXISTS test_user_xnhds;
                CREATE DATABASE test;
                SET database_replicated_allow_replicated_engine_arguments=2;  
                CREATE TABLE test.test_table(date Date, id UInt32)
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/{shard}/replicated/test_table', '{replica}') ORDER BY id PARTITION BY toYYYYMM(date);
            """.format(
                shard=shard, replica=node.name
            )
        )


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        setup_nodes([node1, node2], 1)
        yield cluster
    finally:
        cluster.shutdown()


def test_drop_permissions(start_cluster):
    node1.query(
        "INSERT INTO test.test_table SELECT number, toString(number) FROM numbers(100)"
    )

    # check metadata for replica node2 in keeper
    # this the node replica that we are going to drop later on
    zk = cluster.get_kazoo_client("zoo1")
    # check that the path from zk exists for the replica node2
    assert (
        zk.exists("/clickhouse/tables/test/1/replicated/test_table/replicas/node2")
        is not None
    )

    # create user without any permissions
    node1.query("CREATE USER test_user_xnhds;")
    node1.query("REVOKE ALL ON *.* FROM test_user_xnhds;")

    # we won't be able to drop an active replica
    assert (
        "DB::Exception: Can't drop replica: node2, because it's active."
        in node1.query_and_get_error("SYSTEM DROP REPLICA 'node2'")
    )

    # stop the server on replica node2 and assume it's gone forever
    # this will allow us to drop this replica
    node2.stop_clickhouse()

    # drop replica node2 from replica node1 using user without privileges
    got_error = node1.query_and_get_error(
        "SYSTEM DROP REPLICA 'node2'", user="test_user_xnhds"
    )
    # this operation should not fail silently
    assert (
        "DB::Exception: Access denied for SYSTEM DROP REPLICA. Not enough permissions to drop these databases:"
        in got_error
    )

    # drop replica node2 from replica node1 but using user with privileges
    # this will remove all replicated table metadata belonging to node2 from keeper
    node1.query("SYSTEM DROP REPLICA 'node2'")

    # check that the metadata for replica node2 was removed from keeper
    assert (
        zk.exists(" /clickhouse/tables/test/1/replicated/test_table/replicas/node2")
        is None
    )

    node1.query("DROP USER test_user_xnhds;")
    node1.query("DROP TABLE test.test_table;")
    node1.query("DROP DATABASE test;")
