import pytest

from helpers.cluster import CLICKHOUSE_CI_MIN_TESTED_VERSION, ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)
node_oldest = cluster.add_instance(
    "node_oldest",
    image="clickhouse/clickhouse-server",
    tag="25.12",
    with_installed_binary=True,
    main_configs=["configs/config.d/test_cluster.xml"],
)
old_nodes = [node_oldest]
new_node = cluster.add_instance("node_new")


def query_from_one_node_to_another(client_node, server_node, query):
    client_node.exec_in_container(
        [
            "bash",
            "-c",
            "/usr/bin/clickhouse client --host {} --query {!r}".format(
                server_node.name, query
            ),
        ]
    )


@pytest.fixture(scope="module")
def setup_cluster():
    try:
        cluster.start()
        yield
    finally:
        cluster.shutdown()


@pytest.fixture(scope="function")
def setup_nodes(setup_cluster):
    try:
        for n in old_nodes + [new_node]:
            n.query(
                """CREATE TABLE test_table (id UInt32, value UInt64) ENGINE = MergeTree() ORDER BY tuple()"""
            )

        for n in old_nodes:
            n.query(
                """CREATE TABLE dist_table AS test_table ENGINE = Distributed('test_cluster', 'default', 'test_table')"""
            )

        yield

    finally:
        for n in old_nodes:
            n.query(
                """SYSTEM FLUSH DISTRIBUTED dist_table"""
            )
            n.query(
                """DROP TABLE dist_table SYNC"""
            )

        for n in old_nodes + [new_node]:
            n.query(
                """DROP TABLE test_table SYNC"""
            )


def test_client_is_older_than_server(setup_nodes):
    server = new_node
    for i, client in enumerate(old_nodes):
        query_from_one_node_to_another(
            client, server, "INSERT INTO test_table VALUES (1, {})".format(i)
        )

    for client in old_nodes:
        query_from_one_node_to_another(client, server, "SELECT COUNT() FROM test_table")

    assert (
        server.query("SELECT COUNT() FROM test_table WHERE id=1")
        == str(len(old_nodes)) + "\n"
    )


def test_server_is_older_than_client(setup_nodes):
    client = new_node
    for i, server in enumerate(old_nodes):
        query_from_one_node_to_another(
            client, server, "INSERT INTO test_table VALUES (2, {})".format(i)
        )

    for server in old_nodes:
        query_from_one_node_to_another(client, server, "SELECT COUNT() FROM test_table")

    for server in old_nodes:
        assert server.query("SELECT COUNT() FROM test_table WHERE id=2") == "1\n"


def test_distributed_query_initiator_is_older_than_shard(setup_nodes):
    shard = new_node
    for i, initiator in enumerate(old_nodes):
        initiator.query("INSERT INTO dist_table VALUES (3, {})".format(i))

    assert_eq_with_retry(
        shard,
        "SELECT COUNT() FROM test_table WHERE id=3",
        str(len(old_nodes)),
    )

    for i, initiator in enumerate(old_nodes):
        assert_eq_with_retry(
            initiator,
            "SELECT COUNT() FROM dist_table WHERE id=3",
            str(len(old_nodes)),
        )
