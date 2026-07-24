import pytest

from helpers.cluster import CLICKHOUSE_CI_MIN_TESTED_VERSION, ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)
node_oldest = cluster.add_instance(
    "node_oldest",
    image="clickhouse/clickhouse-server",
    tag=CLICKHOUSE_CI_MIN_TESTED_VERSION,
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
def setup_nodes():
    try:
        cluster.start()

        for n in old_nodes + [new_node]:
            n.query(
                """CREATE TABLE test_table (id UInt32, value UInt64) ENGINE = MergeTree() ORDER BY tuple()"""
            )

        for n in old_nodes:
            n.query(
                """CREATE TABLE dist_table AS test_table ENGINE = Distributed('test_cluster', 'default', 'test_table')"""
            )

        yield cluster
    finally:
        cluster.shutdown()


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
    assert_eq_with_retry(
        initiator,
        "SELECT COUNT() FROM dist_table WHERE id=3",
        str(len(old_nodes)),
    )
