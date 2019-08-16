import time
import pytest

from helpers.cluster import ClickHouseCluster
from multiprocessing.dummy import Pool
from helpers.client import QueryRuntimeException, QueryTimeoutExceedException

from helpers.test_tools import assert_eq_with_retry
cluster = ClickHouseCluster(__file__)
node18_14 = cluster.add_instance('node18_14', image='yandex/clickhouse-server:18.14.19', with_installed_binary=True)
node19_1 = cluster.add_instance('node19_1', image='yandex/clickhouse-server:19.1.16', with_installed_binary=True)
node19_4 = cluster.add_instance('node19_4', image='yandex/clickhouse-server:19.4.5.35', with_installed_binary=True)
node19_6 = cluster.add_instance('node19_6', image='yandex/clickhouse-server:19.6.3.18', with_installed_binary=True)
node19_8 = cluster.add_instance('node19_8', image='yandex/clickhouse-server:19.8.3.8', with_installed_binary=True)
node_new = cluster.add_instance('node_new')

@pytest.fixture(scope="module")
def setup_nodes():
    try:
        cluster.start()
        for n in (node18_14, node19_1, node19_4, node19_6, node19_8, node_new):
            n.query('''CREATE TABLE test_table (id UInt32, value UInt64) ENGINE = MergeTree() ORDER BY tuple()''')

        yield cluster
    finally:
        cluster.shutdown()


def query_from_one_node_to_another(client_node, server_node, query):
    client_node.exec_in_container(["bash", "-c", "/usr/bin/clickhouse client --host {} --query '{}'".format(server_node.name, query)])

def test_client_from_different_versions(setup_nodes):
    old_nodes = (node18_14, node19_1, node19_4, node19_6, node19_8)
    # from new to old
    for n in old_nodes:
        query_from_one_node_to_another(node_new, n, "INSERT INTO test_table VALUES (1, 1)")

    for n in old_nodes:
        query_from_one_node_to_another(node_new, n, "SELECT COUNT() FROM test_table")

    for n in old_nodes:
        assert n.query("SELECT COUNT() FROM test_table") == "1\n"

    # from old to new
    for i, n in enumerate(old_nodes):
        query_from_one_node_to_another(n, node_new, "INSERT INTO test_table VALUES ({i}, {i})".format(i=i))

    for n in old_nodes:
        query_from_one_node_to_another(n, node_new, "SELECT COUNT() FROM test_table")

    assert node_new.query("SELECT COUNT() FROM test_table") == str(len(old_nodes)) + "\n"
