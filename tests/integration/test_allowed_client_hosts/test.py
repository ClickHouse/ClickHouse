import os
import pytest
from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)
server = cluster.add_instance('server', config_dir="configs")

clientA1 = cluster.add_instance('clientA1', hostname = 'clientA1.com')
clientA2 = cluster.add_instance('clientA2', hostname = 'clientA2.com')
clientA3 = cluster.add_instance('clientA3', hostname = 'clientA3.com')
clientB1 = cluster.add_instance('clientB1', hostname = 'clientB001.ru')
clientB2 = cluster.add_instance('clientB2', hostname = 'clientB002.ru')
clientB3 = cluster.add_instance('clientB3', hostname = 'xxx.clientB003.rutracker.com')
clientC1 = cluster.add_instance('clientC1', hostname = 'clientC01.ru')
clientC2 = cluster.add_instance('clientC2', hostname = 'xxx.clientC02.ru')
clientC3 = cluster.add_instance('clientC3', hostname = 'xxx.clientC03.rutracker.com')
clientD1 = cluster.add_instance('clientD1', hostname = 'clientD0001.ru')
clientD2 = cluster.add_instance('clientD2', hostname = 'xxx.clientD0002.ru')
clientD3 = cluster.add_instance('clientD3', hostname = 'clientD0003.ru')


def query_from_one_node_to_another(client_node, server_node, query):
    return client_node.exec_in_container(["bash", "-c", "/usr/bin/clickhouse client --host {} --query {!r}".format(server_node.hostname, query)])


def query(node, query):
    return query_from_one_node_to_another(node, node, query)


@pytest.fixture(scope="module", autouse=True)
def setup_nodes():
    try:
        cluster.start()
        query(server, "CREATE TABLE test_table (x Int32) ENGINE = MergeTree() ORDER BY tuple()")
        query(server, "INSERT INTO test_table VALUES (5)")

        yield cluster

    finally:
        cluster.shutdown()


def test_allowed_host():
    expected_to_pass = [clientA1, clientA3]
    expected_to_fail = [clientA2]

    # Reverse DNS lookup currently isn't working as expected in this test.
    # For example, it gives something like "vitbartestallowedclienthosts_clientB1_1.vitbartestallowedclienthosts_default" instead of "clientB001.ru".
    # Maybe we should setup the test network better.
    #expected_to_pass.extend([clientB1, clientB2, clientB3, clientC1, clientC2, clientD1, clientD3])
    #expected_to_fail.extend([clientC3, clientD2])

    for client_node in expected_to_pass:
        assert query_from_one_node_to_another(client_node, server, "SELECT * FROM test_table") == "5\n"

    for client_node in expected_to_fail:
        with pytest.raises(Exception) as e:
            query_from_one_node_to_another(client_node, server, "SELECT * FROM test_table")
        assert "default: Authentication failed" in str(e)
