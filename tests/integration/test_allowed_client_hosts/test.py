import logging

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
server = cluster.add_instance("server", user_configs=["configs/users.d/network.xml"])

clientA1 = cluster.add_instance("clientA1", hostname="clientA1.com")
clientA2 = cluster.add_instance("clientA2", hostname="clientA2.com")
clientA3 = cluster.add_instance("clientA3", hostname="clientA3.com")


def check_clickhouse_is_ok(client_node, server_node):
    assert (
        client_node.exec_in_container(
            ["bash", "-c", "/usr/bin/curl -s {}:8123 ".format(server_node.hostname)]
        )
        == "Ok.\n"
    )


def query_from_one_node_to_another(client_node, server_node, query):
    check_clickhouse_is_ok(client_node, server_node)
    res1 = client_node.exec_in_container(["ip", "address", "show"])
    res2 = client_node.exec_in_container(["host", "clientA1.com"])
    res3 = client_node.exec_in_container(["host", "clientA2.com"])
    res4 = client_node.exec_in_container(["host", "clientA3.com"])

    logging.debug(f"IP: {res1}, A1 {res2}, A2 {res3}, A3 {res4}")

    return client_node.exec_in_container(
        [
            "bash",
            "-c",
            "/usr/bin/clickhouse client --host {} --query {!r}".format(
                server_node.hostname, query
            ),
        ]
    )


def query(node, query):
    return query_from_one_node_to_another(node, node, query)


@pytest.fixture(scope="module", autouse=True)
def setup_nodes():
    try:
        cluster.start()
        query(server, "DROP TABLE IF EXISTS test_allowed_client_hosts")
        query(
            server,
            "CREATE TABLE test_allowed_client_hosts (x Int32) ENGINE = MergeTree() ORDER BY tuple()",
        )
        query(server, "INSERT INTO test_allowed_client_hosts VALUES (5)")

        s = query(server, "SELECT fqdn(), hostName()")
        a1 = query(clientA1, "SELECT fqdn(), hostName()")
        a2 = query(clientA2, "SELECT fqdn(), hostName()")
        a3 = query(clientA3, "SELECT fqdn(), hostName()")

        logging.debug(f"s:{s}, a1:{a1}, a2:{a2}, a3:{a3}")

        yield cluster

    finally:
        cluster.shutdown()


def test_allowed_host():
    expected_to_pass = [clientA1, clientA3]

    # Reverse DNS lookup currently isn't working as expected in this test.
    # For example, it gives something like "vitbartestallowedclienthosts_clientB1_1.vitbartestallowedclienthosts_default" instead of "clientB001.ru".
    # Maybe we should setup the test network better.
    # expected_to_pass.extend([clientB1, clientB2, clientB3, clientC1, clientC2, clientD1, clientD3])
    # expected_to_fail.extend([clientC3, clientD2])

    for client_node in expected_to_pass:
        assert (
            query_from_one_node_to_another(
                client_node, server, "SELECT * FROM test_allowed_client_hosts"
            )
            == "5\n"
        )


def test_denied_host():
    expected_to_fail = [clientA2]

    for client_node in expected_to_fail:
        with pytest.raises(Exception, match=r"default: Authentication failed"):
            query_from_one_node_to_another(
                client_node, server, "SELECT * FROM test_allowed_client_hosts"
            )
