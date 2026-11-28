#!/usr/bin/env python3

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=[
        "configs/enable_secure_keeper.xml",
        "configs/ssl_conf.xml",
        "configs/dhparam.pem",
        "configs/server.crt",
        "configs/server.key",
    ],
)
node2 = cluster.add_instance(
    "node2",
    main_configs=[
        "configs/use_secure_keeper.xml",
        "configs/ssl_conf.xml",
        "configs/server.crt",
        "configs/server.key",
    ],
)
# Same secure Keeper, but this client actually verifies the certificate it is presented.
node3 = cluster.add_instance(
    "node3",
    main_configs=[
        "configs/use_secure_keeper.xml",
        "configs/ssl_conf_verify.xml",
        "configs/server.crt",
        "configs/server.key",
    ],
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def test_connection(started_cluster):
    # just nothrow
    node2.query_with_retry("SELECT * FROM system.zookeeper WHERE path = '/'")


def test_connection_verifying_certificate(started_cluster):
    # The socket connects to the address <host>node1</host> resolves to, so this only succeeds if
    # the certificate is matched against `node1` rather than against that address.
    node3.query_with_retry("SELECT * FROM system.zookeeper WHERE path = '/'")


def test_keeper_client_four_letter_word(started_cluster):
    # The certificate is verified, so this also checks that it is matched against `node1`.
    data = node1.exec_in_container(
        [
            "bash",
            "-c",
            "clickhouse keeper-client -h node1 -p 10181 --secure "
            "--tls-ca-file /etc/clickhouse-server/config.d/server.crt -q 'flwc ruok'",
        ],
        privileged=True,
    )
    assert data.strip() == "imok"


def test_keeper_client_four_letter_word_secure_node(started_cluster):
    # The node and its <secure>1</secure> come from the server config (use_secure_keeper.xml), not from --secure.
    data = node3.exec_in_container(
        [
            "bash",
            "-c",
            "clickhouse keeper-client -c /etc/clickhouse-server/config.xml "
            "--tls-ca-file /etc/clickhouse-server/config.d/server.crt -q 'flwc ruok'",
        ],
        privileged=True,
    )
    assert data.strip() == "imok"
