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
