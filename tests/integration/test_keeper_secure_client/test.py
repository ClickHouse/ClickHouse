#!/usr/bin/env python3
import pytest
from helpers.cluster import ClickHouseCluster
import string
import os
import time

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance('node1', main_configs=['configs/enable_secure_keeper.xml', 'configs/ssl_conf.xml', "configs/dhparam.pem", "configs/server.crt", "configs/server.key"])
node2 = cluster.add_instance('node2', main_configs=['configs/use_secure_keeper.xml', 'configs/ssl_conf.xml', "configs/server.crt", "configs/server.key"])


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def test_connection(started_cluster):
    # just nothrow
    node2.query("SELECT * FROM system.zookeeper WHERE path = '/'")
