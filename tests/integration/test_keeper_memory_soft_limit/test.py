#!/usr/bin/env python3
import random
import string

import pytest
from kazoo.client import KazooClient, KazooState
from kazoo.exceptions import ConnectionLoss

from helpers import keeper_utils
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__, keeper_config_dir="configs/")

# clickhouse itself will use external zookeeper
node = cluster.add_instance(
    "node",
    stay_alive=True,
    with_zookeeper=True,
)


def random_string(length):
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=length))


def get_connection_zk(nodename, timeout=30.0):
    _fake_zk_instance = KazooClient(
        hosts=cluster.get_instance_ip(nodename) + ":2181", timeout=timeout
    )
    _fake_zk_instance.start()
    return _fake_zk_instance


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def test_soft_limit_create(started_cluster):
    started_cluster.wait_zookeeper_to_start()
    try:
        node_zk = get_connection_zk("zoo1")
        loop_time = 100000
        node_zk.create("/test_soft_limit", b"abc")

        for i in range(loop_time):
            node_zk.create(
                "/test_soft_limit/node_" + str(i), random_string(1000).encode()
            )
    except ConnectionLoss:
        txn = node_zk.transaction()
        for i in range(10):
            txn.delete("/test_soft_limit/node_" + str(i))

        txn.create("/test_soft_limit/node_1000001" + str(i), b"abcde")
        txn.commit()
        return

    raise Exception("all records are inserted but no error occurs")
