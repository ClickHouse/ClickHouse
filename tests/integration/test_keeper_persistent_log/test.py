#!/usr/bin/env python3
import pytest
from helpers.cluster import ClickHouseCluster
import random
import string
import os
import time
from kazoo.client import KazooClient, KazooState


cluster = ClickHouseCluster(__file__)

node = cluster.add_instance('node', main_configs=['configs/enable_keeper.xml', 'configs/use_keeper.xml'], stay_alive=True)


def random_string(length):
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))

def create_random_path(prefix="", depth=1):
    if depth == 0:
        return prefix
    return create_random_path(os.path.join(prefix, random_string(3)), depth - 1)

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()

def get_connection_zk(nodename, timeout=30.0):
    _fake_zk_instance = KazooClient(hosts=cluster.get_instance_ip(nodename) + ":9181", timeout=timeout)
    _fake_zk_instance.start()
    return _fake_zk_instance

def test_state_after_restart(started_cluster):
    try:
        node_zk = None
        node_zk2 = None
        node_zk = get_connection_zk("node")

        node_zk.create("/test_state_after_restart", b"somevalue")
        strs = []
        for i in range(100):
            strs.append(random_string(123).encode())
            node_zk.create("/test_state_after_restart/node" + str(i), strs[i])

        for i in range(100):
            if i % 7 == 0:
                node_zk.delete("/test_state_after_restart/node" + str(i))

        node.restart_clickhouse(kill=True)

        node_zk2 = get_connection_zk("node")

        assert node_zk2.get("/test_state_after_restart")[0] == b"somevalue"
        for i in range(100):
            if i % 7 == 0:
                assert node_zk2.exists("/test_state_after_restart/node" + str(i)) is None
            else:
                assert len(node_zk2.get("/test_state_after_restart/node" + str(i))[0]) == 123
                assert node_zk2.get("/test_state_after_restart/node" + str(i))[0] == strs[i]
    finally:
        try:
            if node_zk is not None:
                node_zk.stop()
                node_zk.close()

            if node_zk2 is not None:
                node_zk2.stop()
                node_zk2.close()
        except:
            pass


# http://zookeeper-user.578899.n2.nabble.com/Why-are-ephemeral-nodes-written-to-disk-tp7583403p7583418.html
def test_ephemeral_after_restart(started_cluster):
    try:
        node_zk = None
        node_zk2 = None
        node_zk = get_connection_zk("node")

        node_zk.create("/test_ephemeral_after_restart", b"somevalue")
        strs = []
        for i in range(100):
            strs.append(random_string(123).encode())
            node_zk.create("/test_ephemeral_after_restart/node" + str(i), strs[i], ephemeral=True)

        for i in range(100):
            if i % 7 == 0:
                node_zk.delete("/test_ephemeral_after_restart/node" + str(i))

        node.restart_clickhouse(kill=True)

        node_zk2 = get_connection_zk("node")

        assert node_zk2.get("/test_ephemeral_after_restart")[0] == b"somevalue"
        for i in range(100):
            if i % 7 == 0:
                assert node_zk2.exists("/test_ephemeral_after_restart/node" + str(i)) is None
            else:
                assert len(node_zk2.get("/test_ephemeral_after_restart/node" + str(i))[0]) == 123
                assert node_zk2.get("/test_ephemeral_after_restart/node" + str(i))[0] == strs[i]
    finally:
        try:
            if node_zk is not None:
                node_zk.stop()
                node_zk.close()

            if node_zk2 is not None:
                node_zk2.stop()
                node_zk2.close()
        except:
            pass
