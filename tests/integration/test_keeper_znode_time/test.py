import pytest
from helpers.cluster import ClickHouseCluster
import random
import string
import os
import time
from multiprocessing.dummy import Pool
from helpers.network import PartitionManager
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/enable_keeper1.xml", "configs/use_keeper.xml"],
    stay_alive=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/enable_keeper2.xml", "configs/use_keeper.xml"],
    stay_alive=True,
)
node3 = cluster.add_instance(
    "node3",
    main_configs=["configs/enable_keeper3.xml", "configs/use_keeper.xml"],
    stay_alive=True,
)

from kazoo.client import KazooClient, KazooState


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def smaller_exception(ex):
    return "\n".join(str(ex).split("\n")[0:2])


def wait_node(node):
    for _ in range(100):
        zk = None
        try:
            node.query("SELECT * FROM system.zookeeper WHERE path = '/'")
            zk = get_fake_zk(node.name, timeout=30.0)
            zk.create("/test", sequence=True)
            print("node", node.name, "ready")
            break
        except Exception as ex:
            time.sleep(0.2)
            print("Waiting until", node.name, "will be ready, exception", ex)
        finally:
            if zk:
                zk.stop()
                zk.close()
    else:
        raise Exception("Can't wait node", node.name, "to become ready")


def wait_nodes():
    for node in [node1, node2, node3]:
        wait_node(node)


def get_fake_zk(nodename, timeout=30.0):
    _fake_zk_instance = KazooClient(
        hosts=cluster.get_instance_ip(nodename) + ":9181", timeout=timeout
    )
    _fake_zk_instance.start()
    return _fake_zk_instance


def assert_eq_stats(stat1, stat2):
    assert stat1.version == stat2.version
    assert stat1.cversion == stat2.cversion
    assert stat1.aversion == stat2.aversion
    assert stat1.aversion == stat2.aversion
    assert stat1.dataLength == stat2.dataLength
    assert stat1.numChildren == stat2.numChildren
    assert stat1.ctime == stat2.ctime
    assert stat1.mtime == stat2.mtime


def test_between_servers(started_cluster):
    try:
        wait_nodes()
        node1_zk = get_fake_zk("node1")
        node2_zk = get_fake_zk("node2")
        node3_zk = get_fake_zk("node3")

        node1_zk.create("/test_between_servers")
        for child_node in range(1000):
            node1_zk.create("/test_between_servers/" + str(child_node))

        for child_node in range(1000):
            node1_zk.set("/test_between_servers/" + str(child_node), b"somevalue")

        for child_node in range(1000):
            stats1 = node1_zk.exists("/test_between_servers/" + str(child_node))
            stats2 = node2_zk.exists("/test_between_servers/" + str(child_node))
            stats3 = node3_zk.exists("/test_between_servers/" + str(child_node))
            assert_eq_stats(stats1, stats2)
            assert_eq_stats(stats2, stats3)

    finally:
        try:
            for zk_conn in [node1_zk, node2_zk, node3_zk]:
                zk_conn.stop()
                zk_conn.close()
        except:
            pass


def test_server_restart(started_cluster):
    try:
        wait_nodes()
        node1_zk = get_fake_zk("node1")

        node1_zk.create("/test_server_restart")
        for child_node in range(1000):
            node1_zk.create("/test_server_restart/" + str(child_node))

        for child_node in range(1000):
            node1_zk.set("/test_server_restart/" + str(child_node), b"somevalue")

        node3.restart_clickhouse(kill=True)

        node2_zk = get_fake_zk("node2")
        node3_zk = get_fake_zk("node3")
        for child_node in range(1000):
            stats1 = node1_zk.exists("/test_between_servers/" + str(child_node))
            stats2 = node2_zk.exists("/test_between_servers/" + str(child_node))
            stats3 = node3_zk.exists("/test_between_servers/" + str(child_node))
            assert_eq_stats(stats1, stats2)
            assert_eq_stats(stats2, stats3)

    finally:
        try:
            for zk_conn in [node1_zk, node2_zk, node3_zk]:
                zk_conn.stop()
                zk_conn.close()
        except:
            pass
