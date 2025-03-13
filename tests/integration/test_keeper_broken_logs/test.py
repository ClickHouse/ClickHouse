import time
from multiprocessing.dummy import Pool

import pytest

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/enable_keeper1.xml"],
    stay_alive=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/enable_keeper2.xml"],
    stay_alive=True,
)
node3 = cluster.add_instance(
    "node3",
    main_configs=["configs/enable_keeper3.xml"],
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


def wait_nodes():
    keeper_utils.wait_nodes(cluster, [node1, node2, node3])


def get_fake_zk(nodename, timeout=30.0):
    _fake_zk_instance = KazooClient(
        hosts=cluster.get_instance_ip(nodename) + ":9181", timeout=timeout
    )
    _fake_zk_instance.start()
    return _fake_zk_instance


def start_clickhouse(node):
    node.start_clickhouse()


def clean_start():
    nodes = [node1, node2, node3]
    for node in nodes:
        node.stop_clickhouse()

    p = Pool(3)
    waiters = []
    for node in nodes:
        node.exec_in_container(["rm", "-rf", "/var/lib/clickhouse/coordination/log"])
        node.exec_in_container(
            ["rm", "-rf", "/var/lib/clickhouse/coordination/snapshots"]
        )
        waiters.append(p.apply_async(start_clickhouse, (node,)))

    for waiter in waiters:
        waiter.wait()


def test_single_node_broken_log(started_cluster):
    clean_start()
    try:
        wait_nodes()
        node1_conn = get_fake_zk("node1")

        node1_conn.create("/test_broken_log")
        for _ in range(10):
            node1_conn.create(f"/test_broken_log/node", b"somedata1", sequence=True)

        def verify_nodes(zk_conn):
            children = zk_conn.get_children("/test_broken_log")
            assert len(children) == 10

            for child in children:
                assert zk_conn.get("/test_broken_log/" + child)[0] == b"somedata1"

        verify_nodes(node1_conn)

        node1_conn.stop()
        node1_conn.close()

        node1.stop_clickhouse()

        # wait until cluster stabilizes with a new leader
        while not keeper_utils.is_leader(
            started_cluster, node2
        ) and not keeper_utils.is_leader(started_cluster, node3):
            time.sleep(1)

        node1.exec_in_container(
            [
                "truncate",
                "-s",
                "-50",
                "/var/lib/clickhouse/coordination/log/changelog_1_100000.bin",
            ]
        )
        node1.start_clickhouse()
        keeper_utils.wait_until_connected(cluster, node1)

        node1_conn = get_fake_zk("node1")
        node1_conn.create(f"/test_broken_log_final_node", b"somedata1")

        verify_nodes(node1_conn)
        assert node1_conn.get("/test_broken_log_final_node")[0] == b"somedata1"

        node2_conn = get_fake_zk("node2")
        verify_nodes(node2_conn)
        assert node2_conn.get("/test_broken_log_final_node")[0] == b"somedata1"

        node3_conn = get_fake_zk("node2")
        verify_nodes(node3_conn)
        assert node3_conn.get("/test_broken_log_final_node")[0] == b"somedata1"

        node1_logs = (
            node1.exec_in_container(["ls", "/var/lib/clickhouse/coordination/log"])
            .strip()
            .split("\n")
        )
        assert len(node1_logs) == 2 and node1_logs[0] == "changelog_1_100000.bin"
        assert (
            node2.exec_in_container(["ls", "/var/lib/clickhouse/coordination/log"])
            == "changelog_1_100000.bin\n"
        )
        assert (
            node3.exec_in_container(["ls", "/var/lib/clickhouse/coordination/log"])
            == "changelog_1_100000.bin\n"
        )
    finally:
        try:
            for zk_conn in [node1_conn, node2_conn, node3_conn]:
                zk_conn.stop()
                zk_conn.close()
        except:
            pass
