#!/usr/bin/env python3

import random
import string

import pytest
from kazoo.client import KazooClient, KazooState

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# clickhouse itself will use external zookeeper
node = cluster.add_instance(
    "node",
    main_configs=["configs/enable_keeper.xml"],
    stay_alive=True,
)


def random_string(length):
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=length))


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def get_connection_zk(nodename, timeout=30.0):
    _fake_zk_instance = KazooClient(
        hosts=cluster.get_instance_ip(nodename) + ":9181", timeout=timeout
    )
    _fake_zk_instance.start()
    return _fake_zk_instance


def restart_clickhouse():
    node.restart_clickhouse()
    keeper_utils.wait_until_connected(cluster, node)


def start_clean_clickhouse():
    node.stop_clickhouse()
    node.exec_in_container(["rm", "-rf", "/var/lib/clickhouse/coordination"])
    node.start_clickhouse()


def test_mntr_data_size_after_restart(started_cluster):
    start_clean_clickhouse()
    try:
        node_zk = None
        node_zk = get_connection_zk("node")

        node_zk.create("/test_mntr_data_size", b"somevalue")
        for i in range(100):
            node_zk.create(
                "/test_mntr_data_size/node" + str(i), random_string(123).encode()
            )

        node_zk.stop()
        node_zk.close()
        node_zk = None

        def get_line_from_mntr(mntr_str, key):
            return next(
                filter(
                    lambda line: key in line,
                    mntr_str.split("\n"),
                ),
                None,
            )

        mntr_result = keeper_utils.send_4lw_cmd(started_cluster, node, "mntr")
        line_size_before = get_line_from_mntr(mntr_result, "zk_approximate_data_size")
        node_count_before = get_line_from_mntr(mntr_result, "zk_znode_count")
        assert (
            get_line_from_mntr(mntr_result, "zk_ephemerals_count")
            == "zk_ephemerals_count\t0"
        )
        assert line_size_before != None

        restart_clickhouse()

        def assert_mntr_stats():
            mntr_result = keeper_utils.send_4lw_cmd(started_cluster, node, "mntr")
            assert (
                get_line_from_mntr(mntr_result, "zk_ephemerals_count")
                == "zk_ephemerals_count\t0"
            )
            assert (
                get_line_from_mntr(mntr_result, "zk_znode_count") == node_count_before
            )
            assert (
                get_line_from_mntr(mntr_result, "zk_approximate_data_size")
                == line_size_before
            )

        assert_mntr_stats()
        keeper_utils.send_4lw_cmd(started_cluster, node, "rclc")
        assert_mntr_stats()
    finally:
        try:
            if node_zk is not None:
                node_zk.stop()
                node_zk.close()
        except:
            pass
