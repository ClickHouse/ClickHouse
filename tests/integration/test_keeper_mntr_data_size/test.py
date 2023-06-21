#!/usr/bin/env python3

import pytest
from helpers.cluster import ClickHouseCluster
import helpers.keeper_utils as keeper_utils
import random
import string
from kazoo.client import KazooClient, KazooState


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


def test_mntr_data_size_after_restart(started_cluster):
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

        def get_line_from_mntr(key):
            return next(
                filter(
                    lambda line: key in line,
                    keeper_utils.send_4lw_cmd(started_cluster, node, "mntr").split(
                        "\n"
                    ),
                ),
                None,
            )

        line_size_before = get_line_from_mntr("zk_approximate_data_size")
        node_count_before = get_line_from_mntr("zk_znode_count")
        assert get_line_from_mntr("zk_ephemerals_count") == "zk_ephemerals_count\t0"
        assert line_size_before != None

        restart_clickhouse()

        def assert_mntr_stats():
            assert get_line_from_mntr("zk_ephemerals_count") == "zk_ephemerals_count\t0"
            assert get_line_from_mntr("zk_znode_count") == node_count_before
            assert get_line_from_mntr("zk_approximate_data_size") == line_size_before

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
