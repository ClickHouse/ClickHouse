import logging
import re
import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.keeper_utils import ss_established
from helpers.network import PartitionManager
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(
    __file__, zookeeper_config_path="configs/zookeeper_load_balancing.xml"
)

# use 3-letter hostnames, so getHostNameDifference("nod1", "zoo1") will work as expected
node1 = cluster.add_instance(
    "nod1", with_zookeeper=True, main_configs=["configs/zookeeper_load_balancing.xml"]
)
node2 = cluster.add_instance(
    "nod2", with_zookeeper=True, main_configs=["configs/zookeeper_load_balancing.xml"]
)
node3 = cluster.add_instance(
    "nod3", with_zookeeper=True, main_configs=["configs/zookeeper_load_balancing.xml"]
)
node4 = cluster.add_instance(
    "nod4", with_zookeeper=True, main_configs=["configs/zookeeper_load_balancing2.xml"]
)

zk1_re = re.compile(
    r"testzookeeperconfigloadbalancing-(gw\d+-)?zoo1-1"
    r".*testzookeeperconfigloadbalancing(-gw\d+)?_default:2181"
)
zk2_re = re.compile(
    r"testzookeeperconfigloadbalancing-(gw\d+-)?zoo2-1"
    r".*testzookeeperconfigloadbalancing(-gw\d+)?_default:2181"
)
zk3_re = re.compile(
    r"testzookeeperconfigloadbalancing-(gw\d+-)?zoo3-1"
    r".*testzookeeperconfigloadbalancing(-gw\d+)?_default:2181"
)


def change_balancing(old, new, reload=True):
    line = "<zookeeper_load_balancing>{}<"
    old_line = line.format(old)
    new_line = line.format(new)
    for node in [node1, node2, node3]:
        node.replace_in_config(
            "/etc/clickhouse-server/config.d/zookeeper_load_balancing.xml",
            old_line,
            new_line,
        )
        if reload:
            node.query("select '{}', '{}'".format(old, new))
            node.query("system reload config")


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_first_or_random(started_cluster):
    try:
        change_balancing("random", "first_or_random")
        for node in (node1, node2, node3):
            connections = (
                node.exec_in_container(ss_established, privileged=True, user="root")
                .strip()
                .split("\n")
            )
            logging.debug("Established connections for 2181:\n%s", connections)
            assert len(connections) == 1
            assert zk1_re.search(connections[0])
    finally:
        change_balancing("first_or_random", "random", reload=False)


def test_in_order(started_cluster):
    try:
        change_balancing("random", "in_order")
        for node in (node1, node2, node3):
            connections = (
                node.exec_in_container(ss_established, privileged=True, user="root")
                .strip()
                .split("\n")
            )
            logging.debug("Established connections for 2181:\n%s", connections)
            assert len(connections) == 1
            assert zk1_re.search(connections[0])
    finally:
        change_balancing("in_order", "random", reload=False)


def test_nearest_hostname(started_cluster):
    try:
        change_balancing("random", "nearest_hostname")
        for node, regexp in ((node1, zk1_re), (node2, zk2_re), (node3, zk3_re)):
            connections = (
                node.exec_in_container(ss_established, privileged=True, user="root")
                .strip()
                .split("\n")
            )
            logging.debug("Established connections for 2181:\n%s", connections)
            assert len(connections) == 1
            assert regexp.search(connections[0])
    finally:
        change_balancing("nearest_hostname", "random", reload=False)


def test_hostname_levenshtein_distance(started_cluster):
    try:
        change_balancing("random", "hostname_levenshtein_distance")
        for node, regexp in ((node1, zk1_re), (node2, zk2_re), (node3, zk3_re)):
            connections = (
                node.exec_in_container(ss_established, privileged=True, user="root")
                .strip()
                .split("\n")
            )
            logging.debug("Established connections for 2181:\n%s", connections)
            assert len(connections) == 1
            assert regexp.search(connections[0])
    finally:
        change_balancing("hostname_levenshtein_distance", "random", reload=False)


def test_round_robin(started_cluster):
    pm = PartitionManager()
    try:
        change_balancing("random", "round_robin")
        for node in [node1, node2, node3]:
            idx = int(
                node.query("select index from system.zookeeper_connection").strip()
            )
            new_idx = (idx + 1) % 3

            pm._add_rule(
                {
                    "source": node.ip_address,
                    "destination": cluster.get_instance_ip("zoo" + str(idx + 1)),
                    "action": "REJECT --reject-with tcp-reset",
                }
            )

            assert_eq_with_retry(
                node,
                "select index from system.zookeeper_connection",
                str(new_idx) + "\n",
            )
            pm.heal_all()
    finally:
        pm.heal_all()
        change_balancing("round_robin", "random", reload=False)


def test_az(started_cluster):
    pm = PartitionManager()
    try:
        # make sure it disconnects from the optimal node
        pm._add_rule(
            {
                "source": node4.ip_address,
                "destination": cluster.get_instance_ip("zoo2"),
                "action": "REJECT --reject-with tcp-reset",
            }
        )

        node4.query_with_retry("select * from system.zookeeper where path='/'")
        assert "az2\n" != node4.query(
            "select availability_zone from system.zookeeper_connection"
        )

        # fallback_session_lifetime.max is 1 second, but it shouldn't drop current session until the node becomes available

        time.sleep(5)  # this is fine
        assert 5 <= int(node4.query("select zookeeperSessionUptime()").strip())

        pm.heal_all()
        assert_eq_with_retry(
            node4, "select availability_zone from system.zookeeper_connection", "az2\n"
        )
    finally:
        pm.heal_all()
