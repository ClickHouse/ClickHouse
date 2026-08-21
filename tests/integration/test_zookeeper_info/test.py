#!/usr/bin/env python3

import pytest

import logging
import re

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/use_keeper.xml", "configs/enable_keeper1.xml"],
    stay_alive=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/use_keeper.xml", "configs/enable_keeper2.xml"],
    stay_alive=True,
)
node3 = cluster.add_instance(
    "node3",
    main_configs=["configs/use_keeper.xml", "configs/enable_keeper3.xml"],
    stay_alive=True,
)


# test that server is able to start
@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def test_select(started_cluster):
    count = node1.query("SELECT count() FROM system.zookeeper_info")
    assert (count == "3\n")
    response = node1.query("SELECT * FROM system.zookeeper_info")
    logging.info(response)
    name = node1.query("SELECT zookeeper_cluster_name FROM system.zookeeper_info limit 1")
    assert (name == "zookeeper\n")
    indices = node1.query("SELECT index FROM system.zookeeper_info")
    assert (indices == "0\n1\n2\n")

def test_info(started_cluster):
    response = node1.query("SELECT * FROM system.zookeeper_info FORMAT Vertical")
    response = re.sub(r'Row \d+:\n─+\n', '', response)
    rows = response.split("\n\n")

    node_infos = []
    for row in rows:
        row = row.strip()
        columns = row.split("\n")
        node_info = {}
        for column in columns:
            key, value = column.split(": ")
            value = value.strip()
            node_info[key] = None if value == 'ᴺᵁᴸᴸ' else value
        node_infos.append(node_info)

    logging.info(node_infos)

    assert all(node_info['port'] == '9181' for node_info in node_infos)

    assert all(node_info['is_readonly'] == '0' for node_info in node_infos)

    assert all(node_info['is_connected'] == '1' for node_info in node_infos)

    assert all(node_info['packets_received'] is not None for node_info in node_infos)
    assert all(node_info['packets_sent'] is not None for node_info in node_infos)

    assert sum(node_info['is_leader'] is not None for node_info in node_infos) == 1

    assert all(int(node_info['zxid']) > 0 for node_info in node_infos)

    assert all(int(node_info['log_dir_size']) > 0 for node_info in node_infos)
    assert all(int(node_info['last_log_idx']) > 0 for node_info in node_infos)
