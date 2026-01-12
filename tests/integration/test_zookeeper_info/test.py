#!/usr/bin/env python3

import pytest

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
    print(response)
    name = node1.query("SELECT zookeeper_cluster_name FROM system.zookeeper_info limit 1")
    assert (name == "zookeeper\n")
    indices = node1.query("SELECT index FROM system.zookeeper_info")
    assert (indices == "0\n1\n2\n")