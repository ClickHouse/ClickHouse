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
    node1.query("SELECT * from system.zookeeper_info")
    assert node1.query("SELECT 1") == "1\n"
