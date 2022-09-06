#!/usr/bin/env python3

import pytest

from helpers.cluster import ClickHouseCluster
import helpers.keeper_utils as keeper_utils

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1", main_configs=["configs/keeper.xml"], stay_alive=True
)

# test that server is able to start
@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        keeper_utils.wait_until_connected(cluster, node1)

        yield cluster
    finally:
        cluster.shutdown()


def test_create_replicated(started_cluster):
    assert node1.query("SELECT 1") == "1\n"
