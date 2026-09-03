#!/usr/bin/env python3

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/keeper.xml", "configs/keeper_without_compression.xml"],
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
    assert node1.query("SELECT 1") == "1\n"
