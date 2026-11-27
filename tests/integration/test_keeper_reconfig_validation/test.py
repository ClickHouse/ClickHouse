#!/usr/bin/env python3

import time

import pytest
from kazoo.exceptions import BadArgumentsError

import helpers.keeper_utils as ku
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# ClickHouse itself uses a separate ZooKeeper, not the Keeper cluster the test reconfigures
node1 = cluster.add_instance(
    "node1", main_configs=["configs/keeper1.xml"], with_zookeeper=True, stay_alive=True
)
node2 = cluster.add_instance(
    "node2", main_configs=["configs/keeper2.xml"], with_zookeeper=True, stay_alive=True
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def get_config(zk):
    return zk.get("/keeper/config")[0].decode()


def test_reconfig_validation(started_cluster):
    ku.wait_nodes(cluster, [node1, node2])
    zk = ku.get_fake_zk(cluster, node1.name)
    try:
        config = get_config(zk)
        assert sorted(config.split("\n")) == [
            "server.1=node1:9234;participant;1",
            "server.2=node2:9234;learner;1",
        ]

        # node2 is a learner, so no participant would be left
        with pytest.raises(BadArgumentsError):
            zk.reconfig(joining=None, leaving="1", new_members=None)

        with pytest.raises(BadArgumentsError):
            zk.reconfig(
                joining="server.2=node2:9234;learner;5", leaving="2", new_members=None
            )

        assert get_config(zk) == config

        new_config = zk.reconfig(joining=None, leaving="2,2", new_members=None)[
            0
        ].decode()
        assert new_config == "server.1=node1:9234;participant;1"

        deadline = time.monotonic() + 60
        while get_config(zk) != new_config:
            assert time.monotonic() < deadline, get_config(zk)
            time.sleep(0.5)
    finally:
        zk.stop()
        zk.close()
