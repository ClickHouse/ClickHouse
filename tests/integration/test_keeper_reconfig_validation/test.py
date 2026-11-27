#!/usr/bin/env python3

import time

import pytest
from kazoo.exceptions import BadArgumentsError

import helpers.keeper_utils as ku
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# ClickHouse itself uses a separate ZooKeeper, not the Keeper cluster the test reconfigures
nodes = [
    cluster.add_instance(
        f"node{i}",
        main_configs=[f"configs/keeper{i}.xml"],
        with_zookeeper=True,
        stay_alive=True,
    )
    for i in range(1, 4)
]


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
    ku.wait_nodes(cluster, nodes)
    zk = ku.get_fake_zk(cluster, nodes[0].name)
    try:
        config = get_config(zk)
        assert sorted(config.split("\n")) == [
            "server.1=node1:9234;participant;1",
            "server.2=node2:9234;learner;1",
            "server.3=node3:9234;participant;1",
        ]

        # Only the learner would be left
        with pytest.raises(BadArgumentsError):
            zk.reconfig(joining=None, leaving="1,3", new_members=None)

        # Server 3 would be left with priority 0, so it could not become leader
        with pytest.raises(BadArgumentsError):
            zk.reconfig(
                joining="server.3=node3:9234;participant;0",
                leaving="1",
                new_members=None,
            )

        with pytest.raises(BadArgumentsError):
            zk.reconfig(
                joining="server.2=node2:9234;learner;5", leaving="2", new_members=None
            )

        with pytest.raises(BadArgumentsError):
            zk.reconfig(
                joining="server.4=node4:9234", leaving="1,2,3", new_members=None
            )

        assert get_config(zk) == config

        new_config = zk.reconfig(joining=None, leaving="2,2,2", new_members=None)[
            0
        ].decode()
        assert sorted(new_config.split("\n")) == [
            "server.1=node1:9234;participant;1",
            "server.3=node3:9234;participant;1",
        ]

        deadline = time.monotonic() + 60
        while get_config(zk) != new_config:
            assert time.monotonic() < deadline, get_config(zk)
            time.sleep(0.5)
    finally:
        zk.stop()
        zk.close()


def test_reconfig_keeps_a_participant():
    # Server 1 is the only participant; with priority 0 it is elected as NuRaft's temporary zero-priority leader
    zero_cluster = ClickHouseCluster(__file__, name="zero_priority")
    zero_nodes = [
        zero_cluster.add_instance(
            f"zero_node{i}", main_configs=[f"configs/zero_priority{i}.xml"]
        )
        for i in range(1, 3)
    ]
    try:
        zero_cluster.start()
        ku.wait_until_connected(zero_cluster, zero_nodes[0], timeout=60)
        zk = ku.get_fake_zk(zero_cluster, zero_nodes[0].name)
        try:
            with pytest.raises(BadArgumentsError):
                zk.reconfig(joining=None, leaving="1", new_members=None)
        finally:
            zk.stop()
            zk.close()
    finally:
        zero_cluster.shutdown()
