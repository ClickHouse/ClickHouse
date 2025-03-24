import struct
import time

import pytest
import docker
import os
import sys
import subprocess
from kazoo.exceptions import NoNodeError

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

from helpers.cluster import get_docker_compose_path

DOCKER_COMPOSE_PATH = get_docker_compose_path()

config_dir = os.path.join(SCRIPT_DIR, './configs')

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1", main_configs=["configs/keeper_config1.xml"], stay_alive=True,
    with_keeper_persistent_watcher=True
)

node2 = cluster.add_instance(
    "node2", main_configs=["configs/keeper_config2.xml"], stay_alive=True,
    with_keeper_persistent_watcher=True
)

node3 = cluster.add_instance(
    "node3", main_configs=["configs/keeper_config3.xml"], stay_alive=True,
    with_keeper_persistent_watcher=True
)

bool_struct = struct.Struct("B")
int_struct = struct.Struct("!i")
int_int_struct = struct.Struct("!ii")
int_int_long_struct = struct.Struct("!iiq")

int_long_int_long_struct = struct.Struct("!iqiq")
long_struct = struct.Struct("!q")
multiheader_struct = struct.Struct("!iBi")
reply_header_struct = struct.Struct("!iqi")
stat_struct = struct.Struct("!qqqqiiiqiiq")


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def destroy_zk_client(zk):
    try:
        if zk:
            zk.stop()
            zk.close()
    except:
        pass


def wait_nodes():
    keeper_utils.wait_nodes(cluster, [node1])


def get_fake_zk(nodename, timeout=30.0):
    return keeper_utils.get_fake_zk(cluster, nodename, timeout=timeout)


def get_keeper_socket(node_name):
    return keeper_utils.get_keeper_socket(cluster, node_name)


def test_create2(started_cluster):
    wait_nodes()

    node1_zk = None
    node1_zk = get_fake_zk(node1.name)
    stats = node1_zk.create('/tea', include_data=True)
    assert stats is not None


def test_persistent_watch(started_cluster):
    node = cluster.instances["node"]

    reference = [
        "Connected!",
        "Trying to add PERSISTENT watch...",
        "PERSISTENT watch added successfully!",
        "Updating node data...",
        "PERSISTENT Watch triggered: /testPersistentWatch2",
        "Updating node data again...",
        "PERSISTENT Watch triggered: /testPersistentWatch2",
        "Updating fake node data...",
        "Trying to remove PERSISTENT watch...",
        "PERSISTENT watch removed successfully!",
        "Updating node data again...",
        "Test finished."
    ]

    res = started_cluster.exec_in_container(
        started_cluster.keeper_persistent_watcher_id,
        [
            "bash",
            "-c",
            f"java ZooKeeperWatcher",
        ],
    )
    assert res == reference


def test_persistent_recursive_watch(started_cluster):
    node = cluster.instances["node"]

    reference = [
        "Connected!",
        "Trying to add PERSISTENT watch...",
        "PERSISTENT watch added successfully!",
        "Updating node data...",
        "PERSISTENT Watch triggered: /testPersistentWatch2 NodeDataChanged",
        "Updating child node data...",
        "PERSISTENT Watch triggered: /testPersistentWatch2 NodeDataChanged",
        "Updating fake node data...",
        "Trying to remove PERSISTENT watch...",
        "PERSISTENT watch removed successfully!",
        "PERSISTENT Watch triggered: /testPersistentWatch2 PersistentWatchRemoved",
        "Updating node data...",
        "Updating node data...",
        "Updating node data...",
        "Test finished."
    ]

    res = started_cluster.exec_in_container(
        started_cluster.keeper_persistent_watcher_id,
        [
            "bash",
            "-c",
            f"java ZooKeeperRecursiveWatcher",
        ],
    )
    assert res == reference
