import struct
import time

import pytest
import docker
import os
import sys
import subprocess
from kazoo.exceptions import NoNodeError
import helpers.keeper_utils as ku
import threading
import re

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster

from kazoo.protocol.states import (
    AddWatchMode,
    KazooState,
    KeeperState,
    WatcherType,
    EventType,
)

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

from helpers.cluster import get_docker_compose_path

DOCKER_COMPOSE_PATH = get_docker_compose_path()

config_dir = os.path.join(SCRIPT_DIR, './configs')

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1", main_configs=["configs/keeper_config1.xml"], stay_alive=True
)
node2 = cluster.add_instance(
    "node2", main_configs=["configs/keeper_config2.xml"], stay_alive=True
)
node3 = cluster.add_instance(
    "node3", main_configs=["configs/keeper_config3.xml"], stay_alive=True
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


def get_fake_zk(node, timeout=30.0):
    return keeper_utils.get_fake_zk(cluster, node.name, timeout=timeout)


def get_keeper_socket(node_name):
    return keeper_utils.get_keeper_socket(cluster, node_name)

def parse_watch_stats():
    data = keeper_utils.send_4lw_cmd(cluster, node1, cmd="wchs")
    list_data = [n for n in data.split("\n") if len(n.strip()) > 0]

    # 37 connections watching 632141 paths
    # Total watches:632141
    matcher = re.match(
        r"([0-9].*) connections watching ([0-9].*) paths", list_data[0], re.S
    )
    conn_count = int(matcher.group(1))
    watch_path_count = int(matcher.group(2))
    watch_count = int(
        re.match(r"Total watches:([0-9].*)", list_data[1], re.S).group(1)
    )
    return conn_count, watch_path_count, watch_count

def test_persistent_watch(started_cluster):
    keeper_utils.wait_until_connected(cluster, node1)
    client = get_fake_zk(node1)
    NODE_PATH = "/testPersistentWatch2"
    FAKE_PATH = "/fakeNode"

    events = []
    event_lock = threading.Event()

    def callback(event):
        events.append(dict(type=event.type, path=event.path))
        event_lock.set()

    client.create(NODE_PATH, b"1")
    client.create(FAKE_PATH, b"1")

    client.add_watch(NODE_PATH, callback, AddWatchMode.PERSISTENT)
    with pytest.raises(Exception) as err:
        client.add_watch(NODE_PATH, callback, AddWatchMode.PERSISTENT)

    conn_count, watch_path_count, watch_count = parse_watch_stats()
    # there could be connection to system node.
    if conn_count == 1:
        assert watch_path_count == 1
        assert watch_count == 1

    full_path = client.chroot + NODE_PATH
    assert len(client._persistent_watchers[full_path]) == 1

    event_lock.clear()
    client.set(NODE_PATH, b"2")
    event_lock.wait(5)
    assert len(events) == 1

    event_lock.clear()
    client.set(NODE_PATH, b"3")
    event_lock.wait(5)
    assert len(events) == 2

    client.set(FAKE_PATH, b"3")
    time.sleep(0.3)
    assert len(events) == 2

    client.remove_all_watches(NODE_PATH, WatcherType.ANY)

    client.set(NODE_PATH, b"Another update")
    time.sleep(1)
    assert len(events) == 2

    client.delete(NODE_PATH)
    client.delete(FAKE_PATH)

def test_persistent_recursive_watch(started_cluster):
    keeper_utils.wait_until_connected(cluster, node1)
    client = get_fake_zk(node1)

    client = get_fake_zk(node1)
    NODE_PATH = "/testPersistentWatch2"
    CHILD_NODE = "/testPersistentWatch2/child"
    FAKE_PATH = "/fakeNode"

    events = []
    event_lock = threading.Event()

    def callback(event):
        events.append(dict(type=event.type, path=event.path))
        event_lock.set()

    client.create(NODE_PATH, b"1")
    client.create(CHILD_NODE, b"1")
    client.create(FAKE_PATH, b"1")

    client.add_watch(NODE_PATH, callback, AddWatchMode.PERSISTENT_RECURSIVE)
    with pytest.raises(Exception) as err:
        client.add_watch(NODE_PATH, callback, AddWatchMode.PERSISTENT_RECURSIVE)

    conn_count, watch_path_count, watch_count = parse_watch_stats()
    if conn_count == 1:
        assert watch_path_count == 1
        assert watch_count == 1

    event_lock.clear()
    client.set(NODE_PATH, b"2")
    event_lock.wait(5)
    assert len(events) == 1
    assert events[-1]["path"] == NODE_PATH

    event_lock.clear()
    client.set(CHILD_NODE, b"3")
    event_lock.wait(5)
    assert len(events) == 2
    assert events[-1]["path"] == NODE_PATH

    event_lock.clear()
    client.set(FAKE_PATH, b"999")
    time.sleep(0.3)
    assert len(events) == 2

    client.remove_all_watches(NODE_PATH, WatcherType.ANY)

    client.set(NODE_PATH, b"after")
    client.set(CHILD_NODE, b"after2")
    client.set(FAKE_PATH, b"after3")
    time.sleep(1)

    assert len(events) == 2
    client.delete(CHILD_NODE)
    client.delete(NODE_PATH)
    client.delete(FAKE_PATH)
