import struct
import time

import logging
import pytest
import os
import threading
import re

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster

from kazoo.protocol.states import (
    AddWatchMode,
    WatcherType,
)

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

from helpers.cluster import get_docker_compose_path

DOCKER_COMPOSE_PATH = get_docker_compose_path()

config_dir = os.path.join(SCRIPT_DIR, './configs')

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1", main_configs=["configs/keeper_config1.xml"], stay_alive=True
)

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
    node1.restart_clickhouse()
    keeper_utils.wait_until_connected(cluster, node1)
    client = get_fake_zk(node1)
    NODE_PATH = "/testPersistentWatch2"
    FAKE_PATH = "/fakeNode"

    for path in [NODE_PATH, FAKE_PATH]:
        if client.exists(path):
            client.delete(path)

    events = []
    event_lock = threading.Event()

    def callback(event):
        events.append(dict(type=event.type, path=event.path))
        event_lock.set()

    client.create(NODE_PATH, b"1")
    client.create(FAKE_PATH, b"1")

    client.add_watch(NODE_PATH, callback, AddWatchMode.PERSISTENT)
    client.add_watch(NODE_PATH, callback, AddWatchMode.PERSISTENT)

    conn_count, watch_path_count, watch_count = parse_watch_stats()
    assert conn_count == 1
    assert watch_path_count == 2
    assert watch_count == 2

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
    node1.restart_clickhouse()
    keeper_utils.wait_until_connected(cluster, node1)
    client = get_fake_zk(node1)
    NODE_PATH = "/testPersistentWatch2"
    CHILD_NODE = "/testPersistentWatch2/child"
    FAKE_PATH = "/fakeNode"

    for path in [CHILD_NODE, NODE_PATH, FAKE_PATH]:
        if client.exists(path):
            client.delete(path)

    events = []
    event_lock = threading.Event()

    def callback(event):
        events.append(dict(type=event.type, path=event.path))
        event_lock.set()

    client.create(NODE_PATH, b"1")
    client.create(CHILD_NODE, b"1")
    client.create(FAKE_PATH, b"1")

    client.add_watch(NODE_PATH, callback, AddWatchMode.PERSISTENT_RECURSIVE)
    client.add_watch(NODE_PATH, callback, AddWatchMode.PERSISTENT_RECURSIVE)

    conn_count, watch_path_count, watch_count = parse_watch_stats()
    assert conn_count == 1
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

def test_persistent_watch_event_fields(started_cluster):
    node1.restart_clickhouse()
    keeper_utils.wait_until_connected(cluster, node1)

    client = get_fake_zk(node1)
    NODE_PATH = "/testEventFields1"
    OTHER_PATH = "/testEventFieldsOther1"

    for path in [NODE_PATH, OTHER_PATH]:
        if client.exists(path):
            client.delete(path)

    client.create(NODE_PATH, b"1")
    client.create(OTHER_PATH, b"1")

    events = []
    event_lock = threading.Event()

    def cb(event):
        events.append(dict(type=event.type, path=event.path, state=getattr(event, "state", None)))
        event_lock.set()

    client.add_watch(NODE_PATH, cb, AddWatchMode.PERSISTENT)

    event_lock.clear()
    client.set(NODE_PATH, b"2")
    event_lock.wait(5)
    assert len(events) == 1
    assert events[-1]["type"] == "CHANGED"
    assert events[-1]["path"] == NODE_PATH
    assert events[-1]["state"] == "CONNECTED"

    event_lock.clear()
    client.set(NODE_PATH, b"3")
    event_lock.wait(5)
    assert len(events) == 2
    assert events[-1]["type"] == "CHANGED"
    assert events[-1]["path"] == NODE_PATH
    assert events[-1]["state"] == "CONNECTED"

    client.set(OTHER_PATH, b"9")
    time.sleep(0.3)
    assert len(events) == 2

    client.remove_all_watches(NODE_PATH, WatcherType.ANY)
    client.set(NODE_PATH, b"after")
    time.sleep(0.5)
    assert len(events) == 2

    if client.exists(NODE_PATH):
        client.delete(NODE_PATH)
    if client.exists(OTHER_PATH):
        client.delete(OTHER_PATH)

def test_persistent_recursive_watch_event_fields(started_cluster):
    node1.restart_clickhouse()
    keeper_utils.wait_until_connected(cluster, node1)

    client = get_fake_zk(node1)
    NODE_PATH = "/testEventFields2"
    CHILD_NODE = f"{NODE_PATH}/child"
    OTHER_PATH = "/testEventFieldsOther2"

    for path in [CHILD_NODE, NODE_PATH, OTHER_PATH]:
        if client.exists(path):
            client.delete(path)

    client.create(NODE_PATH, b"1")
    client.create(CHILD_NODE, b"1")
    client.create(OTHER_PATH, b"1")

    events = []
    event_lock = threading.Event()

    def cb(event):
        events.append(dict(type=event.type, path=event.path, state=getattr(event, "state", None)))
        event_lock.set()

    client.add_watch(NODE_PATH, cb, AddWatchMode.PERSISTENT_RECURSIVE)

    event_lock.clear()
    client.set(NODE_PATH, b"2")
    event_lock.wait(5)
    assert len(events) == 1
    assert events[-1]["path"] == NODE_PATH
    assert events[-1]["type"] == "CHANGED"
    assert events[-1]["state"] == "CONNECTED"

    event_lock.clear()
    client.set(CHILD_NODE, b"3")
    event_lock.wait(5)
    assert len(events) == 2
    assert events[-1]["path"] == NODE_PATH
    assert events[-1]["type"] == "CHANGED"
    assert events[-1]["state"] == "CONNECTED"

    event_lock.clear()
    client.set(OTHER_PATH, b"x")
    time.sleep(0.3)
    assert len(events) == 2

    client.remove_all_watches(NODE_PATH, WatcherType.ANY)
    client.set(NODE_PATH, b"after")
    client.set(CHILD_NODE, b"after2")
    time.sleep(0.5)
    assert len(events) == 2

    if client.exists(CHILD_NODE):
        client.delete(CHILD_NODE)
    if client.exists(NODE_PATH):
        client.delete(NODE_PATH)
    if client.exists(OTHER_PATH):
        client.delete(OTHER_PATH)

@pytest.mark.skip(reason="https://github.com/ClickHouse/ClickHouse/issues/92480")
def test_persistent_watches_cleanup_on_close(started_cluster):
    node1.restart_clickhouse()
    keeper_utils.wait_until_connected(cluster, node1)

    client = get_fake_zk(node1)
    NODE_PATH = "/testPersistentCleanup"

    if client.exists(NODE_PATH):
        client.delete(NODE_PATH)
    client.create(NODE_PATH, b"1")

    def cb(_):
        pass

    client.add_watch(NODE_PATH, cb, AddWatchMode.PERSISTENT)
    client.add_watch(NODE_PATH, cb, AddWatchMode.PERSISTENT_RECURSIVE)

    destroy_zk_client(client)
    for _ in range(20):
        data = keeper_utils.send_4lw_cmd(cluster, node1, cmd="mntr")
        if "zk_watch_count\t0" in data:
            break
        time.sleep(0.1)
    assert "zk_watch_count\t0" in data

def test_clear_watches(started_cluster):
    node1.restart_clickhouse()
    keeper_utils.wait_until_connected(cluster, node1)

    client = get_fake_zk(node1)
    NODE_PATH = "/testPersistentCleanup"

    def cb(_):
        pass

    client.add_watch(NODE_PATH, cb, AddWatchMode.PERSISTENT)
    client.add_watch(NODE_PATH, cb, AddWatchMode.PERSISTENT_RECURSIVE)
    destroy_zk_client(client)
    data = keeper_utils.send_4lw_cmd(cluster, node1, cmd="mntr")
    assert "zk_watch_count\t0" in data
