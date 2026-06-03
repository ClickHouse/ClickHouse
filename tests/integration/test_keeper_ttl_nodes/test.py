#!/usr/bin/env python3

import uuid
import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster
import time
import pytest
from kazoo.exceptions import BadArgumentsError

def get_fake_zk(cluster, nodename, timeout=30.0):
    return keeper_utils.get_fake_zk(cluster, nodename, timeout=timeout)


def wait_nodes_gone(zks, paths, timeout=30.0):
    deadline = time.monotonic() + timeout
    for zk in zks:
        for path in paths:
            while zk.exists(path) is not None:
                if time.monotonic() >= deadline:
                    raise AssertionError(f"{path} still exists on Keeper after {timeout}s")
                time.sleep(0.05)


def wait_nodes_exist(zks, paths, timeout=30.0):
    # A follower applies a committed write to its state machine slightly after
    # the leader has acknowledged it to the client, so a node created on the
    # leader may not be visible on a follower for a brief moment. Poll instead
    # of asserting immediately to avoid a race.
    deadline = time.monotonic() + timeout
    for zk in zks:
        for path in paths:
            while zk.exists(path) is None:
                if time.monotonic() >= deadline:
                    raise AssertionError(f"{path} did not appear on Keeper within {timeout}s")
                time.sleep(0.05)


def test_simple():
    run_uuid = uuid.uuid4()
    cluster = ClickHouseCluster(__file__, str(run_uuid))
    # Disable `with_remote_database_disk` as the test does not use the default Keeper.
    cluster.add_instance(
        "node1", main_configs=["configs/enable_keeper1.xml"], stay_alive=True, with_remote_database_disk=False,
    )
    cluster.add_instance(
        "node2", main_configs=["configs/enable_keeper2.xml"], stay_alive=True, with_remote_database_disk=False,
    )

    node1_zk = None

    cluster.start()

    node1_zk = get_fake_zk(cluster, "node1")
    node2_zk = get_fake_zk(cluster, "node2")
    node1_zk.create("/test_alive", b"aaaa", ttl=1000)
    wait_nodes_exist([node1_zk, node2_zk], ["/test_alive"])
    wait_nodes_gone([node1_zk, node2_zk], ["/test_alive"])

    node1_zk.create("/test_alive", b"aaaa", ttl=1)
    wait_nodes_gone([node1_zk, node2_zk], ["/test_alive"])

    cluster.shutdown()

    if node1_zk:
        node1_zk.stop()
        node1_zk.close()
    if node2_zk:
        node2_zk.stop()
        node2_zk.close()

def test_ttl_node_cannot_have_children():
    run_uuid = uuid.uuid4()
    cluster = ClickHouseCluster(__file__, str(run_uuid))
    cluster.add_instance(
        "node1", main_configs=["configs/enable_keeper1.xml"], stay_alive=True, with_remote_database_disk=False,
    )
    cluster.add_instance(
        "node2", main_configs=["configs/enable_keeper2.xml"], stay_alive=True, with_remote_database_disk=False,
    )

    node1_zk = None
    node2_zk = None
    cluster.start()

    try:
        node1_zk = get_fake_zk(cluster, "node1")
        node2_zk = get_fake_zk(cluster, "node2")

        node1_zk.create("/ttl_parent", b"aaaa", ttl=60000)
        assert node1_zk.exists("/ttl_parent")

        with pytest.raises(BadArgumentsError):
            node1_zk.create("/ttl_parent/child", b"bbbb")
        with pytest.raises(BadArgumentsError):
            node1_zk.create("/ttl_parent/child", b"bbbb", ttl=1000)
        with pytest.raises(BadArgumentsError):
            node2_zk.create("/ttl_parent/child", b"bbbb")

        assert not node1_zk.exists("/ttl_parent/child")
        assert not node2_zk.exists("/ttl_parent/child")
    finally:
        cluster.shutdown()
        if node1_zk:
            node1_zk.stop()
            node1_zk.close()
        if node2_zk:
            node2_zk.stop()
            node2_zk.close()


def test_manual_remove_before_ttl_expiration():
    run_uuid = uuid.uuid4()
    cluster = ClickHouseCluster(__file__, str(run_uuid))
    cluster.add_instance(
        "node1", main_configs=["configs/enable_keeper1.xml"], stay_alive=True, with_remote_database_disk=False,
    )
    cluster.add_instance(
        "node2", main_configs=["configs/enable_keeper2.xml"], stay_alive=True, with_remote_database_disk=False,
    )

    node1_zk = None
    node2_zk = None
    cluster.start()

    try:
        node1_zk = get_fake_zk(cluster, "node1")
        node2_zk = get_fake_zk(cluster, "node1")
        node1_zk.create("/manual_remove", b"aaaa", ttl=5000)
        assert node1_zk.exists("/manual_remove")
        assert node2_zk.exists("/manual_remove")

        node1_zk.delete("/manual_remove")
        assert not node1_zk.exists("/manual_remove")
        assert not node2_zk.exists("/manual_remove")

        time.sleep(5.5)
        assert not node1_zk.exists("/manual_remove")
        assert not node2_zk.exists("/manual_remove")
    finally:
        cluster.shutdown()
        if node1_zk:
            node1_zk.stop()
            node1_zk.close()
        if node2_zk:
            node2_zk.stop()
            node2_zk.close()


def test_many_nodes_with_different_ttls():
    run_uuid = uuid.uuid4()
    cluster = ClickHouseCluster(__file__, str(run_uuid))
    cluster.add_instance(
        "node1", main_configs=["configs/enable_keeper1.xml"], stay_alive=True, with_remote_database_disk=False,
    )
    cluster.add_instance(
        "node2", main_configs=["configs/enable_keeper2.xml"], stay_alive=True, with_remote_database_disk=False,
    )

    node1_zk = None
    node2_zk = None
    cluster.start()

    try:
        node1_zk = get_fake_zk(cluster, "node1")
        node2_zk = get_fake_zk(cluster, "node2")

        for i in range(10):
            node1_zk.create(f"/n{i}", str(i).encode(), ttl=1000 * (i + 1))

        wait_nodes_gone([node1_zk, node2_zk], ["/n0"])
        for i in range(1, 10):
            assert node1_zk.exists(f"/n{i}")
            assert node2_zk.exists(f"/n{i}")

        wait_nodes_gone([node1_zk, node2_zk], ["/n1", "/n2"])
        for i in range(3, 10):
            assert node1_zk.exists(f"/n{i}")
            assert node2_zk.exists(f"/n{i}")
    finally:
        cluster.shutdown()
        if node1_zk:
            node1_zk.stop()
            node1_zk.close()
        if node2_zk:
            node2_zk.stop()
            node2_zk.close()

def test_sibling_ttl_independence():
    run_uuid = uuid.uuid4()
    cluster = ClickHouseCluster(__file__, str(run_uuid))
    cluster.add_instance(
        "node1", main_configs=["configs/enable_keeper1.xml"], stay_alive=True, with_remote_database_disk=False,
    )
    cluster.add_instance(
        "node2", main_configs=["configs/enable_keeper2.xml"], stay_alive=True, with_remote_database_disk=False,
    )

    node1_zk = None
    node2_zk = None
    cluster.start()

    try:
        node1_zk = get_fake_zk(cluster, "node1")
        node2_zk = get_fake_zk(cluster, "node2")
        node1_zk.create("/root", b"root")
        node1_zk.create("/root/a", b"a", ttl=1000)
        node1_zk.create("/root/b", b"b", ttl=3000)

        wait_nodes_gone([node1_zk, node2_zk], ["/root/a"])
        assert node1_zk.exists("/root/b")
        assert node1_zk.exists("/root")
        assert node2_zk.exists("/root/b")
        assert node2_zk.exists("/root")

        wait_nodes_gone([node1_zk, node2_zk], ["/root/b"])
        assert node1_zk.exists("/root")
        assert node2_zk.exists("/root")
    finally:
        cluster.shutdown()
        if node1_zk:
            node1_zk.stop()
            node1_zk.close()
        if node2_zk:
            node2_zk.stop()
            node2_zk.close()

def test_recreate_node_after_ttl_expiration():
    run_uuid = uuid.uuid4()
    cluster = ClickHouseCluster(__file__, str(run_uuid))
    cluster.add_instance(
        "node1", main_configs=["configs/enable_keeper1.xml"], stay_alive=True, with_remote_database_disk=False,
    )
    cluster.add_instance(
        "node2", main_configs=["configs/enable_keeper2.xml"], stay_alive=True, with_remote_database_disk=False,
    )

    node1_zk = None
    node2_zk = None
    cluster.start()

    try:
        node1_zk = get_fake_zk(cluster, "node1")
        node2_zk = get_fake_zk(cluster, "node2")
        node1_zk.create("/recreate", b"old", ttl=1000)

        wait_nodes_gone([node1_zk, node2_zk], ["/recreate"])

        node1_zk.create("/recreate", b"new")
        wait_nodes_exist([node1_zk, node2_zk], ["/recreate"])

        time.sleep(1.2)
        assert node1_zk.exists("/recreate")
        assert node2_zk.exists("/recreate")
    finally:
        cluster.shutdown()
        if node1_zk:
            node1_zk.stop()
            node1_zk.close()
        if node2_zk:
            node2_zk.stop()
            node2_zk.close()
