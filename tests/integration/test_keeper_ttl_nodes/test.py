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

        # Created before the nodes below, so it is a collection candidate for every garbage
        # collector pass that can select any of them: an engine removing a node before its
        # destroy_time removes this one too, and the waits below cannot finish first.
        node1_zk.create("/canary", b"canary", ttl=600000)

        # Ten nodes with distinct TTLs; each must be collected. The relative expiry order is
        # asserted at exact instants in CoordinationTest.TestTTLSiblingExpiryOrdering, since
        # observing it here would race the collector. Keep ttl_step_ms low: the longest TTL is
        # ttl_step_ms * 10 and must land well inside wait_nodes_gone's 30s deadline.
        ttl_step_ms = 1000
        for i in range(10):
            node1_zk.create(f"/n{i}", str(i).encode(), ttl=ttl_step_ms * (i + 1))

        # Created last and awaited on both replicas, so each has applied every create above and a
        # later absence means collected rather than not-yet-applied.
        node1_zk.create("/barrier", b"barrier", ttl=600000)
        wait_nodes_exist([node1_zk, node2_zk], ["/barrier"])

        wait_nodes_gone([node1_zk, node2_zk], [f"/n{i}" for i in range(10)])

        assert node1_zk.exists("/canary")
        assert node2_zk.exists("/canary")
        assert node1_zk.exists("/barrier")
        assert node2_zk.exists("/barrier")
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

        # Created before the TTL children below, so it is a collection candidate for every garbage
        # collector pass that can select either of them: an engine removing a node before its
        # destroy_time removes this one too. Kept outside /root so it cannot interact with the
        # child count of the subtree under test.
        node1_zk.create("/canary", b"canary", ttl=600000)

        node1_zk.create("/root/a", b"a", ttl=1000)
        node1_zk.create("/root/b", b"b", ttl=3000)

        # Created last and awaited on both replicas, so each has applied every create above and a
        # later absence means collected rather than not-yet-applied.
        node1_zk.create("/barrier", b"barrier", ttl=600000)
        wait_nodes_exist([node1_zk, node2_zk], ["/barrier"])

        # Both TTL children must be collected on both replicas. Their relative expiry order is
        # asserted at exact instants in CoordinationTest.TestTTLSiblingExpiryOrdering; asserting
        # it here would require observing a transient window and races the real collector.
        wait_nodes_gone([node1_zk, node2_zk], ["/root/a", "/root/b"])

        # A node created without a TTL has no destroy_time, so no delay can make it disappear.
        assert node1_zk.exists("/root")
        assert node2_zk.exists("/root")
        assert node1_zk.exists("/canary")
        assert node2_zk.exists("/canary")
        assert node1_zk.exists("/barrier")
        assert node2_zk.exists("/barrier")
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
