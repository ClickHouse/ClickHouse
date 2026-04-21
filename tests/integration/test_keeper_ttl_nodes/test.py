#!/usr/bin/env python3

import uuid
import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster
import time

def get_fake_zk(cluster, nodename, timeout=30.0):
    return keeper_utils.get_fake_zk(cluster, nodename, timeout=timeout)


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
    assert node1_zk.exists("/test_alive")
    assert node2_zk.exists("/test_alive")
    time.sleep(1.1)
    assert not node1_zk.exists("/test_alive")
    assert not node2_zk.exists("/test_alive")

    node1_zk.create("/test_alive", b"aaaa", ttl=1)
    time.sleep(0.1)
    assert not node1_zk.exists("/test_alive")
    assert not node2_zk.exists("/test_alive")

    cluster.shutdown()

    if node1_zk:
        node1_zk.stop()
        node1_zk.close()
    if node2_zk:
        node2_zk.stop()
        node2_zk.close()

def test_dont_remove_until_children_alive():
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
    node1_zk.create("/test_alive/child", b"aaaa", ttl=3000)
    node1_zk.create("/test_alive/child2", b"aaaa", ttl=4000)
    assert node1_zk.exists("/test_alive")
    assert node2_zk.exists("/test_alive")
    time.sleep(1.1)
    assert node1_zk.exists("/test_alive")
    assert node1_zk.exists("/test_alive/child")
    assert node1_zk.exists("/test_alive/child2")
    assert node2_zk.exists("/test_alive")
    assert node2_zk.exists("/test_alive/child")
    assert node2_zk.exists("/test_alive/child2")

    time.sleep(2.0)
    assert node1_zk.exists("/test_alive")
    assert not node1_zk.exists("/test_alive/child")
    assert node1_zk.exists("/test_alive/child2")
    assert node2_zk.exists("/test_alive")
    assert not node2_zk.exists("/test_alive/child")
    assert node2_zk.exists("/test_alive/child2")

    time.sleep(1.1)
    assert not node1_zk.exists("/test_alive")
    assert not node1_zk.exists("/test_alive/child")
    assert not node1_zk.exists("/test_alive/child2")
    assert not node2_zk.exists("/test_alive")
    assert not node2_zk.exists("/test_alive/child")
    assert not node2_zk.exists("/test_alive/child2")

    cluster.shutdown()

    if node1_zk:
        node1_zk.stop()
        node1_zk.close()
    if node2_zk:
        node2_zk.stop()
        node2_zk.close()

def test_dont_remove_until_children_alive2():
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
    node1_zk.create("/test_alive/child", b"aaaa", ttl=3000)
    node1_zk.create("/test_alive/child2", b"aaaa")
    assert node1_zk.exists("/test_alive")
    assert node2_zk.exists("/test_alive")
    time.sleep(1.1)
    assert node1_zk.exists("/test_alive")
    assert node1_zk.exists("/test_alive/child")
    assert node1_zk.exists("/test_alive/child2")
    assert node2_zk.exists("/test_alive")
    assert node2_zk.exists("/test_alive/child")
    assert node2_zk.exists("/test_alive/child2")

    time.sleep(2.0)
    assert node1_zk.exists("/test_alive")
    assert not node1_zk.exists("/test_alive/child")
    assert node1_zk.exists("/test_alive/child2")
    assert node2_zk.exists("/test_alive")
    assert not node2_zk.exists("/test_alive/child")
    assert node2_zk.exists("/test_alive/child2")

    node1_zk.delete("/test_alive/child2")
    time.sleep(0.5)
    assert not node1_zk.exists("/test_alive")
    assert not node1_zk.exists("/test_alive/child")
    assert not node1_zk.exists("/test_alive/child2")
    assert not node2_zk.exists("/test_alive")
    assert not node2_zk.exists("/test_alive/child")
    assert not node2_zk.exists("/test_alive/child2")

    cluster.shutdown()

    if node1_zk:
        node1_zk.stop()
        node1_zk.close()
    if node2_zk:
        node2_zk.stop()
        node2_zk.close()

def test_child_with_smaller_ttl_expires_before_parent():
    run_uuid = uuid.uuid4()
    cluster = ClickHouseCluster(__file__, str(run_uuid))
    cluster.add_instance(
        "node1", main_configs=["configs/enable_keeper1.xml"], stay_alive=True, with_remote_database_disk=False,
    )
    cluster.add_instance(
        "node2", main_configs=["configs/enable_keeper2.xml"], stay_alive=True, with_remote_database_disk=False,
    )

    node1_zk = None
    cluster.start()

    try:
        node1_zk = get_fake_zk(cluster, "node1")
        node2_zk = get_fake_zk(cluster, "node1")
        node1_zk.create("/parent", b"aaaa", ttl=4000)
        node1_zk.create("/parent/child", b"bbbb", ttl=1000)

        assert node1_zk.exists("/parent")
        assert node1_zk.exists("/parent/child")
        assert node2_zk.exists("/parent")
        assert node2_zk.exists("/parent/child")

        time.sleep(1.2)
        assert node1_zk.exists("/parent")
        assert not node1_zk.exists("/parent/child")
        assert node2_zk.exists("/parent")
        assert not node2_zk.exists("/parent/child")

        time.sleep(3.1)
        assert not node1_zk.exists("/parent")
        assert not node2_zk.exists("/parent")
    finally:
        cluster.shutdown()
        if node1_zk:
            node1_zk.stop()
            node1_zk.close()
        if node2_zk:
            node2_zk.stop()
            node2_zk.close()

def test_deep_hierarchy_ttl_cleanup():
    run_uuid = uuid.uuid4()
    cluster = ClickHouseCluster(__file__, str(run_uuid))
    cluster.add_instance(
        "node1", main_configs=["configs/enable_keeper1.xml"], stay_alive=True, with_remote_database_disk=False,
    )
    cluster.add_instance(
        "node2", main_configs=["configs/enable_keeper2.xml"], stay_alive=True, with_remote_database_disk=False,
    )

    node1_zk = None
    cluster.start()

    try:
        node1_zk = get_fake_zk(cluster, "node1")
        node2_zk = get_fake_zk(cluster, "node2")
        node1_zk.create("/a", b"a", ttl=1000)
        node1_zk.create("/a/b", b"b", ttl=2000)
        node1_zk.create("/a/b/c", b"c", ttl=3000)

        time.sleep(1.1)
        assert node1_zk.exists("/a")
        assert node1_zk.exists("/a/b")
        assert node1_zk.exists("/a/b/c")
        assert node2_zk.exists("/a")
        assert node2_zk.exists("/a/b")
        assert node2_zk.exists("/a/b/c")

        time.sleep(1.1)
        assert node1_zk.exists("/a")
        assert node1_zk.exists("/a/b")
        assert node1_zk.exists("/a/b/c")
        assert node2_zk.exists("/a")
        assert node2_zk.exists("/a/b")
        assert node2_zk.exists("/a/b/c")

        time.sleep(1.1)
        assert not node1_zk.exists("/a")
        assert not node1_zk.exists("/a/b")
        assert not node1_zk.exists("/a/b/c")
        assert not node2_zk.exists("/a")
        assert not node2_zk.exists("/a/b")
        assert not node2_zk.exists("/a/b/c")
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
    cluster.start()

    try:
        node1_zk = get_fake_zk(cluster, "node1")
        node2_zk = get_fake_zk(cluster, "node2")

        for i in range(10):
            node1_zk.create(f"/n{i}", str(i).encode(), ttl=1000 * (i + 1))

        time.sleep(1.2)
        assert not node1_zk.exists("/n0")
        assert not node2_zk.exists("/n0")
        for i in range(1, 10):
            assert node1_zk.exists(f"/n{i}")
            assert node2_zk.exists(f"/n{i}")

        time.sleep(2.1)
        assert not node1_zk.exists("/n1")
        assert not node1_zk.exists("/n2")
        assert not node2_zk.exists("/n1")
        assert not node2_zk.exists("/n2")
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
    cluster.start()

    try:
        node1_zk = get_fake_zk(cluster, "node1")
        node2_zk = get_fake_zk(cluster, "node2")
        node1_zk.create("/root", b"root")
        node1_zk.create("/root/a", b"a", ttl=1000)
        node1_zk.create("/root/b", b"b", ttl=3000)

        time.sleep(1.2)
        assert not node1_zk.exists("/root/a")
        assert node1_zk.exists("/root/b")
        assert node1_zk.exists("/root")
        assert not node2_zk.exists("/root/a")
        assert node2_zk.exists("/root/b")
        assert node2_zk.exists("/root")

        time.sleep(2.1)
        assert not node1_zk.exists("/root/b")
        assert node1_zk.exists("/root")
        assert not node2_zk.exists("/root/b")
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
    cluster.start()

    try:
        node1_zk = get_fake_zk(cluster, "node1")
        node2_zk = get_fake_zk(cluster, "node2")
        node1_zk.create("/recreate", b"old", ttl=1000)

        time.sleep(1.2)
        assert not node1_zk.exists("/recreate")
        assert not node2_zk.exists("/recreate")

        node1_zk.create("/recreate", b"new")
        assert node1_zk.exists("/recreate")
        assert node2_zk.exists("/recreate")

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
