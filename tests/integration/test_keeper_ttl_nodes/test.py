#!/usr/bin/env python3

import threading
import uuid
import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster
import time

def get_fake_zk(cluster, nodename, timeout=30.0):
    return keeper_utils.get_fake_zk(cluster, nodename, timeout=timeout)


def test_smoke():
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

    try:
        cluster.start()

        node1_zk = get_fake_zk(cluster, "node1")
        node1_zk.create("/test_alive", b"aaaa", ttl=1000)
        assert node1_zk.exists("/test_alive")
        time.sleep(2)
        assert not node1_zk.exists("/test_alive")

    finally:
        cluster.shutdown()

        if node1_zk:
            node1_zk.stop()
            node1_zk.close()

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

def test_increase_time_to_destroy():
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
    time.sleep(0.5)
    assert node1_zk.exists("/test_alive")
    assert node2_zk.exists("/test_alive")
    node1_zk.set("/test_alive", b"foo")
    time.sleep(0.7)
    assert node1_zk.exists("/test_alive")
    assert node2_zk.exists("/test_alive")
    time.sleep(1.0)

    assert not node1_zk.exists("/test_alive")
    assert not node2_zk.exists("/test_alive")

    cluster.shutdown()

    if node1_zk:
        node1_zk.stop()
        node1_zk.close()
    if node2_zk:
        node2_zk.stop()
        node2_zk.close()


def test_concurrent_set_and_gc_delete():
    run_uuid = uuid.uuid4()
    cluster = ClickHouseCluster(__file__, str(run_uuid))
    n1 = cluster.add_instance(
        "node1", main_configs=["configs/enable_keeper1.xml"], stay_alive=True, with_remote_database_disk=False,
    )
    n2 = cluster.add_instance(
        "node2", main_configs=["configs/enable_keeper2.xml"], stay_alive=True, with_remote_database_disk=False,
    )

    node1_zk = None
    node2_zk = None

    try:
        cluster.start()
        keeper_utils.wait_nodes(cluster, [n1, n2])

        leader = keeper_utils.get_leader(cluster, [n1, n2])

        node1_zk = get_fake_zk(cluster, "node1")
        node2_zk = get_fake_zk(cluster, "node2")

        node1_zk.create("/race_node", b"original", ttl=1000)
        assert node1_zk.exists("/race_node")

        time.sleep(1.1)

        leader.query("SYSTEM ENABLE FAILPOINT keeper_ttl_gc_pause_before_remove")

        time.sleep(0.3)

        set_error = [None]
        set_done = threading.Event()

        def do_set():
            try:
                node1_zk.set("/race_node", b"refreshed")
            except Exception as e:
                set_error[0] = e
            finally:
                set_done.set()

        setter = threading.Thread(target=do_set, daemon=True)
        setter.start()
        set_done.wait(timeout=5)
        assert set_done.is_set(), "`set` did not complete while GC was paused"

        leader.query("SYSTEM DISABLE FAILPOINT keeper_ttl_gc_pause_before_remove")

        time.sleep(0.3)

        if set_error[0] is not None:
            assert not node1_zk.exists("/race_node"), (
                f"`set` raised {set_error[0]!r} but the node still exists — inconsistent state"
            )
        else:
            assert node1_zk.exists("/race_node"), (
                "TTL node was deleted even though a concurrent `set` returned ZOK and refreshed its TTL"
            )
            assert node2_zk.exists("/race_node"), (
                "TTL node was deleted on the follower even though a concurrent `set` returned ZOK"
            )

            data, _ = node1_zk.get("/race_node")
            assert data == b"refreshed", f"Unexpected node data after successful `set`: {data!r}"

            time.sleep(1.3)
            assert not node1_zk.exists("/race_node"), (
                "TTL node was not removed after the refreshed TTL expired"
            )
            assert not node2_zk.exists("/race_node"), (
                "TTL node was not removed on the follower after the refreshed TTL expired"
            )
    finally:
        cluster.shutdown()
        if node1_zk:
            node1_zk.stop()
            node1_zk.close()
        if node2_zk:
            node2_zk.stop()
            node2_zk.close()
