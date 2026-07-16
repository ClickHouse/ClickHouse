#!/usr/bin/env python3

import time
import uuid

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster

CONTAINER_EPHEMERAL_OWNER = -(2**63)  # ZooKeeper's Long.MIN_VALUE sentinel for container nodes


def get_fake_zk(cluster, nodename, timeout=30.0):
    return keeper_utils.get_fake_zk(cluster, nodename, timeout=timeout)


def wait_nodes_gone(zks, paths, timeout=30.0):
    deadline = time.monotonic() + timeout
    for zk in zks:
        for path in paths:
            while zk.exists(path) is not None:
                if time.monotonic() >= deadline:
                    raise AssertionError(
                        f"{path} still exists on Keeper after {timeout}s"
                    )
                time.sleep(0.05)


def wait_nodes_exist(zks, paths, timeout=30.0):
    deadline = time.monotonic() + timeout
    for zk in zks:
        for path in paths:
            while zk.exists(path) is None:
                if time.monotonic() >= deadline:
                    raise AssertionError(
                        f"{path} did not appear on Keeper within {timeout}s"
                    )
                time.sleep(0.05)


def stop_zk(zk):
    try:
        if zk:
            zk.stop()
            zk.close()
    except Exception:
        pass


def test_gc_on_last_child_delete():
    run_uuid = uuid.uuid4()
    cluster = ClickHouseCluster(__file__, str(run_uuid))
    cluster.add_instance(
        "node1",
        main_configs=["configs/enable_keeper1.xml"],
        stay_alive=True,
        with_remote_database_disk=False,
    )
    cluster.add_instance(
        "node2",
        main_configs=["configs/enable_keeper2.xml"],
        stay_alive=True,
        with_remote_database_disk=False,
    )

    node1_zk = node2_zk = None
    cluster.start()
    try:
        node1_zk = get_fake_zk(cluster, "node1")
        node2_zk = get_fake_zk(cluster, "node2")

        node1_zk.create("/cont", b"", container=True)
        node1_zk.create("/cont/child", b"data")
        wait_nodes_exist([node1_zk, node2_zk], ["/cont", "/cont/child"])

        node1_zk.delete("/cont/child")
        # container with no children must be GC'd by the leader
        wait_nodes_gone([node1_zk, node2_zk], ["/cont"])
    finally:
        cluster.shutdown()
        stop_zk(node1_zk)
        stop_zk(node2_zk)


def test_children_protect_container():
    run_uuid = uuid.uuid4()
    cluster = ClickHouseCluster(__file__, str(run_uuid))
    cluster.add_instance(
        "node1",
        main_configs=["configs/enable_keeper1.xml"],
        stay_alive=True,
        with_remote_database_disk=False,
    )
    cluster.add_instance(
        "node2",
        main_configs=["configs/enable_keeper2.xml"],
        stay_alive=True,
        with_remote_database_disk=False,
    )

    node1_zk = node2_zk = None
    cluster.start()
    try:
        node1_zk = get_fake_zk(cluster, "node1")
        node2_zk = get_fake_zk(cluster, "node2")

        node1_zk.create("/guarded", b"", container=True)
        node1_zk.create("/guarded/a", b"x")
        node1_zk.create("/guarded/b", b"y")
        wait_nodes_exist([node1_zk, node2_zk], ["/guarded", "/guarded/a", "/guarded/b"])

        # delete one child — container must survive
        node1_zk.delete("/guarded/a")
        time.sleep(1.0)
        assert node1_zk.exists("/guarded") is not None
        assert node2_zk.exists("/guarded") is not None
        assert node1_zk.exists("/guarded/b") is not None

        # delete last child — now the container must go
        node1_zk.delete("/guarded/b")
        wait_nodes_gone([node1_zk, node2_zk], ["/guarded"])
    finally:
        cluster.shutdown()
        stop_zk(node1_zk)
        stop_zk(node2_zk)


def test_ephemeral_owner_stat():
    run_uuid = uuid.uuid4()
    cluster = ClickHouseCluster(__file__, str(run_uuid))
    cluster.add_instance(
        "node1",
        main_configs=["configs/enable_keeper1.xml"],
        stay_alive=True,
        with_remote_database_disk=False,
    )
    cluster.add_instance(
        "node2",
        main_configs=["configs/enable_keeper2.xml"],
        stay_alive=True,
        with_remote_database_disk=False,
    )

    node1_zk = node2_zk = None
    cluster.start()
    try:
        node1_zk = get_fake_zk(cluster, "node1")
        node2_zk = get_fake_zk(cluster, "node2")

        node1_zk.create("/statcheck", b"", container=True)
        node1_zk.create("/statcheck/child", b"")
        wait_nodes_exist([node1_zk, node2_zk], ["/statcheck"])

        _, stat1 = node1_zk.get("/statcheck")
        assert stat1.ephemeralOwner == CONTAINER_EPHEMERAL_OWNER, (
            f"Expected ephemeralOwner={CONTAINER_EPHEMERAL_OWNER}, got {stat1.ephemeralOwner}"
        )

        node2_zk.sync("/statcheck")
        _, stat2 = node2_zk.get("/statcheck")
        assert stat2.ephemeralOwner == CONTAINER_EPHEMERAL_OWNER, (
            f"Follower: expected ephemeralOwner={CONTAINER_EPHEMERAL_OWNER}, got {stat2.ephemeralOwner}"
        )
    finally:
        cluster.shutdown()
        stop_zk(node1_zk)
        stop_zk(node2_zk)


def test_container_preserved_after_restart():
    """
    After a restart that loads from a snapshot, container nodes must retain their
    container flag so that the GC thread can still delete them once childless.
    """
    run_uuid = uuid.uuid4()
    cluster = ClickHouseCluster(__file__, str(run_uuid))
    node = cluster.add_instance(
        "node",
        main_configs=["configs/enable_keeper_single.xml"],
        stay_alive=True,
        with_remote_database_disk=False,
    )

    node_zk = None
    cluster.start()
    try:
        node_zk = get_fake_zk(cluster, "node")

        node_zk.create("/snap_cont", b"", container=True)
        node_zk.create("/snap_cont/child", b"data")

        # write enough entries to guarantee a snapshot is written (snapshot_distance=10)
        for i in range(15):
            node_zk.create(f"/filler_{i}", str(i).encode())
        node.wait_for_log_line("Created persistent snapshot", timeout=30)

        stop_zk(node_zk)
        node_zk = None

        node.restart_clickhouse(kill=True)
        keeper_utils.wait_until_connected(cluster, node)

        node_zk = get_fake_zk(cluster, "node")

        # container and its child must have survived the restart
        wait_nodes_exist([node_zk], ["/snap_cont", "/snap_cont/child"])

        # ephemeralOwner must still identify the node as a container
        _, stat = node_zk.get("/snap_cont")
        assert stat.ephemeralOwner == CONTAINER_EPHEMERAL_OWNER, (
            f"After restart: expected ephemeralOwner={CONTAINER_EPHEMERAL_OWNER}, got {stat.ephemeralOwner}"
        )

        # GC must still work: deleting the last child triggers container removal
        node_zk.delete("/snap_cont/child")
        wait_nodes_gone([node_zk], ["/snap_cont"])
    finally:
        cluster.shutdown()
        stop_zk(node_zk)


def test_gc_never_used_container():
    """
    A container node that was created empty and never received any children must
    be GC'd after container_gc_max_never_used_interval_ms (2 s in the test config).
    """
    run_uuid = uuid.uuid4()
    cluster = ClickHouseCluster(__file__, str(run_uuid))
    cluster.add_instance(
        "node",
        main_configs=["configs/enable_keeper_never_used_gc.xml"],
        stay_alive=True,
        with_remote_database_disk=False,
    )

    node_zk = None
    cluster.start()
    try:
        node_zk = get_fake_zk(cluster, "node")

        # Create a container node but deliberately add no children.
        node_zk.create("/never_used", b"", container=True)
        wait_nodes_exist([node_zk], ["/never_used"])

        # The container must be GC'd after the grace period.
        wait_nodes_gone([node_zk], ["/never_used"])

        # A container that did have children (and then lost them) must still be
        # cleaned up via the standard path, unaffected by the never-used setting.
        node_zk.create("/used_then_emptied", b"", container=True)
        node_zk.create("/used_then_emptied/child", b"data")
        wait_nodes_exist([node_zk], ["/used_then_emptied", "/used_then_emptied/child"])
        node_zk.delete("/used_then_emptied/child")
        wait_nodes_gone([node_zk], ["/used_then_emptied"])
    finally:
        cluster.shutdown()
        stop_zk(node_zk)
