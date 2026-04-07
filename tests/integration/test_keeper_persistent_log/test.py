#!/usr/bin/env python3
import os
import random
import string

import pytest
import helpers.keeper_utils as keeper_utils
from helpers.test_tools import get_retry_number

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/enable_keeper.xml", "configs/use_keeper.xml"],
    stay_alive=True,
    with_remote_database_disk=False,  # Disable `with_remote_database_disk` as the test does not use the default Keeper.
)


def random_string(length):
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=length))


def create_random_path(prefix="", depth=1):
    if depth == 0:
        return prefix
    return create_random_path(os.path.join(prefix, random_string(3)), depth - 1)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def get_connection_zk(nodename, timeout=30.0):
    _fake_zk_instance = keeper_utils.get_fake_zk(cluster, nodename, timeout=timeout)
    return _fake_zk_instance


def restart_clickhouse():
    node.restart_clickhouse(kill=True)


def test_state_after_restart(started_cluster, request):
    try:
        node_zk = None
        node_zk2 = None
        node_zk = get_connection_zk("node")

        chroot = f"/test_state_after_restart_{get_retry_number(request)}"
        node_zk.create(chroot, b"somevalue")
        node_zk.chroot = chroot

        strs = []
        for i in range(100):
            strs.append(random_string(123).encode())
            node_zk.create("/node" + str(i), strs[i])

        for i in range(100):
            if i % 7 == 0:
                node_zk.delete("/node" + str(i))

        restart_clickhouse()

        node_zk2 = get_connection_zk("node")
        node_zk2.chroot = chroot

        assert node_zk2.get("/")[0] == b"somevalue"
        for i in range(100):
            if i % 7 == 0:
                assert node_zk2.exists("/node" + str(i)) is None
            else:
                assert len(node_zk2.get("/node" + str(i))[0]) == 123
                assert node_zk2.get("/node" + str(i))[0] == strs[i]
    finally:
        try:
            if node_zk is not None:
                node_zk.stop()
                node_zk.close()

            if node_zk2 is not None:
                node_zk2.stop()
                node_zk2.close()
        except:
            pass


def test_state_duplicate_restart(started_cluster, request):
    try:
        node_zk = None
        node_zk2 = None
        node_zk3 = None
        node_zk = get_connection_zk("node")

        chroot = f"/test_state_duplicated_restart_{get_retry_number(request)}"
        node_zk.create(chroot, b"somevalue")
        node_zk.chroot = chroot

        strs = []
        for i in range(100):
            strs.append(random_string(123).encode())
            node_zk.create("/node" + str(i), strs[i])

        for i in range(100):
            if i % 7 == 0:
                node_zk.delete("/node" + str(i))

        restart_clickhouse()

        node_zk2 = get_connection_zk("node")
        node_zk2.chroot = chroot

        node_zk2.create("/just_test1")
        node_zk2.create("/just_test2")
        node_zk2.create("/just_test3")

        restart_clickhouse()

        node_zk3 = get_connection_zk("node")
        node_zk3.chroot = chroot

        assert node_zk3.get("/")[0] == b"somevalue"
        for i in range(100):
            if i % 7 == 0:
                assert node_zk3.exists("/node" + str(i)) is None
            else:
                assert len(node_zk3.get("/node" + str(i))[0]) == 123
                assert node_zk3.get("/node" + str(i))[0] == strs[i]
    finally:
        try:
            if node_zk is not None:
                node_zk.stop()
                node_zk.close()

            if node_zk2 is not None:
                node_zk2.stop()
                node_zk2.close()

            if node_zk3 is not None:
                node_zk3.stop()
                node_zk3.close()

        except:
            pass


# http://zookeeper-user.578899.n2.nabble.com/Why-are-ephemeral-nodes-written-to-disk-tp7583403p7583418.html
def test_ephemeral_after_restart(started_cluster, request):
    try:
        node_zk = None
        node_zk2 = None
        node_zk = get_connection_zk("node")

        chroot = f"/test_ephemeral_after_restart_{get_retry_number(request)}"
        node_zk.create(chroot, b"somevalue")
        node_zk.chroot = chroot

        strs = []
        for i in range(100):
            strs.append(random_string(123).encode())
            node_zk.create("/node" + str(i), strs[i], ephemeral=True)

        for i in range(100):
            if i % 7 == 0:
                node_zk.delete("/node" + str(i))

        restart_clickhouse()

        node_zk2 = get_connection_zk("node")
        node_zk2.chroot = chroot

        assert node_zk2.get("/")[0] == b"somevalue"
        for i in range(100):
            if i % 7 == 0:
                assert node_zk2.exists("/node" + str(i)) is None
            else:
                assert len(node_zk2.get("/node" + str(i))[0]) == 123
                assert node_zk2.get("/node" + str(i))[0] == strs[i]
    finally:
        try:
            if node_zk is not None:
                node_zk.stop()
                node_zk.close()

            if node_zk2 is not None:
                node_zk2.stop()
                node_zk2.close()
        except:
            pass
