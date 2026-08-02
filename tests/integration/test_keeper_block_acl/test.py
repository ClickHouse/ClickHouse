#!/usr/bin/env python3

import time
import pytest
from kazoo.client import KazooClient
from kazoo.security import make_acl, make_digest_acl
from kazoo.exceptions import NoAuthError, AuthFailedError, BadVersionError, NoNodeError

from helpers.cluster import ClickHouseCluster

from helpers import keeper_utils as ku

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/keeper_config.xml"],
    with_zookeeper=True,
    stay_alive=True,
)

def get_fake_zk_with_auth(username, password):
    zk = ku.get_fake_zk(cluster, "node")
    zk.add_auth("digest", f"{username}:{password}")
    return zk

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()

def wait_node():
    ku.wait_nodes(cluster, [node])

def setup_node():
    node.stop_clickhouse()
    node.exec_in_container(["rm", "-rf", "/var/lib/clickhouse/coordination/log"])
    node.exec_in_container(
        ["rm", "-rf", "/var/lib/clickhouse/coordination/snapshots"]
    )
    set_acl_blocking(0)
    set_snapshot_on_exit(0)
    node.start_clickhouse()
    wait_node()


def set_acl_blocking(value):
    node.replace_in_config(
        "/etc/clickhouse-server/config.d/keeper_config.xml",
        "<cleanup_old_and_ignore_new_acl>[01]</cleanup_old_and_ignore_new_acl>",
        f"<cleanup_old_and_ignore_new_acl>{value}</cleanup_old_and_ignore_new_acl>",
    )


def set_snapshot_on_exit(value):
    node.replace_in_config(
        "/etc/clickhouse-server/config.d/keeper_config.xml",
        "<create_snapshot_on_exit>[01]</create_snapshot_on_exit>",
        f"<create_snapshot_on_exit>{value}</create_snapshot_on_exit>",
    )


def test_keeper_block_acl(started_cluster):
    setup_node()

    zk_no_auth = None
    zk_with_auth = None

    def close_zk(zk):
        if zk is not None:
            zk.stop()
            zk.close()
            zk = None

    try:
        zk_with_auth = get_fake_zk_with_auth("antonio", "isthebest")

        acl = [make_acl("auth", "", all=True)]
        zk_with_auth.create("/test_auth_node", b"test_data", acl=acl)
        zk_with_auth.create("/test_auth_node/child", b"child_data", acl=acl)

        close_zk(zk_with_auth)

        def check_acl_enabled():
            zk_with_auth = get_fake_zk_with_auth("antonio", "isthebest")

            assert zk_with_auth.get("/test_auth_node")[0] == b"test_data"
            assert zk_with_auth.get("/test_auth_node/child")[0] == b"child_data"

            close_zk(zk_with_auth)

            zk_no_auth = ku.get_fake_zk(cluster, "node")
            zk_no_auth.start()

            with pytest.raises(NoAuthError):
                zk_no_auth.get("/test_auth_node")

            with pytest.raises(NoAuthError):
                zk_no_auth.get("/test_auth_node/child")

            close_zk(zk_no_auth)

        def check_acl_disabled():
            zk_with_auth = get_fake_zk_with_auth("antonio", "isthebest")

            assert zk_with_auth.get("/test_auth_node")[0] == b"test_data"
            assert zk_with_auth.get("/test_auth_node/child")[0] == b"child_data"

            close_zk(zk_with_auth)

            zk_no_auth = ku.get_fake_zk(cluster, "node")
            zk_no_auth.start()

            assert zk_no_auth.get("/test_auth_node")[0] == b"test_data"
            assert zk_no_auth.get("/test_auth_node/child")[0] == b"child_data"

            close_zk(zk_no_auth)

        check_acl_enabled()


        # enable blocking, all requests are applied from changelog
        # but ACL is ignored
        node.stop_clickhouse()
        set_acl_blocking(1)
        node.start_clickhouse()
        wait_node()

        check_acl_disabled()

        # we disable acl blocking again
        # because nothing is stored in snapshot, logs are reapplied
        # but this time with ACL
        node.stop_clickhouse()
        set_acl_blocking(0)
        set_snapshot_on_exit(1)
        node.start_clickhouse()
        wait_node()

        check_acl_enabled()

        # we enable acl blocking again
        # this time we load nodes from snapshot
        node.stop_clickhouse()
        set_acl_blocking(1)
        node.start_clickhouse()
        wait_node()

        check_acl_disabled()

        # blocking is enabled, we try to create a node and verifying
        # that it will be written to snapshot without ACL
        zk_with_auth = get_fake_zk_with_auth("antonio", "isthebest")
        acl = [make_acl("auth", "", all=True)]
        zk_with_auth.create("/test_auth_node_new", b"test_data", acl=acl)
        close_zk(zk_with_auth)

        zk_no_auth = ku.get_fake_zk(cluster, "node")
        zk_no_auth.start()
        assert zk_no_auth.get("/test_auth_node_new")[0] == b"test_data"
        close_zk(zk_no_auth)

        node.stop_clickhouse()
        set_acl_blocking(0)
        node.start_clickhouse()
        wait_node()

        zk_no_auth = ku.get_fake_zk(cluster, "node")
        zk_no_auth.start()
        assert zk_no_auth.get("/test_auth_node_new")[0] == b"test_data"
        close_zk(zk_no_auth)
    finally:
        for zk in [zk_with_auth, zk_no_auth]:
            close_zk(zk)
