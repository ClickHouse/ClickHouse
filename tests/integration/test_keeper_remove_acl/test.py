#!/usr/bin/env python3

import pytest
import logging

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster

from kazoo.security import make_digest_acl
from kazoo.exceptions import NoAuthError


cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/enable_keeper1.xml", "configs/check_node_acl_on_remove.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def wait_nodes():
    keeper_utils.wait_nodes(cluster, [node])


def get_fake_zk(nodename, timeout=30.0):
    return keeper_utils.get_fake_zk(cluster, nodename, timeout=timeout)


def stop_zk_connection(zk_conn):
    zk_conn.stop()
    zk_conn.close()


def test_server_restart(started_cluster):
    try:
        wait_nodes()

        node.stop_clickhouse()
        node.replace_in_config(
            "/etc/clickhouse-server/config.d/check_node_acl_on_remove.xml", "1", "0"
        )
        node.start_clickhouse()

        def create_node_with_acl():
            node_zk = get_fake_zk("node")
            node_zk.add_auth("digest", "clickhouse:password")

            if node_zk.exists("/test_acl_node"):
                node_zk.delete("/test_acl_node")

            acl = make_digest_acl("clickhouse", "password", all=True)
            node_zk.create("/test_acl_node", b"test_data", acl=[acl])
            stop_zk_connection(node_zk)

        def delete_node():
            node_zk = get_fake_zk("node")
            node_zk.delete("/test_acl_node")
            stop_zk_connection(node_zk)

        create_node_with_acl()
        delete_node()
        node.stop_clickhouse()
        node.replace_in_config(
            "/etc/clickhouse-server/config.d/check_node_acl_on_remove.xml", "0", "1"
        )
        node.start_clickhouse()

        create_node_with_acl()

        with pytest.raises(NoAuthError):
            delete_node()
    finally:
        try:
            stop_zk_connection(
                node_zk,
            )
        except:
            pass
