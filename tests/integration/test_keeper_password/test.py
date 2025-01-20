#!/usr/bin/env python3

import pytest

from helpers import keeper_utils
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/keeper_config.xml"],
    with_zookeeper=True,
    use_keeper=False,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/keeper_config_hashed.xml"],
    with_zookeeper=True,
    use_keeper=False,
)

node3 = cluster.add_instance(
    "node3",
    main_configs=["configs/use_keeper_from_second.xml"],
    with_zookeeper=False,
    use_keeper=False,
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        password = "foobar"
        p = (password + "".join([chr(0)] * (16 - len(password)))).encode("ascii")
        keeper_utils.wait_until_connected(cluster, node1, password=p)
        keeper_utils.wait_until_connected(cluster, node2, password=p)

        yield cluster

    finally:
        cluster.shutdown()


def get_zk(nodename, timeout=30.0):
    password = "foobar"
    p = (password + "".join([chr(0)] * (16 - len(password)))).encode("ascii")

    return keeper_utils.get_fake_zk(cluster, nodename, timeout=timeout, password=p)


def get_wrong_password_zk(nodename, timeout=30.0):
    password = "foobarqwe"
    p = (password + "".join([chr(0)] * (16 - len(password)))).encode("ascii")

    return keeper_utils.get_fake_zk(
        cluster, nodename, timeout=timeout, password=p, retries=1
    )


def get_zk_no_password(nodename, timeout=30.0):
    return keeper_utils.get_fake_zk(cluster, nodename, timeout=timeout, retries=1)


def test_password_works(started_cluster):
    auth_connection = get_zk(node1.name)
    auth_connection.get_children("/")

    auth_connection = get_zk(node2.name)
    auth_connection.get_children("/")

    data = node3.exec_in_container(
        [
            "bash",
            "-c",
            "clickhouse keeper-client -h node2 -p 9181 --password foobar -q \"ls '/keeper'\"",
        ],
        privileged=True,
    )
    assert data.strip() == "api_version feature_flags"

    with pytest.raises(Exception):
        auth_connection = get_wrong_password_zk(node1.name, timeout=1)
        auth_connection.get_children("/")

    with pytest.raises(Exception):
        auth_connection = get_wrong_password_zk(node2.name, timeout=1)
        auth_connection.get_children("/")

    with pytest.raises(Exception):
        auth_connection = get_zk_no_password(node1.name, timeout=1)
        auth_connection.get_children("/")

    with pytest.raises(Exception):
        auth_connection = get_zk_no_password(node2.name, timeout=1)
        auth_connection.get_children("/")

    print(node3.query("select * from system.zookeeper where path = '/'", timeout=3))

    node3.replace_in_config(
        "/etc/clickhouse-server/config.d/use_keeper_from_second.xml",
        "foobar",
        "qqqqqq",
    )

    with pytest.raises(Exception):
        node3.restart_clickhouse(stop_start_wait_sec=10)
        print(node3.query("select * from system.zookeeper where path = '/'", timeout=3))
