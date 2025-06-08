#!/usr/bin/env python3
import random
import string

import pytest
from kazoo.client import KazooClient
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__, keeper_config_dir="configs/")

# clickhouse itself will use external zookeeper
node = cluster.add_instance(
    "node",
    stay_alive=True,
    with_zookeeper=True,
    with_remote_database_disk=False,  # Disable `with_remote_database_disk` as the test does not use the default Keeper.
    main_configs=["configs/setting.xml"],
)


def random_string(length):
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=length))


def get_connection_zk(nodename, timeout=30.0):
    # NOTE: here we need KazooClient without implicit retries! (KazooClientWithImplicitRetries)
    _fake_zk_instance = KazooClient(
        hosts=cluster.get_instance_ip(nodename) + ":2181", timeout=timeout
    )
    _fake_zk_instance.start()
    return _fake_zk_instance


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def test_soft_limit_create(started_cluster):
    if node.is_built_with_sanitizer():
        pytest.skip("Disabled for sanitizers")
    started_cluster.wait_zookeeper_to_start()
    node_zk = get_connection_zk("zoo1")
    try:
        loop_time = 100000
        node_zk.create("/test_soft_limit", b"abc")
        path = "/test_soft_limit"

        for i in range(loop_time):
            name = "node_" + str(i)
            node.query(f"INSERT INTO system.zookeeper (name, path, value) values ('{name}', '{path}', repeat('a', 3000))")
    except Exception as e:
        # the message contains out of memory so the users will not be confused.
        assert 'out of memory' in str(e).lower()
        txn = node_zk.transaction()
        for i in range(10):
            txn.delete("/test_soft_limit/node_" + str(i))

        txn.create("/test_soft_limit/node_1000001" + str(i), b"abcde")
        txn.commit()
        assert "0\n"  == node.query("select sum(ProfileEvent_ZooKeeperHardwareExceptions) from system.metric_log")
        return

    raise Exception("all records are inserted but no error occurs")
