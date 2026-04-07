from os import path as p

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.keeper_utils import (
    get_active_zk_connections,
    replace_zookeeper_config,
    reset_zookeeper_config,
)
from helpers.test_tools import assert_eq_with_retry
from helpers.utility import random_string

default_zk_config = p.join(p.dirname(p.realpath(__file__)), "configs/zookeeper.xml")
cluster = ClickHouseCluster(__file__, zookeeper_config_path="configs/zookeeper.xml")
node = cluster.add_instance("node", with_zookeeper=True)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def test_reload_zookeeper(start_cluster):
    # random is used for flaky tests, where ZK is not fast enough to clear the node
    node.query(
        f"""
        CREATE TABLE test_table(date Date, id UInt32)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/shard1/{random_string(7)}/test_table', '1')
        PARTITION BY toYYYYMM(date)
        ORDER BY id
        """
    )
    node.query(
        "INSERT INTO test_table(date, id) select today(), number FROM numbers(1000)"
    )

    ## remove zoo2, zoo3 from configs
    new_config = """
<clickhouse>
    <zookeeper>
        <node index="1">
            <host>zoo1</host>
            <port>2181</port>
        </node>
        <session_timeout_ms>2000</session_timeout_ms>
    </zookeeper>
</clickhouse>
"""
    replace_zookeeper_config(node, new_config)
    ## config reloads, but can still work
    assert_eq_with_retry(
        node, "SELECT COUNT() FROM test_table", "1000", retry_count=120, sleep_time=0.5
    )

    ## stop all zookeepers, table will be readonly
    cluster.stop_zookeeper_nodes(["zoo1", "zoo2", "zoo3"])
    node.query("SELECT COUNT() FROM test_table")
    with pytest.raises(QueryRuntimeException):
        node.query(
            "SELECT COUNT() FROM test_table",
            settings={"select_sequential_consistency": 1},
        )

    ## start zoo2, zoo3, table will be readonly too, because it only connect to zoo1
    cluster.start_zookeeper_nodes(["zoo2", "zoo3"])
    cluster.wait_zookeeper_nodes_to_start(["zoo2", "zoo3"])
    node.query("SELECT COUNT() FROM test_table")
    with pytest.raises(QueryRuntimeException):
        node.query(
            "SELECT COUNT() FROM test_table",
            settings={"select_sequential_consistency": 1},
        )

    ## set config to zoo2, server will be normal
    new_config = """
<clickhouse>
    <zookeeper>
        <node index="1">
            <host>zoo2</host>
            <port>2181</port>
        </node>
        <session_timeout_ms>2000</session_timeout_ms>
    </zookeeper>
</clickhouse>
"""
    replace_zookeeper_config(node, new_config)

    active_zk_connections = get_active_zk_connections(node)
    assert (
        len(active_zk_connections) == 1
    ), "Total connections to ZooKeeper not equal to 1, {}".format(active_zk_connections)

    assert_eq_with_retry(
        node, "SELECT COUNT() FROM test_table", "1000", retry_count=120, sleep_time=0.5
    )

    active_zk_connections = get_active_zk_connections(node)
    assert (
        len(active_zk_connections) == 1
    ), "Total connections to ZooKeeper not equal to 1, {}".format(active_zk_connections)
    # Reset cluster state
    cluster.start_zookeeper_nodes(["zoo1", "zoo2", "zoo3"])
    cluster.wait_zookeeper_nodes_to_start(["zoo1", "zoo2", "zoo3"])
    reset_zookeeper_config(node, default_zk_config)
    node.query("DROP TABLE test_table")
