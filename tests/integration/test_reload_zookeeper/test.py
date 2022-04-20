import time
import pytest
import os

from helpers.cluster import ClickHouseCluster
from helpers.client import QueryRuntimeException
from helpers.test_tools import assert_eq_with_retry


cluster = ClickHouseCluster(__file__, zookeeper_config_path="configs/zookeeper.xml")
node = cluster.add_instance("node", with_zookeeper=True)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        node.query(
            """
        CREATE TABLE test_table(date Date, id UInt32)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/shard1/test/test_table', '1')
        PARTITION BY toYYYYMM(date)
        ORDER BY id
    """
        )

        yield cluster
    finally:
        cluster.shutdown()


def test_reload_zookeeper(start_cluster):
    def wait_zookeeper_node_to_start(zk_nodes, timeout=60):
        start = time.time()
        while time.time() - start < timeout:
            try:
                for instance in zk_nodes:
                    conn = start_cluster.get_kazoo_client(instance)
                    conn.get_children("/")
                print("All instances of ZooKeeper started")
                return
            except Exception as ex:
                print(("Can't connect to ZooKeeper " + str(ex)))
                time.sleep(0.5)

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
    node.replace_config("/etc/clickhouse-server/conf.d/zookeeper.xml", new_config)
    node.query("SYSTEM RELOAD CONFIG")
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
    wait_zookeeper_node_to_start(["zoo2", "zoo3"])
    node.query("SELECT COUNT() FROM test_table")
    with pytest.raises(QueryRuntimeException):
        node.query(
            "SELECT COUNT() FROM test_table",
            settings={"select_sequential_consistency": 1},
        )

    def get_active_zk_connections():
        return str(
            node.exec_in_container(
                [
                    "bash",
                    "-c",
                    "lsof -a -i4 -i6 -itcp -w | grep 2181 | grep ESTABLISHED | wc -l",
                ],
                privileged=True,
                user="root",
            )
        ).strip()

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
    node.replace_config("/etc/clickhouse-server/conf.d/zookeeper.xml", new_config)
    node.query("SYSTEM RELOAD CONFIG")

    active_zk_connections = get_active_zk_connections()
    assert (
        active_zk_connections == "1"
    ), "Total connections to ZooKeeper not equal to 1, {}".format(active_zk_connections)

    assert_eq_with_retry(
        node, "SELECT COUNT() FROM test_table", "1000", retry_count=120, sleep_time=0.5
    )

    active_zk_connections = get_active_zk_connections()
    assert (
        active_zk_connections == "1"
    ), "Total connections to ZooKeeper not equal to 1, {}".format(active_zk_connections)
