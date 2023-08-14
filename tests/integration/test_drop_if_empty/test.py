import re
import time

import pytest
import requests
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__, zookeeper_config_path="configs/zookeeper.xml")

main_configs = [
    "configs/remote_servers.xml",
]

node1 = cluster.add_instance(
    "node1",
    main_configs=main_configs + ["configs/drop_if_empty_check.xml"],
    macros={"replica": "node1", "shard": "shard1"},
    with_zookeeper=True,
)

node2 = cluster.add_instance(
    "node2",
    main_configs=main_configs + ["configs/drop_if_empty_check.xml"],
    macros={"replica": "node2", "shard": "shard1"},
    with_zookeeper=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_drop_if_empty(start_cluster):
    node1.query(
        "CREATE TABLE default.tbl ON CLUSTER 'cluster' ("
        "x UInt64"
        ") ENGINE=ReplicatedMergeTree('/clickhouse/tables/tbl/', '{replica}')"
        "ORDER BY x"
    )

    time.sleep(5)

    assert 1 == int(node2.query("SELECT count() FROM system.tables WHERE name = 'tbl';"))

    node2.query("SYSTEM STOP MERGES;")
    node2.query("SYSTEM STOP FETCHES;")
    node2.query("SYSTEM STOP REPLICATION QUEUES;")

    node1.query("INSERT INTO default.tbl SELECT * FROM system.numbers_mt LIMIT 10000;")
    time.sleep(5)
    assert 0 == int(node2.query("SELECT count() FROM default.tbl;"))

    # node2.query("DROP TABLE IF EMPTY default.tbl ON CLUSTER 'cluster';")
    node2.query("DROP TABLE IF EMPTY default.tbl ON CLUSTER 'cluster';")
    # node2.query("DROP TABLE IF EMPTY default.tbl;")


    assert 0 == int(node1.query("SELECT count() FROM system.tables WHERE name = 'tbl';"))
    assert 0 == int(node2.query("SELECT count() FROM system.tables WHERE name = 'tbl';"))
