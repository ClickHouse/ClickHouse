import re
import time

import pytest
import requests

from helpers.client import QueryRuntimeException
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
        "CREATE DATABASE replicateddb "
        "ENGINE = Replicated('/clickhouse/databases/replicateddb', 'shard1', 'node1')",
    )
    node2.query(
        "CREATE DATABASE replicateddb "
        "ENGINE = Replicated('/clickhouse/databases/replicateddb', 'shard1', 'node2')",
    )
    node1.query(
        "CREATE TABLE default.tbl ON CLUSTER 'cluster' ("
        "x UInt64"
        ") ENGINE=ReplicatedMergeTree('/clickhouse/tables/tbl/', '{replica}')"
        "ORDER BY x"
    )
    node1.query(
        "CREATE TABLE replicateddb.tbl2 (" "x UInt64" ") ENGINE=MergeTree " "ORDER BY x"
    )

    assert 1 == int(
        node2.query("SELECT count() FROM system.tables WHERE name = 'tbl';")
    )
    assert 1 == int(
        node2.query("SELECT count() FROM system.databases WHERE name = 'replicateddb';")
    )
    assert 1 == int(
        node2.query("SELECT count() FROM system.tables WHERE name = 'tbl2';")
    )

    node2.query("SYSTEM STOP MERGES;")
    node2.query("SYSTEM STOP FETCHES;")
    node2.query("SYSTEM STOP REPLICATION QUEUES;")

    node1.query("INSERT INTO default.tbl SELECT * FROM system.numbers_mt LIMIT 10000;")
    node1.query(
        "INSERT INTO replicateddb.tbl2 SELECT * FROM system.numbers_mt LIMIT 10000;"
    )

    assert 0 == int(node2.query("SELECT count() FROM default.tbl;"))
    assert 0 == int(node2.query("SELECT count() FROM replicateddb.tbl2;"))

    node2.query("DROP TABLE IF EMPTY default.tbl ON CLUSTER 'cluster';")
    node2.query("DROP TABLE IF EMPTY replicateddb.tbl2;")

    assert 0 == int(
        node1.query("SELECT count() FROM system.tables WHERE name = 'tbl';")
    )
    assert 0 == int(
        node2.query("SELECT count() FROM system.tables WHERE name = 'tbl';")
    )
    assert 0 == int(
        node1.query("SELECT count() FROM system.tables WHERE name = 'tbl2';")
    )
    assert 0 == int(
        node2.query("SELECT count() FROM system.tables WHERE name = 'tbl2';")
    )

    with pytest.raises(
        QueryRuntimeException,
        match="DB::Exception: DROP IF EMPTY is not implemented for databases.",
    ):
        node2.query("DROP DATABASE IF EMPTY replicateddb;")
