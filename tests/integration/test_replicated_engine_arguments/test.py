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
    main_configs=main_configs,
    macros={"replica": "node1", "shard": "shard1"},
    with_zookeeper=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_replicated_engine_with_arguments(start_cluster):
    node1.query("DROP DATABASE IF EXISTS r")
    node1.query(
        f"CREATE DATABASE r ENGINE=Replicated('/clickhouse/databases/r', '{{shard}}', '{{replica}}')"
    )
    node1.query(
        "CREATE TABLE r.t1 (x UInt8, y String) ENGINE=ReplicatedMergeTree ORDER BY x"
    )
    # Test that `database_replicated_allow_replicated_engine_arguments=0` for *MergeTree tables in Replicated databases work as expected for `CREATE TABLE ... AS ...` queries.
    # While creating t2 with the table structure of t1, the AST for the create query would contain the engine args from t1. For this kind of queries, skip this validation if
    # the value of the arguments match the default values and also skip throwing an exception.
    node1.query(
        "SET database_replicated_allow_replicated_engine_arguments=0; CREATE TABLE r.t2 AS r.t1"
    )  # should not fail
    expected = "CREATE TABLE r.t2\\n(\\n    `x` UInt8,\\n    `y` String\\n)\\nENGINE = ReplicatedMergeTree(\\'/clickhouse/tables/{uuid}/{shard}\\', \\'{replica}\\')\\nORDER BY x\\nSETTINGS index_granularity = 8192\n"
    # ensure that t2 was created with the correct default engine args
    assert node1.query("SHOW CREATE TABLE r.t2") == expected
    node1.query("DROP DATABASE r")
