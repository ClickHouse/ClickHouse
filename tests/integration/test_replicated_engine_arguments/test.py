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
    # From v24.9 onwards we introduced a setting `database_replicated_allow_replicated_engine_arguments` with default value 0.
    # This controls if users are allowed to specify ENGINE arguments for *MergeTree tables in Replicated databases or not.
    # The `CREATE TABLE AS ...` query is interesting because it clones the table structure of t1 and tries to create t2. When engine args are not present, we generate them automatically.
    # When creating t2 with the table structure of t1, the AST for the create query also contains the engine args from t1 which gets validated, and we fail when the
    # setting value is 0. The fix is to drop engine args for `AS TABLE` queries for `*MergeTree`tables in `Replicated` database.
    # This test is to ensure that we are able to do a CREATE TABLE AS ... without any issues for such kind of tables
    node1.query(
        "SET database_replicated_allow_replicated_engine_arguments=0; CREATE TABLE r.t2 AS r.t1"
    )  # should not fail
    node1.query("DROP TABLE IF EXISTS r.t1")
    node1.query("DROP TABLE IF EXISTS r.t2")
