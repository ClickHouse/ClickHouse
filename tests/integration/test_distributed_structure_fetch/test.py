"""A structure-less `CREATE TABLE ... ENGINE = Distributed` has to fetch the structure
of the remote table with a `DESC TABLE` service query. When the `CREATE` runs under a
context without a client version (e.g. the storage's global context), that service query
used to be sent with a zero initiator version and was rejected by `RemoteQueryExecutor`
with a logical error. See https://github.com/ClickHouse/ClickHouse/issues/113671.
"""

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
)
node2 = cluster.add_instance("node2")


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        node2.query(
            "CREATE TABLE default.remote_data (key UInt64, value String) ENGINE = MergeTree ORDER BY key"
        )
        node2.query("INSERT INTO default.remote_data VALUES (1, 'one'), (2, 'two')")
        yield cluster
    finally:
        cluster.shutdown()


def test_structure_less_distributed_over_remote_shard(started_cluster):
    node1.query(
        "CREATE TABLE default.dist ENGINE = Distributed('remote_only_cluster', default, remote_data)"
    )
    assert node1.query("DESC TABLE default.dist") == (
        "key\tUInt64\t\t\t\t\t\nvalue\tString\t\t\t\t\t\n"
    )
    assert node1.query("SELECT sum(key) FROM default.dist") == "3\n"
    node1.query("DROP TABLE default.dist SYNC")


def test_structure_less_distributed_in_replicated_database(started_cluster):
    """The variant BuzzHouse found: the `CREATE` is replayed by the `DDLWorker` of a
    `Replicated` database, whose query context also carried no client version."""
    node1.query(
        "CREATE DATABASE replicated_db ENGINE = Replicated('/clickhouse/databases/replicated_db', 'shard1', 'replica1')"
    )
    try:
        node1.query(
            "CREATE TABLE replicated_db.dist ENGINE = Distributed('remote_only_cluster', default, remote_data)"
        )
        assert node1.query("SELECT sum(key) FROM replicated_db.dist") == "3\n"
    finally:
        node1.query("DROP DATABASE replicated_db SYNC")
