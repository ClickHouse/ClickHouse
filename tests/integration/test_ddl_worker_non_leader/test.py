import pytest
import time
from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager
from helpers.client import QueryRuntimeException

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
)
node2 = cluster.add_instance(
    "node2", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def test_non_leader_replica(started_cluster):

    node1.query_with_retry(
        """CREATE TABLE IF NOT EXISTS sometable(id UInt32, value String)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/0/sometable', '1') ORDER BY tuple()"""
    )

    node2.query_with_retry(
        """CREATE TABLE IF NOT EXISTS sometable(id UInt32, value String)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/0/sometable', '2') ORDER BY tuple() SETTINGS replicated_can_become_leader = 0"""
    )

    node1.query(
        "INSERT INTO sometable SELECT number, toString(number) FROM numbers(100)"
    )
    node2.query_with_retry("SYSTEM SYNC REPLICA sometable", timeout=10)

    assert node1.query("SELECT COUNT() FROM sometable") == "100\n"
    assert node2.query("SELECT COUNT() FROM sometable") == "100\n"

    with PartitionManager() as pm:
        pm.drop_instance_zk_connections(node1)

        # this query should be executed by leader, but leader partitioned from zookeeper
        with pytest.raises(Exception):
            node2.query(
                "ALTER TABLE sometable ON CLUSTER 'test_cluster' MODIFY COLUMN value UInt64 SETTINGS distributed_ddl_task_timeout=5"
            )

    for _ in range(100):
        if "UInt64" in node1.query(
            "SELECT type FROM system.columns WHERE name='value' and table = 'sometable'"
        ):
            break
        time.sleep(0.1)

    for _ in range(100):
        if "UInt64" in node2.query(
            "SELECT type FROM system.columns WHERE name='value' and table = 'sometable'"
        ):
            break
        time.sleep(0.1)

    assert "UInt64" in node1.query(
        "SELECT type FROM system.columns WHERE name='value' and table = 'sometable'"
    )
    assert "UInt64" in node2.query(
        "SELECT type FROM system.columns WHERE name='value' and table = 'sometable'"
    )

    # Checking that DDLWorker doesn't hung and still able to execute DDL queries
    node1.query(
        "CREATE TABLE new_table_with_ddl ON CLUSTER 'test_cluster' (key UInt32) ENGINE=MergeTree() ORDER BY tuple()",
        settings={"distributed_ddl_task_timeout": "10"},
    )
    assert node1.query("EXISTS new_table_with_ddl") == "1\n"
    assert node2.query("EXISTS new_table_with_ddl") == "1\n"
