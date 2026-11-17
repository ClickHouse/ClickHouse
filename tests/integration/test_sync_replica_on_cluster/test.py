import os
import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV, assert_eq_with_retry

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/remote_servers.xml"],
    macros={"replica": "node1"},
    with_zookeeper=True,
)

node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/remote_servers.xml"],
    macros={"replica": "node2"},
    with_zookeeper=True,
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield
    finally:
        cluster.shutdown()


def test_sync_replica_on_cluster():
    table = "tbl_test_sync_replica_on_cluster"
    node1.query(f"DROP TABLE IF EXISTS {table} ON CLUSTER 'cluster' SYNC")
    node1.query(
        f"""
        CREATE TABLE {table} ON CLUSTER 'cluster' (id Int64, str String)
        ENGINE=ReplicatedMergeTree('/clickhouse/tables/{table}/', '{{replica}}')
        ORDER BY id
        """
    )

    # Does not throw
    node2.query(f"SYSTEM SYNC REPLICA ON CLUSTER 'cluster' {table}")

    node2.query("INSERT INTO tbl_test_sync_replica_on_cluster VALUES (2, 'str2')")
    node1.query(f"SYSTEM SYNC REPLICA ON CLUSTER 'cluster' {table}")
    count = node1.query(f"SELECT count() FROM {table}")
    assert count == "1\n"

    node1.query(f"DROP TABLE IF EXISTS {table} ON CLUSTER 'cluster' SYNC")


def test_sync_replica_not_replicated():
    table = "test_sync_replica_not_replicated"

    node1.query(f"DROP TABLE IF EXISTS {table} ON CLUSTER 'cluster' SYNC")
    node1.query(
        f"""
        CREATE TABLE {table} ON CLUSTER 'cluster' (id Int64, str String)
        ENGINE=MergeTree()
        ORDER BY id
        """
    )

    output, error = node1.query_and_get_answer_with_error(
        f"SYSTEM SYNC REPLICA {table}"
    )
    assert "is not replicated" in error

    output, error = node2.query_and_get_answer_with_error(
        f"SYSTEM SYNC REPLICA {table}"
    )
    assert "is not replicated" in error

    node1.query(f"DROP TABLE IF EXISTS {table} ON CLUSTER 'cluster' SYNC")


def test_sync_replica_if_not_exists():
    table = "test_sync_replica_if_not_exists"

    node1.query(f"DROP TABLE IF EXISTS {table} ON CLUSTER 'cluster' SYNC")
    output, error = node1.query_and_get_answer_with_error(
        f"SYSTEM SYNC REPLICA {table} IF EXISTS"
    )
    assert error == ""

    # Table exists only in node1
    node1.query(
        f"""
        CREATE TABLE {table} (id Int64, str String)
        ENGINE=ReplicatedMergeTree('/clickhouse/tables/{table}/', '{{replica}}')
        ORDER BY id
        """
    )
    output, error = node1.query_and_get_answer_with_error(
        f"SYSTEM SYNC REPLICA ON CLUSTER cluster {table} IF EXISTS"
    )
    assert error == ""

    query_id = str(uuid.uuid4())
    output, error = node2.query_and_get_answer_with_error(
        f"SYSTEM SYNC REPLICA ON CLUSTER cluster {table} IF EXISTS", query_id=query_id
    )
    assert error == ""

    # Node 1 should have received the sync request even if the table didn't exist in node2
    node1.query("SYSTEM FLUSH LOGS system.query_log")
    output = node1.query(
        f"SELECT count() FROM system.query_log where initial_query_id = '{query_id}'"
    )
    assert output == "2\n"

    node1.query(f"DROP TABLE IF EXISTS {table} ON CLUSTER 'cluster' SYNC")
