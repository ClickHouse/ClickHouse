import uuid

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
)
node2 = cluster.add_instance(
    "node2", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
)
node3 = cluster.add_instance(
    "node3", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def create_tables(cluster, table_name, skip_last_replica):
    node1.query(
        f"CREATE TABLE {table_name} (key Int64, value String) Engine=ReplicatedMergeTree('/test_parallel_replicas/shard1/{table_name}', 'r1') ORDER BY (key)"
    )
    node2.query(
        f"CREATE TABLE {table_name} (key Int64, value String) Engine=ReplicatedMergeTree('/test_parallel_replicas/shard1/{table_name}', 'r2') ORDER BY (key)"
    )
    if not skip_last_replica:
        node3.query(
            f"CREATE TABLE {table_name} (key Int64, value String) Engine=ReplicatedMergeTree('/test_parallel_replicas/shard1/{table_name}', 'r3') ORDER BY (key)"
        )

    # populate data
    node1.query(
        f"INSERT INTO {table_name} SELECT number % 4, number FROM numbers(1000)"
    )
    node1.query(
        f"INSERT INTO {table_name} SELECT number % 4, number FROM numbers(1000, 1000)"
    )
    node1.query(
        f"INSERT INTO {table_name} SELECT number % 4, number FROM numbers(2000, 1000)"
    )
    node1.query(
        f"INSERT INTO {table_name} SELECT number % 4, number FROM numbers(3000, 1000)"
    )
    node2.query(f"SYSTEM SYNC REPLICA {table_name}")
    if not skip_last_replica:
        node3.query(f"SYSTEM SYNC REPLICA {table_name}")


def test_skip_replicas_without_table(start_cluster):
    cluster_name = "test_1_shard_3_replicas"
    table_name = "tt"
    create_tables(cluster_name, table_name, skip_last_replica=True)

    expected_result = ""
    for i in range(4):
        expected_result += f"{i}\t1000\n"

    log_comment = uuid.uuid4()
    assert (
        node1.query(
            f"SELECT key, count() FROM {table_name} GROUP BY key ORDER BY key",
            settings={
                "enable_parallel_replicas": 2,
                "max_parallel_replicas": 3,
                "cluster_for_parallel_replicas": cluster_name,
                "log_comment": log_comment,
            },
        )
        == expected_result
    )

    node1.query("SYSTEM FLUSH LOGS")
    assert (
        node1.query(
            f"SELECT ProfileEvents['DistributedConnectionMissingTable'], ProfileEvents['ParallelReplicasUnavailableCount'] FROM system.query_log WHERE type = 'QueryFinish' AND query_id IN (SELECT query_id FROM system.query_log WHERE current_database = currentDatabase() AND log_comment = '{log_comment}' AND type = 'QueryFinish' AND initial_query_id = query_id)  SETTINGS enable_parallel_replicas=0"
        )
        == "1\t1\n"
    )
    node1.query(f"DROP TABLE {table_name} SYNC")
    node2.query(f"DROP TABLE {table_name} SYNC")


def test_skip_unresponsive_replicas(start_cluster):
    cluster_name = "test_1_shard_3_replicas"
    table_name = "tt"
    create_tables(cluster_name, table_name, skip_last_replica=False)

    expected_result = ""
    for i in range(4):
        expected_result += f"{i}\t1000\n"

    node1.query("SYSTEM ENABLE FAILPOINT receive_timeout_on_table_status_response")

    assert (
        node1.query(
            f"SELECT key, count() FROM {table_name} GROUP BY key ORDER BY key",
            settings={
                "enable_parallel_replicas": 2,
                "max_parallel_replicas": 3,
                "cluster_for_parallel_replicas": cluster_name,
            },
        )
        == expected_result
    )
    node1.query(f"DROP TABLE {table_name} SYNC")
    node2.query(f"DROP TABLE {table_name} SYNC")
    node3.query(f"DROP TABLE {table_name} SYNC")
