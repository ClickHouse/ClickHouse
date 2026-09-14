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


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def create_tables(cluster, table_name):
    node1.query(
        f"CREATE TABLE {table_name} (key Int64, value String) Engine=ReplicatedMergeTree('/test_parallel_replicas/shard1/{table_name}', 'r1') ORDER BY (key)"
    )
    node2.query(
        f"CREATE TABLE {table_name} (key Int64, value String) Engine=ReplicatedMergeTree('/test_parallel_replicas/shard1/{table_name}', 'r2') ORDER BY (key)"
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


def test_increase_error_count(start_cluster):
    cluster_name = "test_1_shard_3_replicas"
    table_name = "tt"
    create_tables(cluster_name, table_name)

    expected_result = ""
    for i in range(4):
        expected_result += f"{i}\t1000\n"

    node1.query("SYSTEM ENABLE FAILPOINT parallel_replicas_wait_for_unused_replicas");
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

    assert (
        int(
            node1.query(
                f"SELECT errors_count FROM system.clusters WHERE cluster = 'test_1_shard_3_replicas' AND host_name = 'node3'"
            ).strip()
        )
        > 0
    )

    node1.query(f"DROP TABLE {table_name} SYNC")
    node2.query(f"DROP TABLE {table_name} SYNC")
