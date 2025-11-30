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
node4 = cluster.add_instance(
    "node4", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
)
node5 = cluster.add_instance(
    "node5", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
)
node6 = cluster.add_instance(
    "node6", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def create_tables(table_name):
    """Only create the table on node1 and node2, to add more ranges to the stealing queue."""
    node1.query(
        f"CREATE TABLE {table_name} (key Int64, value String) Engine=ReplicatedMergeTree('/test_parallel_replicas/shard1/{table_name}', 'r1') ORDER BY (key)"
    )
    node2.query(
        f"CREATE TABLE {table_name} (key Int64, value String) Engine=ReplicatedMergeTree('/test_parallel_replicas/shard1/{table_name}', 'r2') ORDER BY (key)"
    )

    node1.query(
        f"INSERT INTO {table_name} SELECT number % 4, toString(number) FROM numbers(2000000)"
    )
    node1.query(f"OPTIMIZE TABLE {table_name} FINAL")
    node2.query(f"SYSTEM SYNC REPLICA {table_name}")


def test_all_replicas_can_steal_orphaned_ranges(start_cluster):
    cluster_name = "test_cluster_6_replicas"
    table_name = "test_steal_orphaned_ranges"
    create_tables(table_name)

    expected_result = ""
    for i in range(4):
        expected_result += f"{i}\t500000\n"

    log_comment = str(uuid.uuid4())

    node1.query("SYSTEM ENABLE FAILPOINT parallel_replicas_wait_for_unused_replicas")
    node1.query("SYSTEM ENABLE FAILPOINT slowdown_parallel_replicas_local_plan_read")

    result = node1.query(
        f"SELECT key, count() FROM {table_name} GROUP BY key ORDER BY key",
        settings={
            "enable_parallel_replicas": 2,
            "max_parallel_replicas": 6,
            "cluster_for_parallel_replicas": cluster_name,
            "log_comment": log_comment,
            "max_threads": 1,
            "merge_tree_min_rows_for_concurrent_read": 1,
            "merge_tree_min_bytes_for_concurrent_read": 1,
        },
    )

    assert result == expected_result, f"Expected:\n{expected_result}\nGot:\n{result}"

    node2.query("SYSTEM FLUSH LOGS")

    # Check that node2 (not local plan replica) stole orphaned marks
    node2_orphaned = int(
        node2.query(
            f"""
            SELECT ProfileEvents['ParallelReplicasReadUnassignedMarks'] 
            FROM system.query_log 
            WHERE type = 'QueryFinish' 
            AND current_database = currentDatabase() 
            AND log_comment = '{log_comment}'
            SETTINGS enable_parallel_replicas=0
            """
        ).strip()
    )

    assert (
        node2_orphaned > 0
    ), "node2 (not local plan replica) should have stolen orphaned marks. "

    node1.query("SYSTEM DISABLE FAILPOINT parallel_replicas_wait_for_unused_replicas")
    node1.query("SYSTEM DISABLE FAILPOINT slowdown_parallel_replicas_local_plan_read")

    node1.query(f"DROP TABLE {table_name} SYNC")
    node2.query(f"DROP TABLE {table_name} SYNC")
