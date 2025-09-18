import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node0 = cluster.add_instance(
    "node0",
    main_configs=["configs/clusters.xml"],
    with_zookeeper=True,
    image="clickhouse/clickhouse-server",
    tag="24.3",  # earlier versions lead to "Not found column XXX in block." exception 🤷
    with_installed_binary=True,
    use_old_analyzer=False,
    macros={"replica": "node0", "shard": "shard"}
)
node1 = cluster.add_instance("node1", main_configs=["configs/clusters.xml", "configs/default_serialization_info.xml"], with_zookeeper=True, use_old_analyzer=False, macros={"replica": "node1", "shard": "shard"})
node2 = cluster.add_instance("node2", main_configs=["configs/clusters.xml", "configs/default_serialization_info.xml"], with_zookeeper=True, use_old_analyzer=False,  macros={"replica": "node2", "shard": "shard"})
nodes = [node0, node1, node2]


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def _create_tables(table_name):
    nodes[0].query(
        f"DROP TABLE IF EXISTS {table_name} ON CLUSTER 'parallel_replicas'",
        settings={"database_atomic_wait_for_drop_and_detach_synchronously": True},
    )

    nodes[0].query(
        f"""
        CREATE TABLE {table_name} ON CLUSTER 'parallel_replicas' (a Int64, b Int64)
        Engine=ReplicatedMergeTree('/test_backward_compatability/test_distributed_aggregation_with_out_of_order_buckets/shard0/{table_name}', '{{replica}}')
        ORDER BY ()
        """
    )

    nodes[0].query(f"INSERT INTO {table_name} SELECT number%1000, number%1000 FROM numbers_mt(1e6)")
    # The goal is to have a heavy bucket with a small bucket id to have it delayed by the nodes;
    # 30 belongs to the bucket #6, so it should do the job
    nodes[0].query(f"INSERT INTO {table_name} SELECT 30, number FROM numbers_mt(1e6)")
    nodes[0].query(f"OPTIMIZE TABLE {table_name} FINAL")
    nodes[0].query(f"SYSTEM SYNC REPLICA ON CLUSTER 'parallel_replicas' {table_name}")


def test_distributed_aggregation_with_out_of_order_buckets(start_cluster):
    table_name = "test_distributed_aggregation"
    _create_tables(table_name)

    # Node0 has the old version, so it should be the initiator to catch the situation
    # when some of the replicas decided to send buckets out of order to the old initiator
    node0.query(
        f"""
        SELECT a, throwIf(if(a = 30, 1000000, 1) != uniqExact(b))
        FROM {table_name}
        GROUP BY a
        FORMAT NULL
        """,
        settings={
            "cluster_for_parallel_replicas": "parallel_replicas",
            "allow_experimental_parallel_reading_from_replicas": 1,
            "group_by_two_level_threshold": 1,
            "group_by_two_level_threshold_bytes": 1,
            "max_parallel_replicas": 3,
            "parallel_replicas_for_non_replicated_merge_tree": 1,
            "merge_tree_min_rows_for_concurrent_read": 0,
            "merge_tree_min_bytes_for_concurrent_read": 0,
            "merge_tree_min_read_task_size": 1,
        },
    )
