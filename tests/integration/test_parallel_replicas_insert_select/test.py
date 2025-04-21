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


def create_tables(table_name, populate, skip_last_replica):
    node1.query(f"DROP TABLE IF EXISTS {table_name} SYNC")
    node2.query(f"DROP TABLE IF EXISTS {table_name} SYNC")
    node3.query(f"DROP TABLE IF EXISTS {table_name} SYNC")

    node1.query(
        f"CREATE TABLE {table_name} (key Int64, value String) Engine=ReplicatedMergeTree('/test_parallel_replicas/shard1/{table_name}', 'r1') ORDER BY (key) settings index_granularity=10"
    )
    node2.query(
        f"CREATE TABLE {table_name} (key Int64, value String) Engine=ReplicatedMergeTree('/test_parallel_replicas/shard1/{table_name}', 'r2') ORDER BY (key) settings index_granularity=10"
    )
    if not skip_last_replica:
        node3.query(
            f"CREATE TABLE {table_name} (key Int64, value String) Engine=ReplicatedMergeTree('/test_parallel_replicas/shard1/{table_name}', 'r3') ORDER BY (key) settings index_granularity=10"
        )

    if not populate:
        return

    # populate data
    node1.query(
        f"INSERT INTO {table_name} SELECT number, toString(number) FROM numbers(1_000_000)"
    )
    node2.query(f"SYSTEM SYNC REPLICA {table_name}")
    if not skip_last_replica:
        node3.query(f"SYSTEM SYNC REPLICA {table_name}")

#
# cases
# 3 nodes
#   - max_parallel_replicas 2, 3
#
@pytest.mark.parametrize(
    "cluster_name,max_parallel_replicas,local_pipeline,executed_queries",
    [
        pytest.param("test_1_shard_3_replicas", 2, False, 3),
        pytest.param("test_1_shard_3_replicas", 2, True, 2),
        pytest.param("test_1_shard_3_replicas", 3, False, 4),
        pytest.param("test_1_shard_3_replicas", 3, True, 3),
        pytest.param("test_1_shard_3_replicas_1_unavailable", 3, False, 3),
        pytest.param("test_1_shard_3_replicas_1_unavailable", 3, True, 2),
    ],
)
def test_insert_select(start_cluster, cluster_name, max_parallel_replicas, local_pipeline, executed_queries):
    source_table = "t_source"
    create_tables(source_table, populate=True, skip_last_replica=False)
    target_table = "t_target"
    create_tables(target_table, populate=False, skip_last_replica=False)

    log_comment = uuid.uuid4()
    node1.query(
        f"INSERT INTO {target_table} SELECT * FROM {source_table}",
        settings={
            "parallel_distributed_insert_select": 2,
            "enable_parallel_replicas": 2,
            "max_parallel_replicas": max_parallel_replicas,
            "cluster_for_parallel_replicas": cluster_name,
            "log_comment": log_comment,
            "parallel_replicas_insert_select_local_pipeline": local_pipeline,
        },
    )
    expected_count = 1000000
    assert (
        node1.query(
            f"select count() from {target_table}"
        )
        == f"{expected_count}\n"
    )
    assert (
        node1.query(
            f"select * from {target_table} order by key except select * from {source_table} order by key",
        )
        == ""
    )

    node1.query(f"SYSTEM FLUSH LOGS query_log")
    node2.query(f"SYSTEM FLUSH LOGS query_log")
    node3.query(f"SYSTEM FLUSH LOGS query_log")

    assert (
        node1.query(
            f"""SELECT count() FROM clusterAllReplicas({cluster_name}, system.query_log) WHERE current_database = currentDatabase() AND log_comment = '{log_comment}' AND type = 'QueryFinish' AND query_kind = 'Insert'
                settings skip_unavailable_shards=1"""
        )
        == f"{executed_queries}\n"
    )
