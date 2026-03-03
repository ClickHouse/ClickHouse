import uuid

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "n1", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
)
node2 = cluster.add_instance(
    "n2", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
)
node3 = cluster.add_instance(
    "n3", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
)
node4 = cluster.add_instance(
    "n4", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
)

nodes = [node1, node2, node3, node4]


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def create_tables(table_name):
    for i in range(0, 4):
        nodes[i].query(f"DROP TABLE IF EXISTS {table_name} SYNC")
        nodes[i].query(
            f"CREATE TABLE IF NOT EXISTS {table_name} (key UInt64, value String) Engine=ReplicatedMergeTree('/test_parallel_replicas/shard1/{table_name}', 'r{i+1}') ORDER BY (key)"
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
    node3.query(f"SYSTEM SYNC REPLICA {table_name}")
    node4.query(f"SYSTEM SYNC REPLICA {table_name}")


@pytest.mark.parametrize("use_hedged_requests", [1, 0])
@pytest.mark.parametrize("custom_key", ["sipHash64(key)", "key"])
@pytest.mark.parametrize(
    "parallel_replicas_mode", ["custom_key_sampling", "custom_key_range"]
)
def test_parallel_replicas_custom_key_load_balancing(
    start_cluster,
    use_hedged_requests,
    custom_key,
    parallel_replicas_mode,
):
    cluster_name = "test_single_shard_multiple_replicas"
    table = "test_table"

    create_tables(table)

    expected_result = ""
    for i in range(4):
        expected_result += f"{i}\t1000\n"

    log_comment = uuid.uuid4()
    assert (
        node1.query(
            f"SELECT key, count() FROM cluster('{cluster_name}', currentDatabase(), test_table) GROUP BY key ORDER BY key",
            settings={
                "log_comment": log_comment,
                "prefer_localhost_replica": 0,
                "max_parallel_replicas": 4,
                "enable_parallel_replicas": 1,
                "parallel_replicas_custom_key": custom_key,
                "parallel_replicas_mode": parallel_replicas_mode,
                "use_hedged_requests": use_hedged_requests,
                # avoid considering replica delay on connection choice
                # otherwise connection can be not distributed evenly among available nodes
                # and so custom key secondary queries (we check it bellow)
                "max_replica_delay_for_distributed_queries": 0,
            },
        )
        == expected_result
    )

    for node in nodes:
        node.query("system flush logs")

    # the subqueries should be spread over available nodes
    query_id = node1.query(
        f"SELECT query_id FROM system.query_log WHERE current_database = currentDatabase() AND log_comment = '{log_comment}' AND type = 'QueryFinish' AND initial_query_id = query_id"
    )
    assert query_id != ""
    query_id = query_id[:-1]

    assert (
        node1.query(
            f"SELECT 'subqueries', count() FROM clusterAllReplicas({cluster_name}, system.query_log) WHERE initial_query_id = '{query_id}' AND type ='QueryFinish' AND query_id != initial_query_id SETTINGS skip_unavailable_shards=1"
        )
        == "subqueries\t4\n"
    )

    # With enabled hedged requests, we can't guarantee exact query distribution among nodes
    # In case of a replica being slow in terms of responsiveness, hedged connection can change initial replicas choice
    if use_hedged_requests == 0:
        # check queries per node
        assert (
            node1.query(
                f"SELECT h, count() FROM clusterAllReplicas({cluster_name}, system.query_log) WHERE initial_query_id = '{query_id}' AND type ='QueryFinish' GROUP BY hostname() as h ORDER BY h SETTINGS skip_unavailable_shards=1"
            )
            == "n1\t2\nn2\t1\nn3\t1\nn4\t1\n"
        )
