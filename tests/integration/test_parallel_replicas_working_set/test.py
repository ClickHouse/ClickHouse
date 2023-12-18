import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

nodes = [
    cluster.add_instance(
        f"n{i}", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
    )
    for i in (1, 2, 3)
]


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def create_tables(cluster, table_name, node_with_covering_part):
    # create replicated tables
    for node in nodes:
        node.query(f"DROP TABLE IF EXISTS {table_name} SYNC")

    nodes[0].query(
        f"""CREATE TABLE IF NOT EXISTS {table_name} (key Int64, value String) Engine=ReplicatedMergeTree('/test_parallel_replicas/shard1/{table_name}', 'r1')
            ORDER BY (key)"""
    )
    nodes[1].query(
        f"""CREATE TABLE IF NOT EXISTS {table_name} (key Int64, value String) Engine=ReplicatedMergeTree('/test_parallel_replicas/shard1/{table_name}', 'r2')
            ORDER BY (key)"""
    )
    nodes[2].query(
        f"""CREATE TABLE IF NOT EXISTS {table_name} (key Int64, value String) Engine=ReplicatedMergeTree('/test_parallel_replicas/shard1/{table_name}', 'r3')
            ORDER BY (key)"""
    )
    # stop merges to keep original parts
    # stop fetches to keep only parts created on the nodes
    for i in (0, 1, 2):
        if i != node_with_covering_part:
            nodes[i].query(f"system stop fetches {table_name}")
            nodes[i].query(f"system stop merges {table_name}")

    # populate data, equal number of rows for each replica
    nodes[0].query(
        f"INSERT INTO {table_name} SELECT number, number FROM numbers(10)",
    )
    nodes[0].query(
        f"INSERT INTO {table_name} SELECT number, number FROM numbers(10, 10)"
    )
    nodes[1].query(
        f"INSERT INTO {table_name} SELECT number, number FROM numbers(20, 10)"
    )
    nodes[1].query(
        f"INSERT INTO {table_name} SELECT number, number FROM numbers(30, 10)"
    )
    nodes[2].query(
        f"INSERT INTO {table_name} SELECT number, number FROM numbers(40, 10)"
    )
    nodes[2].query(
        f"INSERT INTO {table_name} SELECT number, number FROM numbers(50, 10)"
    )
    nodes[node_with_covering_part].query(f"system sync replica {table_name}")
    nodes[node_with_covering_part].query(f"optimize table {table_name}")

    # check we have expected set of parts
    expected_active_parts = ""
    if node_with_covering_part == 0:
        expected_active_parts = (
            "all_0_5_1\nall_2_2_0\nall_3_3_0\nall_4_4_0\nall_5_5_0\n"
        )

    if node_with_covering_part == 1:
        expected_active_parts = (
            "all_0_0_0\nall_0_5_1\nall_1_1_0\nall_4_4_0\nall_5_5_0\n"
        )

    if node_with_covering_part == 2:
        expected_active_parts = (
            "all_0_0_0\nall_0_5_1\nall_1_1_0\nall_2_2_0\nall_3_3_0\n"
        )

    assert (
        nodes[0].query(
            f"select distinct name from clusterAllReplicas({cluster}, system.parts) where table='{table_name}' and active order by name"
        )
        == expected_active_parts
    )


@pytest.mark.parametrize("node_with_covering_part", [0, 1, 2])
def test_covering_part_in_announcement(start_cluster, node_with_covering_part):
    """create and populate table in special way (see create_table()),
    node_with_covering_part contains all parts merged into one,
    other nodes contain only parts which are result of insert via the node
    """

    cluster = "test_single_shard_multiple_replicas"
    table_name = "test_table"
    create_tables(cluster, table_name, node_with_covering_part)

    # query result can be one of the following outcomes
    # (1) query result if parallel replicas working set contains all_0_5_1
    expected_full_result = "60\t0\t59\t1770\n"
    expected_results = {expected_full_result}

    # (2) query result if parallel replicas working set DOESN'T contain all_0_5_1
    if node_with_covering_part == 0:
        expected_results.add("40\t20\t59\t1580\n")
    if node_with_covering_part == 1:
        expected_results.add("40\t0\t59\t1180\n")
    if node_with_covering_part == 2:
        expected_results.add("40\t0\t39\t780\n")

    # parallel replicas
    result = nodes[0].query(
        f"SELECT count(), min(key), max(key), sum(key) FROM {table_name}",
        settings={
            "allow_experimental_parallel_reading_from_replicas": 2,
            "prefer_localhost_replica": 0,
            "max_parallel_replicas": 3,
            "use_hedged_requests": 0,
            "cluster_for_parallel_replicas": cluster,
        },
    )
    assert result in expected_results

    # w/o parallel replicas
    assert (
        nodes[node_with_covering_part].query(
            f"SELECT count(), min(key), max(key), sum(key) FROM {table_name}",
            settings={
                "allow_experimental_parallel_reading_from_replicas": 0,
            },
        )
        == expected_full_result
    )
