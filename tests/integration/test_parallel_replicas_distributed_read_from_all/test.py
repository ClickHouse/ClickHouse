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


def create_tables(cluster, table_name):
    """create replicated tables in special way
    - each table is populated by equal number of rows
    - fetches are disabled, so each replica will have different set of rows
      which enforce parallel replicas read from each replica
    """

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
    # stop merges
    nodes[0].query(f"system stop merges {table_name}")
    nodes[1].query(f"system stop merges {table_name}")
    nodes[2].query(f"system stop merges {table_name}")
    # stop fetches
    nodes[0].query(f"system stop fetches {table_name}")
    nodes[1].query(f"system stop fetches {table_name}")
    nodes[2].query(f"system stop fetches {table_name}")

    # create distributed table
    nodes[0].query(f"DROP TABLE IF EXISTS {table_name}_d SYNC")
    nodes[0].query(
        f"""
            CREATE TABLE {table_name}_d AS {table_name}
            Engine=Distributed(
                {cluster},
                currentDatabase(),
                {table_name},
                rand()
            )
            """
    )

    # populate data, equal number of rows for each replica
    nodes[0].query(
        f"INSERT INTO {table_name} SELECT number, number FROM numbers(10)",
        settings={"insert_distributed_sync": 1},
    )
    nodes[0].query(
        f"INSERT INTO {table_name} SELECT number, number FROM numbers(10, 10)",
        settings={"insert_distributed_sync": 1},
    )
    nodes[1].query(
        f"INSERT INTO {table_name} SELECT number, number FROM numbers(20, 10)",
        settings={"insert_distributed_sync": 1},
    )
    nodes[1].query(
        f"INSERT INTO {table_name} SELECT number, number FROM numbers(30, 10)",
        settings={"insert_distributed_sync": 1},
    )
    nodes[2].query(
        f"INSERT INTO {table_name} SELECT number, number FROM numbers(40, 10)",
        settings={"insert_distributed_sync": 1},
    )
    nodes[2].query(
        f"INSERT INTO {table_name} SELECT number, number FROM numbers(50, 10)",
        settings={"insert_distributed_sync": 1},
    )

    return "60\t0\t59\t1770\n"


@pytest.mark.parametrize(
    "prefer_localhost_replica",
    [
        pytest.param(0),
        pytest.param(1),
    ],
)
def test_read_equally_from_each_replica(start_cluster, prefer_localhost_replica):
    """create and populate table in special way (see create_table()),
    so parallel replicas will read equal number of rows from each replica
    """

    cluster = "test_single_shard_multiple_replicas"
    table_name = "test_table"
    expected_result = create_tables(cluster, table_name)

    # parallel replicas
    assert (
        nodes[0].query(
            f"SELECT count(), min(key), max(key), sum(key) FROM {table_name}_d",
            settings={
                "allow_experimental_parallel_reading_from_replicas": 2,
                "prefer_localhost_replica": prefer_localhost_replica,
                "max_parallel_replicas": 3,
                "use_hedged_requests": 0,
            },
        )
        == expected_result
    )

    # check logs for coordinator statistic
    for n in nodes:
        n.query("SYSTEM FLUSH LOGS")

    # each replica has 2 distinct parts (non-intersecting with another replicas),
    # each part less then index granularity, therefore 2 marks for each replica to handle
    coordinator_statistic = "replica 0 - {requests: 3 marks: 2}; replica 1 - {requests: 3 marks: 2}; replica 2 - {requests: 3 marks: 2}"
    assert (
        nodes[0].contains_in_log(coordinator_statistic)
        or nodes[1].contains_in_log(coordinator_statistic)
        or nodes[2].contains_in_log(coordinator_statistic)
    )

    # w/o parallel replicas
    # start fetches back, otherwise the result will be not as expected
    nodes[0].query(f"system start fetches {table_name}")
    nodes[1].query(f"system start fetches {table_name}")
    nodes[2].query(f"system start fetches {table_name}")
    assert (
        nodes[0].query(
            f"SELECT count(), min(key), max(key), sum(key) FROM {table_name}_d"
        )
        == expected_result
    )
