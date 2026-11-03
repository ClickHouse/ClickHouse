import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

nodes = [
    cluster.add_instance(
        f"n{i}", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
    )
    for i in (1, 2, 3, 4, 5, 6)
]


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def create_tables(cluster, table_name):
    # create replicated tables
    for node in nodes:
        node.query(f"DROP TABLE IF EXISTS {table_name} SYNC")

    if cluster == "test_single_shard_multiple_replicas":
        nodes[0].query(
            f"CREATE TABLE IF NOT EXISTS {table_name} (key Int64, value String) Engine=ReplicatedMergeTree('/test_parallel_replicas/shard1/{table_name}', 'r1') ORDER BY (key)"
        )
        nodes[1].query(
            f"CREATE TABLE IF NOT EXISTS {table_name} (key Int64, value String) Engine=ReplicatedMergeTree('/test_parallel_replicas/shard1/{table_name}', 'r2') ORDER BY (key)"
        )
        nodes[2].query(
            f"CREATE TABLE IF NOT EXISTS {table_name} (key Int64, value String) Engine=ReplicatedMergeTree('/test_parallel_replicas/shard1/{table_name}', 'r3') ORDER BY (key)"
        )
        nodes[3].query(
            f"CREATE TABLE IF NOT EXISTS {table_name} (key Int64, value String) Engine=ReplicatedMergeTree('/test_parallel_replicas/shard1/{table_name}', 'r4') ORDER BY (key)"
        )
    elif cluster == "test_multiple_shards_multiple_replicas":
        # shard 1
        nodes[0].query(
            f"CREATE TABLE IF NOT EXISTS {table_name} (key Int64, value String) Engine=ReplicatedMergeTree('/test_parallel_replicas/shard1/{table_name}', 'r1') ORDER BY (key)"
        )
        nodes[1].query(
            f"CREATE TABLE IF NOT EXISTS {table_name} (key Int64, value String) Engine=ReplicatedMergeTree('/test_parallel_replicas/shard1/{table_name}', 'r2') ORDER BY (key)"
        )
        nodes[2].query(
            f"CREATE TABLE IF NOT EXISTS {table_name} (key Int64, value String) Engine=ReplicatedMergeTree('/test_parallel_replicas/shard1/{table_name}', 'r3') ORDER BY (key)"
        )
        # shard 2
        nodes[3].query(
            f"CREATE TABLE IF NOT EXISTS {table_name} (key Int64, value String) Engine=ReplicatedMergeTree('/test_parallel_replicas/shard2/{table_name}', 'r1') ORDER BY (key)"
        )
        nodes[4].query(
            f"CREATE TABLE IF NOT EXISTS {table_name} (key Int64, value String) Engine=ReplicatedMergeTree('/test_parallel_replicas/shard2/{table_name}', 'r2') ORDER BY (key)"
        )
        nodes[5].query(
            f"CREATE TABLE IF NOT EXISTS {table_name} (key Int64, value String) Engine=ReplicatedMergeTree('/test_parallel_replicas/shard2/{table_name}', 'r3') ORDER BY (key)"
        )
    else:
        raise Exception(f"Unexpected cluster: {cluster}")

    # create distributed table
    nodes[0].query(f"DROP TABLE IF EXISTS {table_name}_d SYNC")
    nodes[0].query(
        f"""
            CREATE TABLE {table_name}_d AS {table_name}
            Engine=Distributed(
                {cluster},
                currentDatabase(),
                {table_name},
                key
            )
            """
    )

    # populate data
    nodes[0].query(
        f"INSERT INTO {table_name}_d SELECT number, number FROM numbers(1000)",
        settings={"distributed_foreground_insert": 1},
    )
    nodes[0].query(
        f"INSERT INTO {table_name}_d SELECT number, number FROM numbers(2000)",
        settings={"distributed_foreground_insert": 1},
    )
    nodes[0].query(
        f"INSERT INTO {table_name}_d SELECT -number, -number FROM numbers(1000)",
        settings={"distributed_foreground_insert": 1},
    )
    nodes[0].query(
        f"INSERT INTO {table_name}_d SELECT -number, -number FROM numbers(2000)",
        settings={"distributed_foreground_insert": 1},
    )
    nodes[0].query(
        f"INSERT INTO {table_name}_d SELECT number, number FROM numbers(3)",
        settings={"distributed_foreground_insert": 1},
    )


@pytest.mark.parametrize(
    "cluster,max_parallel_replicas,prefer_localhost_replica",
    [
        pytest.param("test_single_shard_multiple_replicas", 2, 0),
        pytest.param("test_single_shard_multiple_replicas", 3, 0),
        pytest.param("test_single_shard_multiple_replicas", 4, 0),
        pytest.param("test_single_shard_multiple_replicas", 10, 0),
        pytest.param("test_single_shard_multiple_replicas", 2, 1),
        pytest.param("test_single_shard_multiple_replicas", 3, 1),
        pytest.param("test_single_shard_multiple_replicas", 4, 1),
        pytest.param("test_single_shard_multiple_replicas", 10, 1),
        pytest.param("test_multiple_shards_multiple_replicas", 2, 0),
        pytest.param("test_multiple_shards_multiple_replicas", 3, 0),
        pytest.param("test_multiple_shards_multiple_replicas", 4, 0),
        pytest.param("test_multiple_shards_multiple_replicas", 10, 0),
        pytest.param("test_multiple_shards_multiple_replicas", 2, 1),
        pytest.param("test_multiple_shards_multiple_replicas", 3, 1),
        pytest.param("test_multiple_shards_multiple_replicas", 4, 1),
        pytest.param("test_multiple_shards_multiple_replicas", 10, 1),
    ],
)
def test_parallel_replicas_over_distributed(
    start_cluster, cluster, max_parallel_replicas, prefer_localhost_replica
):
    table_name = "test_table"
    create_tables(cluster, table_name)

    node = nodes[0]
    expected_result = f"6003\t-1999\t1999\t3\n"

    # sync all replicas to get consistent result
    node.query(f"SYSTEM SYNC REPLICA ON CLUSTER {cluster} {table_name}")

    # parallel replicas
    assert (
        node.query(
            f"SELECT count(), min(key), max(key), sum(key) FROM {table_name}_d",
            settings={
                "enable_parallel_replicas": 2,
                "prefer_localhost_replica": prefer_localhost_replica,
                "max_parallel_replicas": max_parallel_replicas,
            },
        )
        == expected_result
    )

    # w/o parallel replicas
    assert (
        node.query(
            f"SELECT count(), min(key), max(key), sum(key) FROM {table_name}_d",
            settings={
                "enable_parallel_replicas": 0,
            },
        )
        == expected_result
    )
