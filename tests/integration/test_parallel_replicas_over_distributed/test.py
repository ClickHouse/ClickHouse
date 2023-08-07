import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

nodes = [
    cluster.add_instance(f"n{i}", main_configs=["configs/remote_servers.xml"], with_zookeeper=True)
    for i in (1, 2, 3, 4)
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
        # shard 2
        nodes[2].query(
            f"CREATE TABLE IF NOT EXISTS {table_name} (key Int64, value String) Engine=ReplicatedMergeTree('/test_parallel_replicas/shard2/{table_name}', 'r1') ORDER BY (key)"
        )
        nodes[3].query(
            f"CREATE TABLE IF NOT EXISTS {table_name} (key Int64, value String) Engine=ReplicatedMergeTree('/test_parallel_replicas/shard2/{table_name}', 'r2') ORDER BY (key)"
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
    nodes[0].query(f"INSERT INTO {table_name}_d SELECT number, number FROM numbers(1000)", settings={"insert_distributed_sync": 1})
    nodes[0].query(f"INSERT INTO {table_name}_d SELECT number, number FROM numbers(2000)", settings={"insert_distributed_sync": 1})
    nodes[0].query(f"INSERT INTO {table_name}_d SELECT -number, -number FROM numbers(1000)", settings={"insert_distributed_sync": 1})
    nodes[0].query(f"INSERT INTO {table_name}_d SELECT -number, -number FROM numbers(2000)", settings={"insert_distributed_sync": 1})
    nodes[0].query(f"INSERT INTO {table_name}_d SELECT number, number FROM numbers(1)", settings={"insert_distributed_sync": 1})


@pytest.mark.parametrize(
    "cluster",
    ["test_single_shard_multiple_replicas", "test_multiple_shards_multiple_replicas"],
)
def test_parallel_replicas_custom_key(start_cluster, cluster):
    table_name = "test_table"
    create_tables(cluster, table_name)

    node = nodes[0]
    expected_result = f"6001\t-1999\t1999\t0\n"

    # w/o parallel replicas
    assert (
        node.query(
            f"SELECT count(), min(key), max(key), sum(key) FROM {table_name}_d"
        )
        == expected_result
    )

    # parallel replicas
    assert (
        node.query(
            f"SELECT count(), min(key), max(key), sum(key) FROM {table_name}_d",
            settings={
                "allow_experimental_parallel_reading_from_replicas": 2,
                "prefer_localhost_replica": 0,
                "max_parallel_replicas": 4,
                "use_hedged_requests": 0,
                "cluster_for_parallel_replicas": cluster,
            },
        )
        == expected_result
    )
