import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "n1", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
)
node2 = cluster.add_instance(
    "n2", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
)

nodes = [node1, node2]


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def create_tables(cluster, table_name):
    node1.query(f"DROP TABLE IF EXISTS {table_name} SYNC")
    node2.query(f"DROP TABLE IF EXISTS {table_name} SYNC")

    node1.query(
        f"CREATE TABLE IF NOT EXISTS {table_name} (key Int64, value String) Engine=ReplicatedMergeTree('/test_parallel_replicas/shard1/{table_name}', 'r1') ORDER BY (key)"
    )
    node2.query(
        f"CREATE TABLE IF NOT EXISTS {table_name} (key Int64, value String) Engine=ReplicatedMergeTree('/test_parallel_replicas/shard1/{table_name}', 'r2') ORDER BY (key)"
    )

    # create distributed table
    node1.query(f"DROP TABLE IF EXISTS {table_name}_d SYNC")
    node1.query(
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
    node1.query(
        f"INSERT INTO {table_name} SELECT number % 4, number FROM numbers(1000)"
    )
    node2.query(
        f"INSERT INTO {table_name} SELECT number % 4, number FROM numbers(1000, 1000)"
    )
    node1.query(f"SYSTEM SYNC REPLICA {table_name}")
    node2.query(f"SYSTEM SYNC REPLICA {table_name}")


@pytest.mark.parametrize("custom_key", ["sipHash64(key)", "key"])
@pytest.mark.parametrize("filter_type", ["default", "range"])
@pytest.mark.parametrize("prefer_localhost_replica", [0, 1])
@pytest.mark.parametrize("use_hedged_requests", [1, 0])
def test_parallel_replicas_custom_key_failover(
    start_cluster,
    custom_key,
    filter_type,
    use_hedged_requests,
    prefer_localhost_replica,
):
    for node in nodes:
        node.rotate_logs()

    cluster = "test_single_shard_multiple_replicas"
    table = "test_table"

    create_tables(cluster, table)

    expected_result = ""
    for i in range(4):
        expected_result += f"{i}\t500\n"

    assert (
        node1.query(
            f"SELECT key, count() FROM {table}_d GROUP BY key ORDER BY key",
            settings={
                "prefer_localhost_replica": prefer_localhost_replica,
                "max_parallel_replicas": 3,
                "parallel_replicas_custom_key": custom_key,
                "parallel_replicas_custom_key_filter_type": filter_type,
                "use_hedged_requests": use_hedged_requests,
            },
        )
        == expected_result
    )
