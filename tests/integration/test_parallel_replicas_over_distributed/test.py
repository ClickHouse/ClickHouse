import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

nodes = [
    cluster.add_instance(f"n{i}", main_configs=["configs/remote_servers.xml"])
    for i in (1, 2, 3, 4)
]


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def create_tables(cluster):
    for node in nodes:
        node.query("DROP TABLE IF EXISTS test_table")
        node.query("DROP TABLE IF EXISTS dist_table")
        node.query(
            "CREATE TABLE IF NOT EXISTS test_table (key Int64, value String) Engine=MergeTree ORDER BY (key)"
        )

    nodes[0].query(
        f"""
            CREATE TABLE dist_table AS test_table
            Engine=Distributed(
                {cluster},
                currentDatabase(),
                test_table,
                rand()
            )
            """
    )

    nodes[0].query(f"INSERT INTO test_table SELECT number, number FROM numbers(1000)")
    nodes[1].query(f"INSERT INTO test_table SELECT number, number FROM numbers(2000)")
    nodes[2].query(f"INSERT INTO test_table SELECT -number, -number FROM numbers(1000)")
    nodes[3].query(f"INSERT INTO test_table SELECT -number, -number FROM numbers(2000)")
    nodes[0].query(f"INSERT INTO test_table SELECT number, number FROM numbers(1)")


@pytest.mark.parametrize(
    "cluster",
    ["test_multiple_shards_multiple_replicas", "test_single_shard_multiple_replicas"],
)
def test_parallel_replicas_custom_key(start_cluster, cluster):
    create_tables(cluster)

    expected_result = f"6001\t-1999\t1999\t0\n"
    node = nodes[0]
    assert (
        node.query(
            "SELECT count(), min(key), max(key), sum(key) FROM dist_table",
            settings={
                "allow_experimental_parallel_reading_from_replicas": 1,
                "prefer_localhost_replica": 0,
                "max_parallel_replicas": 4,
                "use_hedged_requests": 1,
                "cluster_for_parallel_replicas": cluster,
            },
        )
        == expected_result
    )
