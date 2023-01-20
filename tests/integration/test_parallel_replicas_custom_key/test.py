import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

n1 = cluster.add_instance(
    "n1", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
)
n2 = cluster.add_instance(
    "n2", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
)
n3 = cluster.add_instance(
    "n3", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
)
n4 = cluster.add_instance(
    "n4", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
)
nodes = [n1, n2, n3, n4]


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def create_tables(cluster):
    n1.query("DROP TABLE IF EXISTS dist_table")
    n1.query(f"DROP TABLE IF EXISTS test_table ON CLUSTER {cluster}")

    n1.query(
        f"CREATE TABLE test_table ON CLUSTER {cluster} (key Int32, value String) Engine=MergeTree ORDER BY (key, sipHash64(value))"
    )
    n1.query(
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


def insert_data(cluster, row_num):
    create_tables(cluster)
    n1.query(f"INSERT INTO dist_table SELECT number, number FROM numbers({row_num})")
    n1.query("SYSTEM FLUSH DISTRIBUTED dist_table")


@pytest.mark.parametrize("custom_key", ["sipHash64(value)", "key"])
@pytest.mark.parametrize("filter_type", ["default", "range"])
@pytest.mark.parametrize(
    "cluster",
    ["test_multiple_shards_multiple_replicas", "test_single_shard_multiple_replicas"],
)
def test_parallel_replicas_custom_key(start_cluster, cluster, custom_key, filter_type):
    for node in nodes:
        node.rotate_logs()

    row_num = 1000
    insert_data(cluster, row_num)
    assert (
        int(
            n1.query(
                "SELECT count() FROM dist_table",
                settings={
                    "prefer_localhost_replica": 0,
                    "max_parallel_replicas": 3,
                    "parallel_replicas_mode": "custom_key",
                    "parallel_replicas_custom_key": custom_key,
                    "parallel_replicas_custom_key_filter_type": filter_type,
                },
            )
        )
        == row_num
    )

    if cluster == "test_multiple_shards_multiple_replicas":
        # we simply process query on all replicas for each shard by appending the filter on replica
        assert all(
            node.contains_in_log("Processing query on a replica using custom_key")
            for node in nodes
        )
    else:
        # we first transform all replicas into shards and then append for each shard filter
        assert n1.contains_in_log(
            "Single shard cluster used with custom_key, transforming replicas into virtual shards"
        )
