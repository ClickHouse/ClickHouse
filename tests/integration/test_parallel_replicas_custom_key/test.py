import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

nodes = [
    cluster.add_instance(
        f"n{i}",
        main_configs=["configs/remote_servers.xml"],
        with_zookeeper=True,
        macros={"replica": f"r{i}"},
    )
    for i in range(1, 5)
]


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def insert_data(table_name, row_num, all_nodes=False):
    query = (
        f"INSERT INTO {table_name} SELECT number % 4, number FROM numbers({row_num})"
    )

    if all_nodes:
        for n in nodes:
            n.query(query)
    else:
        n1 = nodes[0]
        n1.query(query)


@pytest.mark.parametrize("custom_key", ["sipHash64(key)", "key"])
@pytest.mark.parametrize("parallel_replicas_mode", ["custom_key_sampling"])
@pytest.mark.parametrize(
    "cluster",
    ["test_multiple_shards_multiple_replicas", "test_single_shard_multiple_replicas"],
)
def test_parallel_replicas_custom_key_distributed(
    start_cluster, cluster, custom_key, parallel_replicas_mode
):
    for node in nodes:
        node.rotate_logs()

    row_num = 1000

    n1 = nodes[0]
    n1.query(f"DROP TABLE IF EXISTS dist_table ON CLUSTER {cluster} SYNC")
    n1.query(f"DROP TABLE IF EXISTS test_table_for_dist ON CLUSTER {cluster} SYNC")
    n1.query(
        f"CREATE TABLE test_table_for_dist ON CLUSTER {cluster} (key UInt32, value String) Engine=MergeTree ORDER BY (key, sipHash64(value))"
    )

    n1.query(
        f"""
            CREATE TABLE dist_table AS test_table_for_dist
            Engine=Distributed(
                {cluster},
                currentDatabase(),
                test_table_for_dist,
                rand()
            )
            """
    )

    insert_data("dist_table", row_num)

    n1.query("SYSTEM FLUSH DISTRIBUTED dist_table")

    expected_result = ""
    for i in range(4):
        expected_result += f"{i}\t250\n"

    n1 = nodes[0]
    assert (
        n1.query(
            "SELECT key, count() FROM dist_table GROUP BY key ORDER BY key",
            settings={
                "max_parallel_replicas": 4,
                "parallel_replicas_custom_key": custom_key,
                "enable_parallel_replicas": 1,
                "parallel_replicas_mode": parallel_replicas_mode,
                "prefer_localhost_replica": 0,
            },
        )
        == expected_result
    )

    if cluster == "test_multiple_shards_multiple_replicas":
        # we simply process query on all replicas for each shard by appending the filter on replica
        assert all(
            node.contains_in_log("Processing query on a replica using custom_key")
            for node in nodes
        )


@pytest.mark.parametrize("custom_key", ["sipHash64(key)", "key"])
@pytest.mark.parametrize(
    "parallel_replicas_mode", ["custom_key_sampling", "custom_key_range"]
)
@pytest.mark.parametrize(
    "cluster",
    ["test_single_shard_multiple_replicas"],
)
def test_parallel_replicas_custom_key_mergetree(
    start_cluster, cluster, custom_key, parallel_replicas_mode
):
    for node in nodes:
        node.rotate_logs()

    row_num = 1000
    n1 = nodes[0]
    n1.query(f"DROP TABLE IF EXISTS test_table_for_mt ON CLUSTER {cluster} SYNC")
    n1.query(
        f"CREATE TABLE test_table_for_mt ON CLUSTER {cluster} (key UInt32, value String) Engine=MergeTree ORDER BY (key, sipHash64(value))"
    )

    insert_data("test_table_for_mt", row_num, all_nodes=True)

    expected_result = ""
    for i in range(4):
        expected_result += f"{i}\t250\n"

    n1 = nodes[0]
    assert (
        n1.query(
            "SELECT key, count() FROM test_table_for_mt GROUP BY key ORDER BY key",
            settings={
                "max_parallel_replicas": 4,
                "enable_parallel_replicas": 1,
                "parallel_replicas_custom_key": custom_key,
                "parallel_replicas_mode": parallel_replicas_mode,
                "parallel_replicas_for_non_replicated_merge_tree": 1,
                "cluster_for_parallel_replicas": cluster,
            },
        )
        == expected_result
    )


@pytest.mark.parametrize("custom_key", ["sipHash64(key)", "key"])
@pytest.mark.parametrize(
    "parallel_replicas_mode", ["custom_key_sampling", "custom_key_range"]
)
@pytest.mark.parametrize(
    "cluster",
    ["test_single_shard_multiple_replicas"],
)
def test_parallel_replicas_custom_key_replicatedmergetree(
    start_cluster, cluster, custom_key, parallel_replicas_mode
):
    for node in nodes:
        node.rotate_logs()

    row_num = 1000
    n1 = nodes[0]
    n1.query(f"DROP TABLE IF EXISTS test_table_for_rmt ON CLUSTER {cluster} SYNC")
    n1.query(
        f"CREATE TABLE test_table_for_rmt ON CLUSTER {cluster} (key UInt32, value String) Engine=ReplicatedMergeTree('/clickhouse/tables', '{{replica}}') ORDER BY (key, sipHash64(value))"
    )

    insert_data("test_table_for_rmt", row_num, all_nodes=False)

    for node in nodes:
        node.query("SYSTEM SYNC REPLICA test_table_for_rmt LIGHTWEIGHT")

    expected_result = ""
    for i in range(4):
        expected_result += f"{i}\t250\n"

    n1 = nodes[0]
    assert (
        n1.query(
            "SELECT key, count() FROM test_table_for_rmt GROUP BY key ORDER BY key",
            settings={
                "max_parallel_replicas": 4,
                "enable_parallel_replicas": 1,
                "parallel_replicas_custom_key": custom_key,
                "parallel_replicas_mode": parallel_replicas_mode,
                "cluster_for_parallel_replicas": cluster,
            },
        )
        == expected_result
    )
