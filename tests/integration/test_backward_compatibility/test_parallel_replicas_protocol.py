import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
cluster_name = "parallel_replicas"
nodes = [
    cluster.add_instance(
        f"node{num}",
        main_configs=["configs/clusters.xml"],
        with_zookeeper=True,
        image="clickhouse/clickhouse-server",
        tag="24.3",  # earlier versions lead to "Not found column sum(a) in block." exception 🤷
        stay_alive=True,
        use_old_analyzer=False,
        with_installed_binary=True,
    )
    for num in range(2)
] + [
    cluster.add_instance(
        "node2",
        main_configs=["configs/clusters.xml"],
        with_zookeeper=True,
        use_old_analyzer=False,
    )
]


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_backward_compatability(start_cluster):
    for num in range(len(nodes)):
        node = nodes[num]
        node.query("drop table if exists t sync")
        node.query(
            f"""
            create table if not exists t(a UInt64)
            engine = ReplicatedMergeTree('/test_backward_compatability/test_parallel_replicas_protocol/shard0/t', '{num}')
            order by (a)
        """
        )
        node.query("insert into t select number % 100000 from numbers_mt(1000000)")
        node.query("optimize table t final")

    # all we want is the query to run without errors
    for node in nodes:
        assert (
            node.query(
                """
                select sum(a)
                from t
                """,
                settings={
                    "cluster_for_parallel_replicas": "parallel_replicas",
                    "max_parallel_replicas": 3,
                    "allow_experimental_parallel_reading_from_replicas": 1,
                    "parallel_replicas_for_non_replicated_merge_tree": 1,
                    "merge_tree_min_rows_for_concurrent_read": 0,
                    "merge_tree_min_bytes_for_concurrent_read": 0,
                    "merge_tree_min_read_task_size": 1,
                },
            )
            == "49999500000\n"
        )

    for node in nodes:
        node.query("drop table t sync")
