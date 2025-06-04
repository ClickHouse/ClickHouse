import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
nodes = [
    cluster.add_instance(
        "node0",
        main_configs=["configs/clusters.xml"],
        with_zookeeper=False,
        image="library/clickhouse",
        tag="25.1",
        stay_alive=True,
        with_installed_binary=True,
    )
] + [
    cluster.add_instance(
        f"node{num + 1}",
        main_configs=["configs/clusters.xml"],
        with_zookeeper=False,
    )
    for num in range(2)
]


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_simple_distributed_aggregation_with_parallel_replicas(start_cluster):
    for node in nodes:
        node.query("drop table if exists t")
        node.query("create table t (a UInt64) engine = MergeTree() order by tuple()")
        node.query("insert into t select number % 100000 from numbers_mt(1e7)")
        node.query("optimize table t final")

    # when initiator has an old version
    nodes[0].query(
        """
        select sum(a)
        from t
        group by a
        format Null
        """,
        settings={
            "cluster_for_parallel_replicas": "parallel_replicas",
            "enable_analyzer": 1,
            "allow_experimental_parallel_reading_from_replicas": 1,
            "max_parallel_replicas": 3,
            "parallel_replicas_for_non_replicated_merge_tree": 1,
            "merge_tree_min_bytes_per_task_for_remote_reading": 1,
            "max_threads": 2,
        },
    )

    # when initiator has a newer version
    nodes[2].query(
        """
        select sum(a)
        from t
        group by a
        format Null
        """,
        settings={
            "cluster_for_parallel_replicas": "parallel_replicas",
            "enable_analyzer": 1,
            "allow_experimental_parallel_reading_from_replicas": 1,
            "max_parallel_replicas": 3,
            "parallel_replicas_for_non_replicated_merge_tree": 1,
            "merge_tree_min_bytes_per_task_for_remote_reading": 1,
            "max_threads": 2,
        },
    )

    for node in nodes:
        node.query("drop table t")

