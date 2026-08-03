import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
nodes = [
    cluster.add_instance(
        "node0",
        main_configs=["configs/clusters.xml"],
        with_zookeeper=True,
        image="library/clickhouse",
        tag="25.1",
        stay_alive=True,
        with_installed_binary=True,
    )
] + [
    cluster.add_instance(
        f"node{num + 1}",
        main_configs=["configs/clusters.xml"],
        with_zookeeper=True,
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
    for num in range(len(nodes)):
        node = nodes[num]
        node.query("drop table if exists t sync")
        node.query(
            f"""
            create table if not exists t (a UInt64)
            engine = ReplicatedMergeTree('/test_simple_distributed_aggregation_with_parallel_replicas/shard0/t', '{num}')
            order by ()
        """
        )
        node.query("insert into t select number % 100000 from numbers_mt(1e7)")
        node.query("optimize table t final")

    # The initiator has an old version. If replicas won't recognize that fact, we will get an error like:
    # "Unknown serialization kind 2: while receiving packet from node1:9000"
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
            "merge_tree_min_bytes_per_task_for_remote_reading": 1,
            "max_threads": 2,
        },
    )

    # The initiator has a newer version. It is totally fine. It only means that some replicas will send columns
    # with new serialization and some other will use only the old serialization. Still let's check that it works.
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
            "merge_tree_min_bytes_per_task_for_remote_reading": 1,
            "max_threads": 2,
        },
    )
