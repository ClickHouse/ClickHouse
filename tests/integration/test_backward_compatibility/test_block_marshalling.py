import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
cluster_name = "parallel_replicas"
nodes = [
    cluster.add_instance(
        f"node{num}",
        main_configs=["configs/clusters.xml"],
        with_zookeeper=False,
        image="library/clickhouse",
        tag="25.1",
        stay_alive=True,
        use_old_analyzer=True,
        with_installed_binary=True,
    )
    for num in range(2)
] + [
    cluster.add_instance(
        "node2",
        main_configs=["configs/clusters.xml"],
        with_zookeeper=False,
        use_old_analyzer=True,
    )
]


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_query_plan_for_replicas(start_cluster):
    for node in nodes:
        node.query("create table t (a UInt64) engine = MergeTree order by tuple()")
        node.query("insert into t select number % 100000 from numbers_mt(1000000)")

    # when initiator has an old version
    assert (
        nodes[0].query(
            """
            explain distributed=1
            select sum(a)
            from t
            """,
            settings={
                "cluster_for_parallel_replicas": "parallel_replicas",
                "max_parallel_replicas": 3,
                "allow_experimental_parallel_reading_from_replicas": 1,
                "parallel_replicas_for_non_replicated_merge_tree": 1,
            },
        )
        == """Expression ((Projection + Before ORDER BY))
  MergingAggregated
    ReadFromRemoteParallelReplicas (Query: SELECT sum(`a`) FROM `default`.`t` Replicas: node0:9000, node1:9000, node2:9000)
      Expression ((Projection + Before ORDER BY))
        Aggregating
          Expression (Before GROUP BY)
            ReadFromMergeTree (default.t)
      Expression ((Projection + Before ORDER BY))
        Aggregating
          Expression (Before GROUP BY)
            ReadFromMergeTree (default.t)
      Aggregating
        Expression (Before GROUP BY)
          ReadFromMergeTree (default.t)
"""
    )

    # when initiator has a newer version
    assert (
        nodes[2].query(
            """
            explain distributed=1
            select sum(a)
            from t
            group by a
            """,
            settings={
                "cluster_for_parallel_replicas": "parallel_replicas",
                "enable_analyzer": 1,
                "enable_parallel_replicas": 1,
                "max_parallel_replicas": 3,
                "parallel_replicas_for_non_replicated_merge_tree": 1,
            },
        )
        == """Expression ((Project names + Projection))
  MergingAggregated
    Union
      Aggregating
        Expression ((Before GROUP BY + Change column names to column identifiers))
          ReadFromMergeTree (default.t)
      ReadFromRemoteParallelReplicas (Query: SELECT sum(`__table1`.`a`) AS `sum(a)` FROM `default`.`t` AS `__table1` GROUP BY `__table1`.`a` Replicas: node0:9000, node1:9000)
        Expression ((Project names + Projection))
          Aggregating
            Expression ((Before GROUP BY + Change column names to column identifiers))
              ReadFromMergeTree (default.t)
        Expression ((Project names + Projection))
          Aggregating
            Expression ((Before GROUP BY + Change column names to column identifiers))
              ReadFromMergeTree (default.t)
"""
    )

    for node in nodes:
        node.query("drop table t")

