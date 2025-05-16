import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
cluster_name = "parallel_replicas"
nodes = [
    cluster.add_instance(
        f"node{num}",
        main_configs=["configs/clusters.xml"],
        with_zookeeper=False,
        image="clickhouse/clickhouse-server",
        tag="23.11",  # earlier versions lead to "Not found column sum(a) in block." exception 🤷
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


def test_backward_compatability(start_cluster):
    for node in nodes:
        node.query("create table t (a UInt64) engine = MergeTree order by tuple()")
        node.query("insert into t select number % 100000 from numbers_mt(1000000)")

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
                },
            )
            == "49999500000\n"
        )

    for node in nodes:
        node.query("drop table t")
