import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node0 = cluster.add_instance(
    "node0",
    main_configs=["configs/clusters.xml"],
    with_zookeeper=True,
    image="clickhouse/clickhouse-server",
    tag="23.11",  # earlier versions lead to "Not found column sum(a) in block." exception 🤷
    with_installed_binary=True,
)
node1 = cluster.add_instance("node1", with_zookeeper=True, use_old_analyzer=True)
node2 = cluster.add_instance("node2", with_zookeeper=True, use_old_analyzer=True)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_distributed_aggregation(start_cluster):
    for node in start_cluster.instances.values():
        node.query(
            """
            CREATE TABLE IF NOT EXISTS test_distributed_aggregation
            (
                a UInt64,
                b UInt64
            )
            ENGINE = MergeTree
            ORDER BY tuple();

            INSERT INTO test_distributed_aggregation SELECT
                number,
                number
            FROM numbers_mt(1e3);
            """
        )

    node1.query(
        """
        -- The goal is to have a heavy bucket with a small bucket id to have it delayed by the nodes;
        -- 30 belongs to the bucket #6, so it should do the job
        INSERT INTO test_distributed_aggregation SELECT
            30,
            number
        FROM numbers_mt(1e6);
        """
    )

    # Node0 has the old version, so it should be the initiator to catch the situation
    # when some of the replicas decided to send buckets out of order to the old initiator
    node0.query(
        """
        SELECT a, throwIf(if(a = 30, 1000000, 1) != uniqExact(b))
        FROM test_distributed_aggregation
        GROUP BY a
        FORMAT Null
        """,
        settings={
            "cluster_for_parallel_replicas": "parallel_replicas",
            "allow_experimental_parallel_reading_from_replicas": 1,
            "group_by_two_level_threshold": 1,
            "group_by_two_level_threshold_bytes": 1,
            "max_parallel_replicas": 3,
            "parallel_replicas_for_non_replicated_merge_tree": 1,
            "use_hedged_requests": 0,
        },
    )
