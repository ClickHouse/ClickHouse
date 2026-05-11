"""
Test backward compatibility of parallel replicas protocol with stream_id
in a mixed-version cluster (old nodes don't have the stream_id field).
UNION ALL view exercises multi-stream coordinator dispatch.
"""

import uuid

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
        tag="25.12",
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


def test_parallel_replicas_with_view(start_cluster):
    for num in range(len(nodes)):
        node = nodes[num]
        node.query("DROP TABLE IF EXISTS t1 SYNC")
        node.query("DROP TABLE IF EXISTS t2 SYNC")
        node.query("DROP VIEW IF EXISTS v")
        node.query(
            f"""
            CREATE TABLE IF NOT EXISTS t1(key UInt64, value UInt64)
            ENGINE = ReplicatedMergeTree('/test_pr_protocol_stream_id/shard0/t1', '{num}')
            ORDER BY key
            """
        )
        node.query(
            f"""
            CREATE TABLE IF NOT EXISTS t2(key UInt64, value UInt64)
            ENGINE = ReplicatedMergeTree('/test_pr_protocol_stream_id/shard0/t2', '{num}')
            ORDER BY key
            """
        )
        node.query(
            "CREATE VIEW IF NOT EXISTS v AS SELECT key, value FROM t1 UNION ALL SELECT key, value FROM t2"
        )

    nodes[0].query(
        "INSERT INTO t1 SELECT number, number * 10 FROM numbers_mt(100000) ORDER BY ALL"
    )
    nodes[0].query(
        "INSERT INTO t2 SELECT number + 100000, number * 20 FROM numbers_mt(100000) ORDER BY ALL"
    )

    nodes[1].query("SYSTEM SYNC REPLICA t1")
    nodes[2].query("SYSTEM SYNC REPLICA t1")

    nodes[1].query("SYSTEM SYNC REPLICA t2")
    nodes[2].query("SYSTEM SYNC REPLICA t2")

    settings = {
        "enable_analyzer": 1,
        "enable_parallel_replicas": 1,
        "cluster_for_parallel_replicas": "parallel_replicas",
        "max_parallel_replicas": 3,
    }

    # t1: sum(n*10, n=0..99999) = 49999500000
    # t2: sum(n*20, n=0..99999) = 99999000000
    expected = "149998500000"

    result = nodes[0].query("SELECT sum(value) FROM v", settings=settings)
    assert result.strip() == expected

    result = nodes[1].query("SELECT sum(value) FROM v", settings=settings)
    assert result.strip() == expected

    def run_and_check(extra_settings, expected_unavailable, expected_used):
        """Run query from new node, verify result and that old replicas are unavailable."""
        merged = {**settings, **extra_settings}
        qid = f"test_pr_protocol_with_stream_id_{uuid.uuid4()}"
        result = nodes[2].query("SELECT sum(value) FROM v", settings=merged, query_id=qid)
        assert result.strip() == expected, f"Wrong result with settings {extra_settings}"

        # node2 is the new node, nodes 0 and 1 are old (no stream_id support).
        # Old replicas should be filtered out at connection time and marked unavailable.
        nodes[2].query("SYSTEM FLUSH LOGS")
        profile_events = nodes[2].query(
            f"""
            SELECT
                ProfileEvents['ParallelReplicasUnavailableCount'],
                ProfileEvents['ParallelReplicasUsedCount']
            FROM system.query_log
            WHERE type = 'QueryFinish'
                AND query_id = '{qid}'
            SETTINGS enable_parallel_replicas = 0
            """
        )
        assert profile_events.strip() == f"{expected_unavailable}\t{expected_used}", \
            f"Wrong profile events with settings {extra_settings}: {profile_events.strip()}"

    run_and_check({"parallel_replicas_allow_view_over_mergetree": 1}, 2, 1)
    run_and_check({"parallel_replicas_allow_view_over_mergetree": 1, "parallel_replicas_local_plan": 1}, 2, 1)
    run_and_check({"parallel_replicas_allow_view_over_mergetree": 1, "parallel_replicas_local_plan": 0}, 2, 1)
    # With view-over-MergeTree disabled, the view is expanded into two separate
    # sub-queries (one per underlying table), each with its own set of replicas.
    run_and_check({"parallel_replicas_allow_view_over_mergetree": 0}, 4, 2)

    for node in nodes:
        node.query("DROP VIEW IF EXISTS v")
        node.query("DROP TABLE IF EXISTS t2 SYNC")
        node.query("DROP TABLE IF EXISTS t1 SYNC")
