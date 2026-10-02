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

    def run_and_check(extra_settings, expected_unavailable, expected_used, *, strict_unavailable=False):
        """Run query from new node, verify result and that old replicas are filtered out.

        ``strict_unavailable=True`` requires `ParallelReplicasUnavailableCount` to equal
        `expected_unavailable` exactly. Use it for deterministic configurations
        (`parallel_replicas_local_plan = 0`). For racy configurations
        (`parallel_replicas_local_plan = 1`, default), the count is upper-bounded:
        see comment below.
        """
        merged = {**settings, **extra_settings}
        qid = f"test_pr_protocol_with_stream_id_{uuid.uuid4()}"
        result = nodes[2].query("SELECT sum(value) FROM v", settings=merged, query_id=qid)
        assert result.strip() == expected, f"Wrong result with settings {extra_settings}"

        # node2 is the new node, nodes 0 and 1 are old (no stream_id support).
        # Old replicas are filtered out at connection time inside `RemoteQueryExecutor`'s
        # `create_connections` lambda: the version check disconnects entries whose
        # `parallel_replicas_version` is below `DBMS_PARALLEL_REPLICAS_MIN_VERSION_WITH_STREAM_ID`.
        # The `ParallelReplicasUnavailableCount` ProfileEvent is incremented later in
        # `RemoteQueryExecutor::sendQueryUnlocked` when it observes the empty
        # `MultiplexedConnections`. However, when `parallel_replicas_local_plan = 1`,
        # the local plan can serve all required data quickly and cancel the pipeline,
        # in which case the `RemoteSource` for some old replicas may be cancelled
        # BEFORE `tryGenerate` ever calls `sendQuery` (see `RemoteSource::prepare`
        # short-circuit on `isCancelled`). For those replicas the version check never
        # runs and `ParallelReplicasUnavailableCount` is not incremented.
        # Hence with `parallel_replicas_local_plan = 1` (default) the count is bounded
        # above by `expected_unavailable` but can be smaller (down to 0) due to this
        # dispatch race; we accept any value in that range. With
        # `parallel_replicas_local_plan = 0` there is no local plan to short-circuit
        # the pipeline, so `sendQuery` always runs the version check and the count is
        # deterministic — strict equality is enforced via `strict_unavailable=True`.
        # `ParallelReplicasUsedCount` reflects how many task assignments the
        # coordinator handed out and remains deterministic in both cases — it must
        # equal `expected_used` (the number of new replicas times the number of
        # sub-queries). The query result is the primary correctness check; if any old
        # replica were ever used, the sum would also be wrong.
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
        unavail_str, used_str = profile_events.strip().split("\t")
        unavail = int(unavail_str)
        used = int(used_str)
        assert used == expected_used, (
            f"Wrong ParallelReplicasUsedCount with settings {extra_settings}: "
            f"got {used}, expected {expected_used}"
        )
        if strict_unavailable:
            assert unavail == expected_unavailable, (
                f"Wrong ParallelReplicasUnavailableCount with settings {extra_settings}: "
                f"got {unavail}, expected {expected_unavailable} (deterministic)"
            )
        else:
            assert 0 <= unavail <= expected_unavailable, (
                f"Wrong ParallelReplicasUnavailableCount with settings {extra_settings}: "
                f"got {unavail}, expected 0..{expected_unavailable}"
            )

    # `parallel_replicas_local_plan` defaults to 1 — racy path, upper-bounded check.
    run_and_check({"parallel_replicas_allow_view_over_mergetree": 1}, 2, 1)
    run_and_check({"parallel_replicas_allow_view_over_mergetree": 1, "parallel_replicas_local_plan": 1}, 2, 1)
    # `parallel_replicas_local_plan = 0` — no local plan, no early cancel, deterministic count.
    run_and_check(
        {"parallel_replicas_allow_view_over_mergetree": 1, "parallel_replicas_local_plan": 0},
        2,
        1,
        strict_unavailable=True,
    )
    # With view-over-MergeTree disabled, the view is expanded into two separate
    # sub-queries (one per underlying table), each with its own set of replicas.
    # `parallel_replicas_local_plan` defaults to 1 — racy path, upper-bounded check.
    run_and_check({"parallel_replicas_allow_view_over_mergetree": 0}, 4, 2)

    for node in nodes:
        node.query("DROP VIEW IF EXISTS v")
        node.query("DROP TABLE IF EXISTS t2 SYNC")
        node.query("DROP TABLE IF EXISTS t1 SYNC")
