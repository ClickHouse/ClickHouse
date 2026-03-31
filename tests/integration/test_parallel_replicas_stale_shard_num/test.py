"""
Regression test for: LOGICAL_ERROR 'Shard number is greater than shard count'
when cluster_for_parallel_replicas has fewer shards than the outer Distributed
cluster.

Trigger scenario (mixed analyzer config between initiator and remote):

  Initiator (n1) — old analyzer profile (allow_experimental_analyzer=0):
    canUseTaskBasedParallelReplicas() = false
    → cluster_for_parallel_replicas NOT overridden in ReadFromRemote::addPipe
    → _shard_num IS set unconditionally (shard 2 → _shard_num=2)

  Remote shard 2 (n3) — allow_experimental_analyzer forced to 1 via constraint:
    TCPHandler receives allow_experimental_analyzer=0 from n1 as a
    non-initial (distributed sub-)query → silently clamped to 1.
    canUseTaskBasedParallelReplicas() = true (new planner, PR enabled)
    → prepareClusterForParallelReplicas reads _shard_num=2, shard_count=1
    → OLD code: LOGICAL_ERROR (server exception)
    → NEW code: LOG_WARNING + use all replicas (this test verifies)
"""

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# n1 — initiator, old analyzer.  canUseTaskBasedParallelReplicas()=false
# because allow_experimental_analyzer=0 AND parallel_replicas_only_with_analyzer=1
# (compiled default, so it is NOT sent to remotes).
n1 = cluster.add_instance(
    "n1",
    main_configs=["configs/remote_servers.xml", "configs/enable_text_log.xml"],
    use_old_analyzer=True,
)

# n2 — remote shard 1.  _shard_num=1 ≤ shard_count=1, so no stale-shard_num
# condition.  No special config needed.
n2 = cluster.add_instance(
    "n2",
    main_configs=["configs/remote_servers.xml", "configs/enable_text_log.xml"],
    use_old_analyzer=False,
)

# n3 — remote shard 2.  _shard_num=2 > shard_count=1 triggers the bug.
# The <min>1</min> constraint on allow_experimental_analyzer forces the new
# planner even when n1 sends allow_experimental_analyzer=0.
n3 = cluster.add_instance(
    "n3",
    main_configs=["configs/remote_servers.xml", "configs/enable_text_log.xml"],
    user_configs=["configs/force_allow_experimental_analyzer.xml"],
    use_old_analyzer=False,
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_stale_shard_num_single_shard_pr_cluster(start_cluster):
    """
    Outer Distributed cluster has 2 shards (n2, n3).
    cluster_for_parallel_replicas points to a 1-shard / 2-replica cluster
    (both n2 and n3).

    The mismatch in analyzer settings means the cluster override in
    ReadFromRemote::addPipe does not fire, so _shard_num=2 leaks into
    prepareClusterForParallelReplicas on shard 2 (n3).

    Before the fix: LOGICAL_ERROR (server exception).
    After the fix: LOG_WARNING, query succeeds, correct result returned.
    """
    for node in (n2, n3):
        node.query("DROP TABLE IF EXISTS t")
        node.query("CREATE TABLE t (n UInt64) ENGINE = MergeTree() ORDER BY n")
        node.query("INSERT INTO t SELECT * FROM numbers(10)")

    n1.query("DROP TABLE IF EXISTS t_dist")
    n1.query(
        "CREATE TABLE t_dist (n UInt64)"
        " ENGINE = Distributed('outer_2_shards', default, t)"
    )

    pr_settings = {
        "enable_parallel_replicas": 2,
        "max_parallel_replicas": 2,
        "parallel_replicas_for_non_replicated_merge_tree": 1,
        "cluster_for_parallel_replicas": "pr_1_shard_2_replicas",
        "serialize_query_plan": 0,
        "automatic_parallel_replicas_mode": 0,
        # Disable the trivial count optimization so that count() actually reads data.
        # Without this, n3 sees "Disabling parallel replicas to be able to use a trivial
        # count optimization" and prepareClusterForParallelReplicas is never called,
        # preventing the stale shard_num path from being exercised.
        "optimize_trivial_count_query": 0,
    }

    result = n1.query("SELECT count() FROM t_dist", settings=pr_settings)
    assert result.strip() == "20", f"Unexpected result: {result!r}"

    # Verify the WARNING was emitted on n3 (shard 2, _shard_num=2 > shard_count=1).
    n3.query("SYSTEM FLUSH LOGS text_log")
    warning_count = n3.query(
        """
        SELECT count()
        FROM system.text_log
        WHERE level = 'Warning'
          AND message LIKE '%shard_num%greater than shard count%'
          AND event_time >= now() - INTERVAL 120 SECOND
        """
    )
    assert int(warning_count.strip()) > 0, (
        "Expected WARNING about stale _shard_num on n3, but none found in text_log"
    )

    n1.query("DROP TABLE t_dist")
    for node in (n2, n3):
        node.query("DROP TABLE t")


def test_stale_shard_num_multi_shard_pr_cluster(start_cluster):
    """
    Outer Distributed cluster has 3 shards (n2, n2, n3).
    cluster_for_parallel_replicas points to a 2-shard cluster (n2, n3).

    n3 is shard 3 of the outer cluster, so it receives _shard_num=3.
    On n3, canUseParallelReplicasOnInitiator reads _shard_num=3 and
    shard_count=2 from pr_2_shards → shard_num > shard_count.

    Before the fix: LOGICAL_ERROR (causes server abort in debug-like builds).
    After the fix: UNEXPECTED_CLUSTER exception; n3 remains alive.
    """
    for node in (n2, n3):
        node.query("DROP TABLE IF EXISTS t2")
        node.query("CREATE TABLE t2 (n UInt64) ENGINE = MergeTree() ORDER BY n")
        node.query("INSERT INTO t2 SELECT * FROM numbers(10)")

    n1.query("DROP TABLE IF EXISTS t2_dist")
    n1.query(
        "CREATE TABLE t2_dist (n UInt64)"
        " ENGINE = Distributed('outer_3_shards', default, t2)"
    )

    pr_settings = {
        "enable_parallel_replicas": 2,
        "max_parallel_replicas": 2,
        "parallel_replicas_for_non_replicated_merge_tree": 1,
        "cluster_for_parallel_replicas": "pr_2_shards",
        "serialize_query_plan": 0,
        "automatic_parallel_replicas_mode": 0,
        "optimize_trivial_count_query": 0,
    }

    with pytest.raises(Exception) as exc_info:
        n1.query("SELECT count() FROM t2_dist", settings=pr_settings)
    assert "UNEXPECTED_CLUSTER" in str(exc_info.value), (
        f"Expected UNEXPECTED_CLUSTER exception, got: {exc_info.value!r}"
    )

    # Verify n3 is still alive (not aborted by LOGICAL_ERROR).
    assert n3.query("SELECT 1").strip() == "1", "n3 should be alive after UNEXPECTED_CLUSTER"

    n1.query("DROP TABLE t2_dist")
    for node in (n2, n3):
        node.query("DROP TABLE t2")
