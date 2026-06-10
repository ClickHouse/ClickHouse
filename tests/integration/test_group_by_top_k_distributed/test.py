"""
Correctness tests for `enable_group_by_top_k_optimization` under non-final
(partial) aggregation.

The optimization is currently gated to FINAL aggregating steps (see
`validateAggregatingStep` in `optimizeGroupByLimitPushdown.cpp`).  In
distributed and parallel-replicas scenarios the shards run partial
aggregation, so as written the optimization never kicks in there.

These tests pin the behaviour from the coordinator's perspective:
the result with the optimization enabled must equal the result with it
disabled, for both:

  * Pattern 1 (`GROUP BY ... ORDER BY <prefix> LIMIT N`) - which the analysis
    in `optimizeGroupByLimitPushdown.cpp` argues would still be safe if the
    gate were relaxed to partial aggregation.
  * Pattern 2 (`GROUP BY ... LIMIT N`) - which is NOT safe on the partial
    side because the coordinator's final `LIMIT` has no ORDER BY to discard
    keys with corrupted partial state.

The data is deliberately skewed so the heap would actually filter rows if
allowed: shard 1 has many distinct small keys (would fill an N=10 heap with
small-key entries on its own), shard 2 has a few large-key rows whose
aggregates must remain complete to produce the correct global answer.

If anyone relaxes the `!getFinal()` guard incorrectly, these tests should
catch the regression by observing diverging values between
`enable_group_by_top_k_optimization = 0` and `= 1`.
"""

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/remote_servers.xml"],
    with_zookeeper=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/remote_servers.xml"],
    with_zookeeper=True,
)


def _make_local_shards():
    """Create plain MergeTree shards on each node and load skewed data.

    Each shard sees a different slice of keys, with overlap so that some
    keys live on both shards.  The fanout per key is intentionally non-uniform
    so a per-shard top-K heap (if it were ever applied to partial aggregation)
    would drop rows whose aggregate state the coordinator still needs.
    """
    for node in (node1, node2):
        node.query("DROP TABLE IF EXISTS t_local SYNC")
        node.query(
            """
            CREATE TABLE t_local
            (
                k UInt32,
                v UInt64
            )
            ENGINE = MergeTree
            ORDER BY k
            """
        )

    # Shard 1: keys 0..999, every key with 100 rows, val = 1.
    # This shard alone has 1000 distinct keys with weight 100 each.
    node1.query(
        """
        INSERT INTO t_local
        SELECT number % 1000, 1
        FROM numbers(100000)
        """
    )

    # Shard 2: only large keys (10000..10009), each with 1 row, val = 1.
    # If shard 1 ran a partial top-K=10 by key ASC, the keys it would keep
    # are {0..9}.  Shard 2 keeps only its 10 large keys.  The merged view
    # has 20 keys; the global top-10 by ASC key is {0..9} - and the aggregate
    # of each must include the full 100-row contribution from shard 1.
    node2.query(
        """
        INSERT INTO t_local
        SELECT 10000 + number, 1
        FROM numbers(10)
        """
    )

    # Add an overlap row on shard 2: small key that ALSO exists on shard 1.
    # If a partial heap on shard 2 ever pruned this, the merged sum() for
    # key=5 would lose shard 2's contribution.
    node2.query("INSERT INTO t_local VALUES (5, 1)")


def _create_replicated_shards(table):
    """Create a replicated table on both nodes for the parallel-replicas pattern."""
    for node in (node1, node2):
        node.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node1.query(
        f"""
        CREATE TABLE {table}
        (
            k UInt32,
            v UInt64
        )
        ENGINE = ReplicatedMergeTree('/test_gby_topk/{table}', 'r1')
        ORDER BY k
        """
    )
    node2.query(
        f"""
        CREATE TABLE {table}
        (
            k UInt32,
            v UInt64
        )
        ENGINE = ReplicatedMergeTree('/test_gby_topk/{table}', 'r2')
        ORDER BY k
        """
    )
    # Populate from one node; the other catches up via replication.
    node1.query(
        f"""
        INSERT INTO {table}
        SELECT number % 1000, 1
        FROM numbers(100000)
        """
    )
    node1.query(
        f"""
        INSERT INTO {table}
        SELECT 10000 + number, 1
        FROM numbers(10)
        """
    )
    node1.query(f"INSERT INTO {table} VALUES (5, 1)")
    node2.query(f"SYSTEM SYNC REPLICA {table}")


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def _run(node, query, opt):
    return node.query(query, settings={"enable_group_by_top_k_optimization": opt})


def _assert_same_result(node, query):
    """The optimization must not change the result."""
    off = _run(node, query, 0)
    on = _run(node, query, 1)
    assert off == on, (
        f"enable_group_by_top_k_optimization changed the result.\n"
        f"  query: {query}\n"
        f"  off:\n{off}\n  on:\n{on}\n"
    )


# ---------------------------------------------------------------------------
# Distributed via remote() table function (two physical shards)
# ---------------------------------------------------------------------------


def test_distributed_remote_pattern1_order_by_prefix(start_cluster):
    """`GROUP BY k ORDER BY k LIMIT N` over a Distributed table.

    Analysis says relaxing the gate would still be correct here, because a
    key filtered by a shard's local heap cannot be in the global top-N.
    This is the pattern the optimization is most likely to be extended to,
    so it gets the strongest assertion.
    """
    _make_local_shards()
    query = (
        "SELECT k, sum(v) "
        "FROM remote('node{1,2}', currentDatabase(), t_local) "
        "GROUP BY k "
        "ORDER BY k ASC "
        "LIMIT 10"
    )
    _assert_same_result(node1, query)

    # Expected ground truth: keys 0..9, each present on shard 1 only except
    # key=5 which also has 1 row from shard 2.
    expected = "\n".join(
        f"{k}\t{100 if k != 5 else 101}" for k in range(10)
    ) + "\n"
    assert _run(node1, query, 0) == expected


def _make_sharded_distributed_table():
    """A `Distributed` table with sharding key = `k`, so that `GROUP BY k`
    + `distributed_push_down_limit = 1` activates the
    `WithMergeableStateAfterAggregationAndLimit` stage on shards.

    In that mode the shard's plan has both a (final = true) `AggregatingStep`
    AND a `LimitStep` above it - i.e. it's exactly the shape the
    `tryOptimizeGroupByLimitPushdown` optimization already matches, so the
    heap kicks in inside the shard's partial pipeline.  Note: `final` is
    `true` here because stage 4 > stage 1, so this doesn't exercise the
    `!getFinal()` branch.  It does verify that the optimization is already
    used cross-machine in this corner case.
    """
    for node in (node1, node2):
        node.query("DROP TABLE IF EXISTS t_sharded_local SYNC")
        node.query("DROP TABLE IF EXISTS t_sharded SYNC")
        node.query(
            "CREATE TABLE t_sharded_local (k UInt32, v UInt64) "
            "ENGINE = MergeTree ORDER BY k"
        )
        # Sharding key MUST match GROUP BY for the stage-4 optimization to
        # apply.  intHash64(k) % 2 maps each k deterministically to one shard.
        node.query(
            "CREATE TABLE t_sharded AS t_sharded_local "
            "ENGINE = Distributed(two_shards, currentDatabase(), t_sharded_local, intHash64(k))"
        )
    node1.query(
        "INSERT INTO t_sharded SELECT number % 1000, 1 FROM numbers(100000)",
        settings={"distributed_foreground_insert": 1},
    )
    node1.query(
        "INSERT INTO t_sharded SELECT 10000 + number, 1 FROM numbers(10)",
        settings={"distributed_foreground_insert": 1},
    )


def test_sharded_distributed_pattern1_with_push_down_limit(start_cluster):
    """Sanity-check that the optimization, applied on shards in stage 4
    (sharding-key-aligned GROUP BY with `distributed_push_down_limit = 1`),
    produces the same result as without it.

    This is *not* the partial-aggregation case the user is asking about,
    but it's the one cross-machine scenario where the existing optimization
    already runs at the shard side - confirming the shape of the plan tree
    that any non-final extension would have to recreate.
    """
    _make_sharded_distributed_table()
    query = (
        "SELECT k, sum(v) "
        "FROM t_sharded "
        "GROUP BY k ORDER BY k ASC LIMIT 10"
    )
    settings_base = {
        "distributed_push_down_limit": 1,
        "optimize_distributed_group_by_sharding_key": 1,
        "optimize_skip_unused_shards": 1,
    }
    settings_off = dict(settings_base, enable_group_by_top_k_optimization=0)
    settings_on = dict(settings_base, enable_group_by_top_k_optimization=1)
    off = node1.query(query, settings=settings_off)
    on = node1.query(query, settings=settings_on)
    assert off == on, (
        f"enable_group_by_top_k_optimization changed sharded-push-down result.\n"
        f"  off:\n{off}\n  on:\n{on}\n"
    )
    expected = "\n".join(f"{k}\t100" for k in range(10)) + "\n"
    assert off == expected


def test_explain_plan_shows_no_aggregating_step_with_limit(start_cluster):
    """Sanity check on the plan structure: in distributed mode, the
    coordinator's plan tree contains a `LimitStep` but no `AggregatingStep`
    (only `MergingAggregatedStep`).  The `AggregatingStep(final=false)` lives
    in the shards' own plan trees, where there is no `LimitStep`.

    This demonstrates *why* the `!getFinal()` gate in
    `optimizeGroupByLimitPushdown.cpp` is unreachable as written: the two
    steps never coexist in the same tree.
    """
    _make_local_shards()
    plan = node1.query(
        "EXPLAIN PLAN "
        "SELECT k, sum(v) "
        "FROM remote('node{1,2}', currentDatabase(), t_local) "
        "GROUP BY k ORDER BY k ASC LIMIT 10",
        settings={"enable_group_by_top_k_optimization": 0},
    )
    assert "Limit" in plan
    assert "MergingAggregated" in plan
    assert "Aggregating " not in plan, (
        "Initiator plan should not contain a non-final AggregatingStep; "
        "the partial aggregation lives inside the remote sub-query.\n"
        f"Got:\n{plan}"
    )


def test_distributed_remote_pattern1_order_by_desc(start_cluster):
    """`GROUP BY k ORDER BY k DESC LIMIT N` - DESC path of Pattern 1."""
    _make_local_shards()
    query = (
        "SELECT k, sum(v) "
        "FROM remote('node{1,2}', currentDatabase(), t_local) "
        "GROUP BY k "
        "ORDER BY k DESC "
        "LIMIT 5"
    )
    _assert_same_result(node1, query)


def test_distributed_remote_pattern1_composite_prefix(start_cluster):
    """`GROUP BY (k, v) ORDER BY k LIMIT N` - prefix-mode of Pattern 1."""
    _make_local_shards()
    query = (
        "SELECT k, v, count() "
        "FROM remote('node{1,2}', currentDatabase(), t_local) "
        "GROUP BY k, v "
        "ORDER BY k ASC "
        "LIMIT 10"
    )
    _assert_same_result(node1, query)


def test_distributed_remote_pattern2_no_order_by(start_cluster):
    """`GROUP BY k LIMIT N` (no ORDER BY) over a Distributed table.

    Pattern 2 is the one the gate definitively protects: there is no ORDER
    BY at the coordinator to evict tuples with corrupted partial state, so
    if the gate were ever wrongly relaxed for partial aggregation this
    would diverge.

    To make the comparison deterministic regardless of LIMIT's arbitrary
    tie-breaking, we compare a deterministic aggregate over the LIMIT'd
    rowset.
    """
    _make_local_shards()
    inner = (
        "SELECT k, sum(v) AS s "
        "FROM remote('node{1,2}', currentDatabase(), t_local) "
        "GROUP BY k "
        "LIMIT 100"
    )
    # Sum + sort the surviving (k, s) pairs to get a stable comparison.
    outer = f"SELECT sum(k), sum(s), count() FROM ({inner})"
    _assert_same_result(node1, outer)


# ---------------------------------------------------------------------------
# Parallel replicas (one shard, two replicas)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("max_parallel_replicas", [2])
def test_parallel_replicas_pattern1(start_cluster, max_parallel_replicas):
    """Parallel replicas exercise a different partial -> final split than
    `Distributed` but produce the same shape of plan: shards run partial
    aggregation, coordinator merges.  The optimization must give the same
    result with it on as with it off."""
    table = "t_pr"
    _create_replicated_shards(table)
    query = f"SELECT k, sum(v) FROM {table} GROUP BY k ORDER BY k ASC LIMIT 10"
    settings_off = {
        "enable_group_by_top_k_optimization": 0,
        "enable_parallel_replicas": 2,
        "max_parallel_replicas": max_parallel_replicas,
        "cluster_for_parallel_replicas": "one_shard_two_replicas",
    }
    settings_on = dict(settings_off, enable_group_by_top_k_optimization=1)
    off = node1.query(query, settings=settings_off)
    on = node1.query(query, settings=settings_on)
    assert off == on, (
        f"enable_group_by_top_k_optimization changed the parallel-replicas result.\n"
        f"  off:\n{off}\n  on:\n{on}\n"
    )

    expected = "\n".join(
        f"{k}\t{100 if k != 5 else 101}" for k in range(10)
    ) + "\n"
    assert off == expected


def test_remote_partial_aggregation_has_top_k(start_cluster):
    """`EXPLAIN PLAN distributed=1` exposes the remote sub-plan that is
    serialized to the parallel-replicas follower.  When
    `enable_group_by_top_k_optimization = 1` the helper added to
    `createRemotePlanForParallelReplicas` must mark the partial
    `AggregatingStep` with a `Top-K` annotation; turning the setting off
    must leave it absent.

    Without this assertion the test suite could only prove the result is
    *correct* (the rank argument guarantees that regardless of whether the
    heap fires).  This test directly verifies the pushdown happened.
    """
    table = "t_pr"
    _create_replicated_shards(table)
    query = (
        "EXPLAIN distributed=1, actions=1 "
        f"SELECT k, sum(v) FROM {table} GROUP BY k ORDER BY k ASC LIMIT 10"
    )
    settings_base = {
        "enable_parallel_replicas": 2,
        "max_parallel_replicas": 2,
        "cluster_for_parallel_replicas": "one_shard_two_replicas",
        "serialize_query_plan": 1,
    }
    plan_on = node1.query(
        query, settings=dict(settings_base, enable_group_by_top_k_optimization=1)
    )
    plan_off = node1.query(
        query, settings=dict(settings_base, enable_group_by_top_k_optimization=0)
    )
    assert "Top-K:" in plan_on, (
        f"Expected Top-K hint in the remote partial AggregatingStep when "
        f"the optimization is enabled.\nFull plan:\n{plan_on}"
    )
    assert "Top-K:" not in plan_off, (
        f"Did not expect Top-K hint when the optimization is disabled.\n"
        f"Full plan:\n{plan_off}"
    )


@pytest.mark.parametrize("max_parallel_replicas", [2])
def test_parallel_replicas_pattern1_serialize_query_plan(
    start_cluster, max_parallel_replicas
):
    """End-to-end test of the heap pushdown into the partial AggregatingStep
    via `serialize_query_plan = 1`.  When that mode is on, the initiator
    builds the remote sub-plan locally and ships it to the replica; this is
    the path where the new `tryPushDownTopKToPartialAggregation` helper in
    `createRemotePlanForParallelReplicas` is allowed to mutate the partial
    `AggregatingStep` before serialization.

    The assertion still only requires that the result matches the
    optimization-off baseline - the rank argument means the heap pushdown
    must not change observable output."""
    table = "t_pr"
    _create_replicated_shards(table)
    query = f"SELECT k, sum(v) FROM {table} GROUP BY k ORDER BY k ASC LIMIT 10"
    settings_base = {
        "enable_parallel_replicas": 2,
        "max_parallel_replicas": max_parallel_replicas,
        "cluster_for_parallel_replicas": "one_shard_two_replicas",
        "serialize_query_plan": 1,
    }
    off = node1.query(
        query, settings=dict(settings_base, enable_group_by_top_k_optimization=0)
    )
    on = node1.query(
        query, settings=dict(settings_base, enable_group_by_top_k_optimization=1)
    )
    assert off == on, (
        f"serialize_query_plan + enable_group_by_top_k_optimization diverged.\n"
        f"  off:\n{off}\n  on:\n{on}\n"
    )
    expected = "\n".join(
        f"{k}\t{100 if k != 5 else 101}" for k in range(10)
    ) + "\n"
    assert off == expected


@pytest.mark.parametrize("max_parallel_replicas", [2])
def test_parallel_replicas_pattern2(start_cluster, max_parallel_replicas):
    """Parallel replicas, no ORDER BY (Pattern 2).  Compared via a stable
    outer aggregation as in `test_distributed_remote_pattern2_no_order_by`."""
    table = "t_pr"
    _create_replicated_shards(table)
    inner = f"SELECT k, sum(v) AS s FROM {table} GROUP BY k LIMIT 100"
    outer = f"SELECT sum(k), sum(s), count() FROM ({inner})"
    settings_off = {
        "enable_group_by_top_k_optimization": 0,
        "enable_parallel_replicas": 2,
        "max_parallel_replicas": max_parallel_replicas,
        "cluster_for_parallel_replicas": "one_shard_two_replicas",
    }
    settings_on = dict(settings_off, enable_group_by_top_k_optimization=1)
    off = node1.query(outer, settings=settings_off)
    on = node1.query(outer, settings=settings_on)
    assert off == on, (
        f"enable_group_by_top_k_optimization changed the parallel-replicas result.\n"
        f"  off:\n{off}\n  on:\n{on}\n"
    )
