"""
Correctness tests for `enable_group_by_top_k_optimization` under non-final
(partial) aggregation - distributed tables and parallel replicas.

Partial aggregation gets its top-K parameters from the Planner hook
`applyTopKPushdownToPartialAggregation`, which only applies when each node
plans the query text itself (not with `serialize_query_plan`) and only for
queries with a real ORDER BY over a leading prefix of the GROUP BY keys:

  * with ORDER BY (`GROUP BY ... ORDER BY <prefix> LIMIT N`) - safe on the
    partial side: a key rejected by a shard-local heap cannot be in the
    global top-N, and the initiator's final sort + LIMIT discards any group
    a remote eviction left incomplete.
  * without ORDER BY (`GROUP BY ... LIMIT N`) - stays final-only: the
    synthesized sort that makes this shape safe cannot be placed above the
    initiator's merge, so the pushdown must never fire on followers.

The data is deliberately skewed so a heap would actually filter rows:
shard 1 has many distinct small keys (fills an N=10 heap on its own),
shard 2 has a few large-key rows whose aggregates must remain complete to
produce the correct global answer.

The result with the optimization enabled must always equal the result with
it disabled; the EXPLAIN tests additionally pin where the `Top-K`
annotation may and may not appear, and the profile-event test proves the
follower-side heap actually runs on the text-planned path.
"""

import json

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
    """Create a replicated table on both nodes for the parallel-replicas tests."""
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


def test_distributed_remote_order_by_prefix(start_cluster):
    """`GROUP BY k ORDER BY k LIMIT N` over a Distributed table.

    The shards plan the query text themselves, so the partial-aggregation
    pushdown applies there: a key filtered by a shard's local heap cannot be
    in the global top-N.  This is the shape the pushdown targets, so it gets
    the strongest assertion (exact expected result).
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


def test_sharded_distributed_order_by_with_push_down_limit(start_cluster):
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


def test_distributed_remote_order_by_desc(start_cluster):
    """`GROUP BY k ORDER BY k DESC LIMIT N` - the DESC direction."""
    _make_local_shards()
    query = (
        "SELECT k, sum(v) "
        "FROM remote('node{1,2}', currentDatabase(), t_local) "
        "GROUP BY k "
        "ORDER BY k DESC "
        "LIMIT 5"
    )
    _assert_same_result(node1, query)


def test_distributed_remote_composite_prefix(start_cluster):
    """`GROUP BY (k, v) ORDER BY k LIMIT N` - prefix mode."""
    _make_local_shards()
    query = (
        "SELECT k, v, count() "
        "FROM remote('node{1,2}', currentDatabase(), t_local) "
        "GROUP BY k, v "
        "ORDER BY k ASC "
        "LIMIT 10"
    )
    _assert_same_result(node1, query)


def test_distributed_remote_no_order_by(start_cluster):
    """`GROUP BY k LIMIT N` (no ORDER BY) over a Distributed table.

    The no-ORDER-BY shape is the one the gate definitively protects: there
    is no ORDER BY at the coordinator to evict tuples with corrupted partial
    state, so if the gate were ever wrongly relaxed for partial aggregation
    this would diverge.

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


def test_distributed_remote_array_join_projection(start_cluster):
    """`arrayJoin` in the projection changes row multiplicity after the
    aggregation - `range(k % 2)` expands even keys to zero rows - so the
    partial-aggregation pushdown must not fire.  If it did, shard 1's heap
    would keep keys {0..4}, the initiator's `arrayJoin` would annihilate the
    even ones, and the LIMIT would come up short of the true answer (the
    five smallest odd keys).
    """
    _make_local_shards()
    query = (
        "SELECT k, arrayJoin(range(k % 2)) AS a "
        "FROM remote('node{1,2}', currentDatabase(), t_local) "
        "GROUP BY k "
        "ORDER BY k ASC "
        "LIMIT 5"
    )
    _assert_same_result(node1, query)
    # Ground truth: the five smallest odd keys, each expanded to a single
    # zero by range(1).
    expected = "\n".join(f"{k}\t0" for k in (1, 3, 5, 7, 9)) + "\n"
    assert _run(node1, query, 1) == expected


# ---------------------------------------------------------------------------
# Parallel replicas (one shard, two replicas)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("max_parallel_replicas", [2])
def test_parallel_replicas_order_by(start_cluster, max_parallel_replicas):
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


def test_remote_partial_aggregation_top_k(start_cluster):
    """Partial aggregation derives the top-K parameters from the analyzed
    query in the Planner, and that only reaches the followers when each node
    plans the query text itself.  With `serialize_query_plan` the initiator's
    serialized sub-plan is shipped instead and `AggregatingStep::serialize`
    deliberately does not carry top-K (the plan-serialization protocol has no
    version negotiation), so the pushdown is gated off entirely there - the
    plan must not advertise a `Top-K` the followers would never run."""
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
        "query_plan_max_limit_for_top_k_optimization": 1000,
    }
    for serialize in (0, 1):
        for opt in (0, 1):
            plan = node1.query(
                query,
                settings=dict(
                    settings_base,
                    serialize_query_plan=serialize,
                    enable_group_by_top_k_optimization=opt,
                ),
            )
            if opt and not serialize:
                assert "Top-K:" in plan, (
                    f"serialize_query_plan={serialize}, opt={opt}\nFull plan:\n{plan}"
                )
            else:
                assert "Top-K:" not in plan, (
                    f"serialize_query_plan={serialize}, opt={opt}\nFull plan:\n{plan}"
                )


def test_remote_partial_aggregation_follower_heap_engaged(start_cluster):
    """Follower-side proof that the text-planned partial aggregation actually
    runs the heap: the follower replicas must report `AggregationTopKRowsSkipped`
    for the initiator's query.  Result equality alone cannot distinguish a
    working remote heap from one silently dropped on the way to the follower.

    `parallel_replicas_local_plan = 0` makes every replica (including the
    initiator's own) a text-planned follower, so the secondary queries carry
    all the partial-aggregation work regardless of how it is scheduled."""
    table = "t_pr"
    _create_replicated_shards(table)
    comment = "topk_follower_heap_proof"
    query = f"SELECT k, sum(v) FROM {table} GROUP BY k ORDER BY k ASC LIMIT 10"
    node1.query(
        query,
        settings={
            "enable_group_by_top_k_optimization": 1,
            "enable_parallel_replicas": 2,
            "max_parallel_replicas": 2,
            "cluster_for_parallel_replicas": "one_shard_two_replicas",
            "serialize_query_plan": 0,
            "parallel_replicas_local_plan": 0,
            "query_plan_max_limit_for_top_k_optimization": 1000,
            "log_comment": comment,
        },
    )
    for node in (node1, node2):
        node.query("SYSTEM FLUSH LOGS query_log")
    initial_query_id = node1.query(
        f"SELECT query_id FROM system.query_log "
        f"WHERE log_comment = '{comment}' AND is_initial_query AND type = 'QueryFinish' "
        f"ORDER BY event_time_microseconds DESC LIMIT 1"
    ).strip()
    assert initial_query_id, "initial query not found in query_log"
    skipped = 0
    for node in (node1, node2):
        skipped += int(
            node.query(
                f"SELECT sum(ProfileEvents['AggregationTopKRowsSkipped']) "
                f"FROM system.query_log "
                f"WHERE initial_query_id = '{initial_query_id}' "
                f"AND NOT is_initial_query AND type = 'QueryFinish'"
            ).strip()
        )
    assert skipped > 0, "no follower reported top-K skipped rows"


@pytest.mark.parametrize("max_parallel_replicas", [2])
def test_parallel_replicas_order_by_serialize_query_plan(
    start_cluster, max_parallel_replicas
):
    """With `serialize_query_plan = 1` the initiator ships a serialized
    sub-plan and the partial top-K pushdown is gated off in
    `applyTopKPushdownToPartialAggregation` (top-K is not serialized).  The
    query must still work and match the optimization-off baseline - i.e. the
    gate degrades to plain partial aggregation, nothing more."""
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


def test_remote_partial_aggregation_no_top_k_with_having(start_cluster):
    """`HAVING` is applied on the coordinator over the full set of merged
    groups, so the follower must not prune partial aggregation states: a
    replica could keep only the heap's top key, the coordinator would filter
    it out by `HAVING`, and the real answer would already be gone.

    Data shape: keys 0..999 have sum(v) >= 100, keys 10000..10009 have
    sum(v) = 1.  `HAVING s = 1 ORDER BY k LIMIT 1` must return key 10000.
    A per-replica top-1 heap by `k ASC` would keep only key 0, which the
    coordinator then discards - returning an empty (wrong) result.
    """
    table = "t_pr"
    _create_replicated_shards(table)
    query = (
        f"SELECT k, sum(v) AS s FROM {table} GROUP BY k "
        "HAVING s = 1 ORDER BY k ASC LIMIT 1"
    )
    settings_base = {
        "enable_parallel_replicas": 2,
        "max_parallel_replicas": 2,
        "cluster_for_parallel_replicas": "one_shard_two_replicas",
        "serialize_query_plan": 1,
    }
    off = node1.query(
        query, settings=dict(settings_base, enable_group_by_top_k_optimization=0)
    )
    on = node1.query(
        query, settings=dict(settings_base, enable_group_by_top_k_optimization=1)
    )
    assert off == on == "10000\t1\n", (
        f"HAVING must disable the partial top-K pushdown.\n"
        f"  off:\n{off}\n  on:\n{on}\n"
    )

    plan = node1.query(
        f"EXPLAIN distributed=1, actions=1 {query}",
        settings=dict(settings_base, enable_group_by_top_k_optimization=1),
    )
    assert "Top-K:" not in plan, (
        f"Top-K must not be pushed into partial aggregation under HAVING.\n"
        f"Full plan:\n{plan}"
    )


def test_remote_partial_aggregation_no_top_k_with_post_aggregation_clauses(
    start_cluster,
):
    """QUALIFY, window functions (inline and via a named WINDOW clause) and
    DISTINCT all consume the full set of groups on the coordinator, so the
    partial-aggregation pushdown (`applyTopKPushdownToPartialAggregation`)
    must skip them - the plan-shape optimizer never matches these cases, and
    the Planner hook must not be more permissive."""
    table = "t_pr"
    _create_replicated_shards(table)
    queries = [
        # QUALIFY (with an inline window function)
        f"SELECT k, sum(v) AS s, row_number() OVER (ORDER BY k ASC) AS rn "
        f"FROM {table} GROUP BY k QUALIFY rn <= 3 ORDER BY k ASC LIMIT 10",
        # Inline window function without QUALIFY
        f"SELECT k, sum(sum(v)) OVER () AS total "
        f"FROM {table} GROUP BY k ORDER BY k ASC LIMIT 10",
        # Named WINDOW clause
        f"SELECT k, count() OVER w FROM {table} GROUP BY k "
        f"WINDOW w AS (ORDER BY k ASC) ORDER BY k ASC LIMIT 10",
        # DISTINCT between aggregation and ORDER BY / LIMIT
        f"SELECT DISTINCT k, sum(v) FROM {table} GROUP BY k "
        f"ORDER BY k ASC LIMIT 10",
    ]
    settings = {
        "enable_parallel_replicas": 2,
        "max_parallel_replicas": 2,
        "cluster_for_parallel_replicas": "one_shard_two_replicas",
        "serialize_query_plan": 1,
        "enable_group_by_top_k_optimization": 1,
    }
    for query in queries:
        plan = node1.query(f"EXPLAIN distributed=1, actions=1 {query}", settings=settings)
        assert "Top-K:" not in plan, (
            f"Top-K must not be pushed into partial aggregation.\n"
            f"  query: {query}\nFull plan:\n{plan}"
        )


def test_remote_partial_aggregation_no_top_k_with_exact_rows_before_limit(
    start_cluster,
):
    """`exact_rows_before_limit = 1` promises an exact
    `rows_before_limit_at_least` counter, which requires counting every
    group.  The follower must not prune partial aggregation in that mode."""
    table = "t_pr"
    _create_replicated_shards(table)
    query = f"SELECT k, sum(v) FROM {table} GROUP BY k ORDER BY k ASC LIMIT 10"
    settings = {
        "enable_parallel_replicas": 2,
        "max_parallel_replicas": 2,
        "cluster_for_parallel_replicas": "one_shard_two_replicas",
        "serialize_query_plan": 1,
        "enable_group_by_top_k_optimization": 1,
        "exact_rows_before_limit": 1,
    }
    plan = node1.query(f"EXPLAIN distributed=1, actions=1 {query}", settings=settings)
    assert "Top-K:" not in plan, (
        f"Top-K must not be pushed down with exact_rows_before_limit.\n"
        f"Full plan:\n{plan}"
    )

    # The dataset has 1010 distinct keys: 0..999 and 10000..10009.
    result = node1.query(f"{query} FORMAT JSON", settings=settings)
    rows_before_limit = json.loads(result)["rows_before_limit_at_least"]
    assert rows_before_limit == 1010, (
        f"exact_rows_before_limit must count all groups, got {rows_before_limit}"
    )


@pytest.mark.parametrize("max_parallel_replicas", [2])
def test_parallel_replicas_no_order_by(start_cluster, max_parallel_replicas):
    """Parallel replicas, no ORDER BY.  Compared via a stable
    outer aggregation as in `test_distributed_remote_no_order_by`."""
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
