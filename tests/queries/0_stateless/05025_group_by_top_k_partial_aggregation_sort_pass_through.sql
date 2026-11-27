-- The shard-side top-K pushdown in `applyTopKPushdownToPartialAggregation` matches the ORDER BY against the
-- GROUP BY keys by action name.  A name match alone does not prove that the projection and the ORDER BY
-- expressions hand the key through to the coordinator's sort unchanged, so the pushdown additionally requires
-- `isSortKeyPassThrough` on both DAGs - the same invariant `tryOptimizeGroupByTopK` enforces on the plan.
-- Without it, a shape where the coordinator orders by a rewritten value could let the shards prune by the wrong
-- ranking and drop the real winners.
SET prefer_localhost_replica = 0;  -- both shards go over TCP and appear as secondary queries in query_log
SET max_rows_to_group_by = 0;
SET optimize_trivial_group_by_limit_query = 0;
-- CI randomizes query_plan_max_limit_for_top_k_optimization (can be tiny); pin it.
SET query_plan_max_limit_for_top_k_optimization = 1000;
-- With serialized plans the initiator's plan is shipped instead of the query text and top-K is (deliberately)
-- not serialized; pin the text path, which is the one this test covers.
SET serialize_query_plan = 0;
-- The partial pushdown is derived in the analyzer's Planner.
SET enable_analyzer = 1;

SELECT 'alias shadowing the grouped key';
SELECT -k AS k, count() FROM remote('127.0.0.1,localhost', view(
    SELECT number % 1000 AS k FROM numbers(100000)
)) GROUP BY k ORDER BY k ASC LIMIT 5
SETTINGS enable_group_by_top_k_optimization = 1;

SELECT -k AS k, count() FROM remote('127.0.0.1,localhost', view(
    SELECT number % 1000 AS k FROM numbers(100000)
)) GROUP BY k ORDER BY k ASC LIMIT 5
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'ORDER BY position over a rewritten projection column';
SELECT -k AS k, count() FROM remote('127.0.0.1,localhost', view(
    SELECT number % 1000 AS k FROM numbers(100000)
)) GROUP BY k ORDER BY 1 ASC LIMIT 5
SETTINGS enable_group_by_top_k_optimization = 1;

SELECT -k AS k, count() FROM remote('127.0.0.1,localhost', view(
    SELECT number % 1000 AS k FROM numbers(100000)
)) GROUP BY k ORDER BY 1 ASC LIMIT 5
SETTINGS enable_group_by_top_k_optimization = 0;

-- The projection rewrites the key into a different output column, but ORDER BY still ranks the key itself, so
-- the key does reach the sort unchanged and the pushdown must stay enabled.
SELECT 'rewritten projection column, ORDER BY on the key itself';
SELECT -k AS x, count() FROM remote('127.0.0.1,localhost', view(
    SELECT number % 1000 AS k FROM numbers(100000)
)) GROUP BY k ORDER BY k ASC LIMIT 5
SETTINGS enable_group_by_top_k_optimization = 1, log_comment = '05025_pass_through_on';

SELECT -k AS x, count() FROM remote('127.0.0.1,localhost', view(
    SELECT number % 1000 AS k FROM numbers(100000)
)) GROUP BY k ORDER BY k ASC LIMIT 5
SETTINGS enable_group_by_top_k_optimization = 0;

SYSTEM FLUSH LOGS query_log;

-- The pass-through shape must still push the heap down to the shards: the guard rejects rewritten sort keys, not
-- every projection.
SELECT 'shards still prune when the key reaches the sort unchanged';
SELECT sum(ProfileEvents['AggregationTopKRowsSkipped']) > 0
FROM system.query_log
WHERE type = 'QueryFinish'
    AND event_date >= yesterday()
    AND NOT is_initial_query
    AND initial_query_id IN
    (
        SELECT query_id FROM system.query_log
        WHERE current_database = currentDatabase()
            AND log_comment = '05025_pass_through_on'
            AND is_initial_query
            AND type = 'QueryFinish'
            AND event_date >= yesterday()
    );
