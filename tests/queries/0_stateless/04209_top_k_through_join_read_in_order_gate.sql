-- Verify that `topKThroughJoin` does not silently disable itself when the user
-- has disabled `query_plan_read_in_order_through_join` (while keeping
-- `query_plan_read_in_order` on).
--
-- The optimization defers to `optimizeReadInOrder` only when the second-pass
-- through-join read-in-order optimization can actually apply. If the user has
-- disabled it, `topKThroughJoin` must still fire so the query gets at least
-- one of the two optimizations, instead of falling through both gates.

SET enable_analyzer = 1;
SET query_plan_top_k_through_join = 1;

DROP TABLE IF EXISTS t_l;
DROP TABLE IF EXISTS t_r;

-- Sort key (`k`) is the storage's primary key, so `optimizeReadInOrder` could
-- in principle stream the rows in order from MergeTree without an explicit
-- sort. The deferral check inside `topKThroughJoin` is what decides whether
-- to leave that to the second pass or insert the explicit `Sort + Limit n`.
CREATE TABLE t_l (k Int64, payload String) ENGINE = MergeTree() ORDER BY k;
CREATE TABLE t_r (k Int64, value String) ENGINE = MergeTree() ORDER BY k;

INSERT INTO t_l SELECT number, repeat('a', 8) FROM numbers(1000);
INSERT INTO t_r SELECT number, repeat('b', 8) FROM numbers(1000);

-- Both flags on: defer to `optimizeReadInOrder`, no explicit inner Sort + Limit.
-- The deferral inside `topKThroughJoin` keys on `optimize_read_in_order &&
-- query_plan_read_in_order`, both of which the stateless test runner randomizes;
-- pin them on so the deferral check is deterministic. `enable_parallel_replicas`
-- is randomized too and would replace `ReadFromMergeTree` with a coordinator
-- step that breaks the deferral's storage-step lookup.
-- `max_bytes_*_before_external_join = 0` pins automatic spilling off so the
-- deferral does not bail out on `SpillingHashJoin::hasDelayedBlocks()` (the
-- default `max_bytes_ratio_before_external_join = 0.5` wraps every hash join,
-- which `optimizeReadInOrder`'s join traversal rejects).
SELECT 'both_on' AS label, countIf(explain LIKE '%Sorting%') AS sort_count, countIf(explain LIKE '%Limit%') AS limit_count
FROM ( EXPLAIN actions = 0
    SELECT l.k, r.value FROM t_l AS l LEFT JOIN t_r AS r ON r.k = l.k
    ORDER BY l.k DESC LIMIT 10
    SETTINGS optimize_read_in_order = 1,
             query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
             query_plan_join_swap_table = false, query_plan_max_limit_for_top_k_optimization = 0,
             enable_join_runtime_filters = 0, enable_lazy_columns_replication = 0,
             query_plan_optimize_lazy_materialization = 0,
             enable_parallel_replicas = 0,
             max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
);

-- `read_in_order_through_join` off: the second pass cannot stream through the
-- join, so `topKThroughJoin` must fire and add a Sort + Limit on the preserved
-- input. Expect at least two of each (the outer pair + the injected pair).
SELECT 'through_join_off' AS label, countIf(explain LIKE '%Sorting%') AS sort_count, countIf(explain LIKE '%Limit%') AS limit_count
FROM ( EXPLAIN actions = 0
    SELECT l.k, r.value FROM t_l AS l LEFT JOIN t_r AS r ON r.k = l.k
    ORDER BY l.k DESC LIMIT 10
    SETTINGS optimize_read_in_order = 1,
             query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 0,
             query_plan_join_swap_table = false, query_plan_max_limit_for_top_k_optimization = 0,
             enable_join_runtime_filters = 0, enable_lazy_columns_replication = 0,
             query_plan_optimize_lazy_materialization = 0,
             enable_parallel_replicas = 0,
             max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
);

-- Spilling-on path: even with both flags on, automatic spilling can wrap the
-- chosen hash join in `SpillingHashJoin` (`hasDelayedBlocks=true`), which the
-- second-pass join traversal rejects. The deferral must therefore NOT fire -
-- otherwise both optimizations get silently disabled. `topKThroughJoin` is
-- expected to inject its own `Sort + Limit`, mirroring the
-- `through_join_off` case.
SELECT 'spilling_on' AS label, countIf(explain LIKE '%Sorting%') AS sort_count, countIf(explain LIKE '%Limit%') AS limit_count
FROM ( EXPLAIN actions = 0
    SELECT l.k, r.value FROM t_l AS l LEFT JOIN t_r AS r ON r.k = l.k
    ORDER BY l.k DESC LIMIT 10
    SETTINGS optimize_read_in_order = 1,
             query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
             query_plan_join_swap_table = false, query_plan_max_limit_for_top_k_optimization = 0,
             enable_join_runtime_filters = 0, enable_lazy_columns_replication = 0,
             query_plan_optimize_lazy_materialization = 0,
             enable_parallel_replicas = 0,
             max_bytes_ratio_before_external_join = 0.5
);

-- Result equivalence across flag combinations.
-- `enable_parallel_replicas = 0`: under parallel replicas, `ORDER BY l.k DESC`
-- combined with `t_l ORDER BY k` engages read-in-order (`ReverseOrder`), and
-- the coordinator may receive announcements with a different mode from the
-- replica step that wraps the local plan, raising
-- `Coordination mode mismatch for stream ...` from
-- `ParallelReplicasReadingCoordinator::getOrCreateCoordinator`. This test
-- verifies plan-level result equivalence, not distributed execution.
SELECT 'result_both_on' AS label, count(*), max(k), min(k) FROM (
    SELECT l.k AS k, r.value FROM t_l AS l LEFT JOIN t_r AS r ON r.k = l.k
    ORDER BY l.k DESC LIMIT 10
    SETTINGS query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
             enable_parallel_replicas = 0
);

SELECT 'result_through_join_off' AS label, count(*), max(k), min(k) FROM (
    SELECT l.k AS k, r.value FROM t_l AS l LEFT JOIN t_r AS r ON r.k = l.k
    ORDER BY l.k DESC LIMIT 10
    SETTINGS query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 0,
             enable_parallel_replicas = 0
);

DROP TABLE t_l;
DROP TABLE t_r;
