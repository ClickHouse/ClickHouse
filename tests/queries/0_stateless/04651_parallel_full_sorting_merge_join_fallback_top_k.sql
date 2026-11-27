-- Regression coverage for the second `join_algorithm` priority-list side effect of
-- `parallel_full_sorting_merge` (the first one - strict join-key inference - is pinned by
-- `04602_parallel_full_sorting_merge_join_fallback_strict_keys`).
--
-- `joinDefeatsReadInOrderThroughJoin` runs on `JoinStepLogical`, before `chooseJoinAlgorithm`
-- has picked anything, so it checks the *configured list* rather than the selected algorithm.
-- Listing a merge join as a lower-priority fallback therefore disables the
-- `topKThroughJoin` -> `optimizeReadInOrder` deferral even when the join that actually runs is
-- plain `hash`: the query gets `topKThroughJoin`'s own `Sort + Limit` on the preserved side
-- instead of streaming it in primary-key order. That is a plan-shape pessimization, not a wrong
-- result, and it is exactly what the pre-existing `hash,full_sorting_merge` configuration does.
--
-- The invariant pinned here: `hash,parallel_full_sorting_merge` behaves EXACTLY like
-- `hash,full_sorting_merge` (both inject the extra `Sort + Limit`), so adding the new algorithm
-- as a fallback introduces no divergence relative to master, while plain `hash` still defers.

SET enable_analyzer = 1;
SET query_plan_top_k_through_join = 1;

DROP TABLE IF EXISTS pfsmj_topk_left;
DROP TABLE IF EXISTS pfsmj_topk_right;

-- The sort key (`k`) is the primary key of the preserved (left) table, so `optimizeReadInOrder`
-- can stream the rows in order - if the deferral fires.
CREATE TABLE pfsmj_topk_left (k Int64, payload String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE pfsmj_topk_right (k Int64, value String) ENGINE = MergeTree ORDER BY k;

INSERT INTO pfsmj_topk_left SELECT number, repeat('a', 8) FROM numbers(1000);
INSERT INTO pfsmj_topk_right SELECT number, repeat('b', 8) FROM numbers(1000);

-- `optimize_read_in_order`, `query_plan_read_in_order` and `enable_parallel_replicas` are
-- randomized by the test runner and all three change the deferral outcome, so pin them.
-- `max_bytes_*_before_external_join = 0` keeps automatic spilling off: `SpillingHashJoin`
-- reports delayed blocks, which blocks the deferral for every algorithm and would mask the
-- effect under test.

-- Plain `hash`: nothing on the list defeats read-in-order, so `topKThroughJoin` defers and
-- injects no extra `Sort + Limit` - only the outer pair remains.
SELECT 'hash' AS label, countIf(explain LIKE '%Sorting%') AS sort_count, countIf(explain LIKE '%Limit%') AS limit_count
FROM ( EXPLAIN actions = 0
    SELECT l.k, r.value FROM pfsmj_topk_left AS l LEFT JOIN pfsmj_topk_right AS r ON r.k = l.k
    ORDER BY l.k DESC LIMIT 10
    SETTINGS optimize_read_in_order = 1,
             query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
             query_plan_join_swap_table = false, query_plan_max_limit_for_top_k_optimization = 0,
             enable_join_runtime_filters = 0, enable_lazy_columns_replication = 0,
             query_plan_optimize_lazy_materialization = 0,
             enable_parallel_replicas = 0,
             join_algorithm = 'hash',
             max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
);

-- `hash,full_sorting_merge` (pre-existing behavior on master): the list contains a merge join,
-- so the deferral is skipped and `topKThroughJoin` adds its own `Sort + Limit`.
SELECT 'hash_fsm' AS label, countIf(explain LIKE '%Sorting%') AS sort_count, countIf(explain LIKE '%Limit%') AS limit_count
FROM ( EXPLAIN actions = 0
    SELECT l.k, r.value FROM pfsmj_topk_left AS l LEFT JOIN pfsmj_topk_right AS r ON r.k = l.k
    ORDER BY l.k DESC LIMIT 10
    SETTINGS optimize_read_in_order = 1,
             query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
             query_plan_join_swap_table = false, query_plan_max_limit_for_top_k_optimization = 0,
             enable_join_runtime_filters = 0, enable_lazy_columns_replication = 0,
             query_plan_optimize_lazy_materialization = 0,
             enable_parallel_replicas = 0,
             join_algorithm = 'hash,full_sorting_merge',
             max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
);

-- `hash,parallel_full_sorting_merge`: must match `hash_fsm` exactly.
SELECT 'hash_pfsm' AS label, countIf(explain LIKE '%Sorting%') AS sort_count, countIf(explain LIKE '%Limit%') AS limit_count
FROM ( EXPLAIN actions = 0
    SELECT l.k, r.value FROM pfsmj_topk_left AS l LEFT JOIN pfsmj_topk_right AS r ON r.k = l.k
    ORDER BY l.k DESC LIMIT 10
    SETTINGS optimize_read_in_order = 1,
             query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
             query_plan_join_swap_table = false, query_plan_max_limit_for_top_k_optimization = 0,
             enable_join_runtime_filters = 0, enable_lazy_columns_replication = 0,
             query_plan_optimize_lazy_materialization = 0,
             enable_parallel_replicas = 0,
             join_algorithm = 'hash,parallel_full_sorting_merge',
             max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
);

-- The pessimization is not unique to the full-sorting variants: `partial_merge` re-sorts the
-- left blocks, so listing it defeats the deferral in exactly the same way. Pinned here so the
-- `join_algorithm` documentation of the effect stays honest about its scope.
SELECT 'hash_pm' AS label, countIf(explain LIKE '%Sorting%') AS sort_count, countIf(explain LIKE '%Limit%') AS limit_count
FROM ( EXPLAIN actions = 0
    SELECT l.k, r.value FROM pfsmj_topk_left AS l LEFT JOIN pfsmj_topk_right AS r ON r.k = l.k
    ORDER BY l.k DESC LIMIT 10
    SETTINGS optimize_read_in_order = 1,
             query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
             query_plan_join_swap_table = false, query_plan_max_limit_for_top_k_optimization = 0,
             enable_join_runtime_filters = 0, enable_lazy_columns_replication = 0,
             query_plan_optimize_lazy_materialization = 0,
             enable_parallel_replicas = 0,
             join_algorithm = 'hash,partial_merge',
             max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
);

-- The plan shape differs, the results must not.
SELECT 'result_hash' AS label, count(), max(k), min(k) FROM (
    SELECT l.k AS k, r.value FROM pfsmj_topk_left AS l LEFT JOIN pfsmj_topk_right AS r ON r.k = l.k
    ORDER BY l.k DESC LIMIT 10
    SETTINGS join_algorithm = 'hash', enable_parallel_replicas = 0
);

SELECT 'result_hash_fsm' AS label, count(), max(k), min(k) FROM (
    SELECT l.k AS k, r.value FROM pfsmj_topk_left AS l LEFT JOIN pfsmj_topk_right AS r ON r.k = l.k
    ORDER BY l.k DESC LIMIT 10
    SETTINGS join_algorithm = 'hash,full_sorting_merge', enable_parallel_replicas = 0
);

SELECT 'result_hash_pfsm' AS label, count(), max(k), min(k) FROM (
    SELECT l.k AS k, r.value FROM pfsmj_topk_left AS l LEFT JOIN pfsmj_topk_right AS r ON r.k = l.k
    ORDER BY l.k DESC LIMIT 10
    SETTINGS join_algorithm = 'hash,parallel_full_sorting_merge', enable_parallel_replicas = 0
);

DROP TABLE pfsmj_topk_left;
DROP TABLE pfsmj_topk_right;
