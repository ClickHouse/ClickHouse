-- Tags: no-random-settings, no-random-merge-tree-settings

-- Regression test for the read-in-order per-part `PrefetchingConcat` safeguard flags
-- surviving the lazy-FINAL split.
--
-- `optimizeLazyFinal` reconstructs the `ReadFromMergeTree` step from `getQueryInfo()`.
-- `query_info` carries `input_order_info` (so the reconstructed step still reads in order),
-- but the safeguard members `has_outer_limit` and `prefer_multiple_streams` are set by
-- `requestReadingInOrder`/`setPreferMultipleStreams` and are NOT part of `query_info`.
-- Without propagating them into the rebuilt step, an aggregation-in-order FINAL query would
-- silently collapse its parallel per-part streams into a single stream via per-part
-- `PrefetchingConcat` inside the lazy-FINAL branch (the very thing `setPreferMultipleStreams`
-- exists to prevent). See `ReadFromMergeTree::copyReadInOrderContractFrom`.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_lazy_final_prefetch;

CREATE TABLE t_lazy_final_prefetch
(
    key UInt64,
    version UInt64,
    value UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY key
SETTINGS index_granularity = 1024;

SYSTEM STOP MERGES t_lazy_final_prefetch;

-- Two disjoint-key parts (non-intersecting: they feed the reconstructed non-FINAL read,
-- which is where read-in-order + per-part prefetching happen) and two overlapping-key parts
-- (intersecting: they still need the FINAL merge, so the lazy-FINAL split keeps an
-- `InputSelector` in the plan — the non-vacuous anchor below).
INSERT INTO t_lazy_final_prefetch SELECT number, 1, number FROM numbers(0, 20000);
INSERT INTO t_lazy_final_prefetch SELECT number, 1, number FROM numbers(20000, 20000);
INSERT INTO t_lazy_final_prefetch SELECT number, 1, number FROM numbers(40000, 20000);
INSERT INTO t_lazy_final_prefetch SELECT number, 2, number * 10 FROM numbers(40000, 20000);

-- Non-vacuous anchor: the lazy-FINAL optimization actually fires for this aggregation-in-order
-- FINAL query, so the reconstructed reading step (the one that must carry the flags) is exercised.
SELECT 'lazy_final_engaged';
SELECT count() > 0 FROM (
    EXPLAIN actions = 0
    SELECT key, count() FROM t_lazy_final_prefetch FINAL WHERE value > 0 GROUP BY key ORDER BY key
    SETTINGS enable_parallel_replicas = 0, max_threads = 6,
             optimize_read_in_order = 1, optimize_aggregation_in_order = 1,
             query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000, min_filtered_ratio_for_lazy_final = 0
) WHERE explain LIKE '%InputSelector%';

-- The safeguard survives: `prefer_multiple_streams` (set by aggregation-in-order) is propagated
-- into the lazy-FINAL branch, so per-part `PrefetchingConcat` — which would collapse the parallel
-- streams the aggregation wants — must NOT appear. Without the flag propagation this prints
-- `PrefetchingConcat` lines. Absence is robust across environments; presence would be
-- stream-count dependent, so we only assert absence.
SELECT 'no_prefetching_lazy_final_aggregation_in_order';
SELECT * FROM (
    EXPLAIN PIPELINE
    SELECT key, count() FROM t_lazy_final_prefetch FINAL WHERE value > 0 GROUP BY key ORDER BY key
    SETTINGS enable_parallel_replicas = 0, max_threads = 6,
             optimize_read_in_order = 1, optimize_aggregation_in_order = 1,
             query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000, min_filtered_ratio_for_lazy_final = 0
) WHERE explain LIKE '%PrefetchingConcat%';

-- Correctness: the lazy-FINAL aggregation-in-order result must be identical to the plain FINAL
-- result (both directions of EXCEPT must be empty). Guards against the split dropping/duplicating
-- rows regardless of the streaming shape.
SELECT 'correctness';
SELECT count() FROM (
    (
        SELECT key, count() AS c, sum(value) AS s FROM t_lazy_final_prefetch FINAL WHERE value > 0 GROUP BY key
        SETTINGS query_plan_optimize_lazy_final = 0
    )
    EXCEPT
    (
        SELECT key, count() AS c, sum(value) AS s FROM t_lazy_final_prefetch FINAL WHERE value > 0 GROUP BY key
        SETTINGS optimize_read_in_order = 1, optimize_aggregation_in_order = 1,
                 query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000, min_filtered_ratio_for_lazy_final = 0
    )
);
SELECT count() FROM (
    (
        SELECT key, count() AS c, sum(value) AS s FROM t_lazy_final_prefetch FINAL WHERE value > 0 GROUP BY key
        SETTINGS optimize_read_in_order = 1, optimize_aggregation_in_order = 1,
                 query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000, min_filtered_ratio_for_lazy_final = 0
    )
    EXCEPT
    (
        SELECT key, count() AS c, sum(value) AS s FROM t_lazy_final_prefetch FINAL WHERE value > 0 GROUP BY key
        SETTINGS query_plan_optimize_lazy_final = 0
    )
);

-- A few concrete rows to pin the FINAL result (version 2 wins on the overlapping key range 40000..59999).
SELECT 'sample_rows';
SELECT key, value FROM t_lazy_final_prefetch FINAL WHERE value > 0 AND key IN (10000, 30000, 50000) ORDER BY key
SETTINGS optimize_read_in_order = 1, optimize_aggregation_in_order = 1,
         query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000, min_filtered_ratio_for_lazy_final = 0;

DROP TABLE t_lazy_final_prefetch;
