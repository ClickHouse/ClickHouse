-- Tags: no-random-settings, no-random-merge-tree-settings

-- Regression test for the read-in-order per-part `PrefetchingConcat` safeguard flags
-- surviving the lazy-FINAL replacement.
--
-- `optimizeLazyFinal` reconstructs the `ReadFromMergeTree` step from `getQueryInfo()`.
-- `query_info` carries `input_order_info` (so the reconstructed step still reads in order),
-- but the safeguard members `has_outer_limit` and `prefer_multiple_streams` are set by
-- `requestReadingInOrder`/`setPreferMultipleStreams` and are NOT part of `query_info`.
-- Without propagating them into the rebuilt step, an aggregation-in-order FINAL query would
-- silently collapse its parallel per-part streams into a single stream via per-part
-- `PrefetchingConcat` inside the lazy-FINAL branch (the very thing `setPreferMultipleStreams`
-- exists to prevent). See `ReadFromMergeTree::copyReadInOrderContractFrom`.
--
-- Under read-in-order the lazy-FINAL *partial* split (the `InputSelector` shape) is disabled:
-- the set-building replacement does not preserve the reading order and defeats early exit.
-- Only the *full* non-intersecting replacement still fires for in-order reads, so that is the
-- reconstruction site exercised here: all parts must be disjoint by primary key.

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

-- Three disjoint-key parts: all non-intersecting, so the full replacement plan fires and
-- rebuilds the reading step (the one that must carry the safeguard flags) without the FINAL merge.
INSERT INTO t_lazy_final_prefetch SELECT number, 1, number FROM numbers(0, 20000);
INSERT INTO t_lazy_final_prefetch SELECT number, 1, number FROM numbers(20000, 20000);
INSERT INTO t_lazy_final_prefetch SELECT number, 1, number FROM numbers(40000, 20000);

-- Baseline: without lazy FINAL the pipeline contains the `ReplacingSorted` FINAL merge,
-- so its absence below is a non-vacuous signal that the replacement actually fired.
SELECT 'replacing_sorted_without_lazy_final';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE
    SELECT key, count() FROM t_lazy_final_prefetch FINAL WHERE value > 0 GROUP BY key ORDER BY key
    SETTINGS enable_parallel_replicas = 0, max_threads = 6,
             optimize_read_in_order = 1, optimize_aggregation_in_order = 1,
             query_plan_optimize_lazy_final = 0
) WHERE explain LIKE '%ReplacingSorted%';

-- Non-vacuous anchor: the lazy-FINAL full replacement fires for this aggregation-in-order
-- FINAL query — the FINAL merge disappears and the reconstructed reading step is exercised.
SELECT 'lazy_final_full_replacement_engaged';
SELECT count() = 0 FROM (
    EXPLAIN PIPELINE
    SELECT key, count() FROM t_lazy_final_prefetch FINAL WHERE value > 0 GROUP BY key ORDER BY key
    SETTINGS enable_parallel_replicas = 0, max_threads = 6,
             optimize_read_in_order = 1, optimize_aggregation_in_order = 1,
             query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000, min_filtered_ratio_for_lazy_final = 0
) WHERE explain LIKE '%ReplacingSorted%';

-- The reconstructed reading step still reads in order (`input_order_info` is carried over).
SELECT 'read_in_order_preserved';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE
    SELECT key, count() FROM t_lazy_final_prefetch FINAL WHERE value > 0 GROUP BY key ORDER BY key
    SETTINGS enable_parallel_replicas = 0, max_threads = 6,
             optimize_read_in_order = 1, optimize_aggregation_in_order = 1,
             query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000, min_filtered_ratio_for_lazy_final = 0
) WHERE explain LIKE '%algorithm: InOrder%';

-- The safeguard survives: `prefer_multiple_streams` (set by aggregation-in-order) is propagated
-- into the rebuilt step, so per-part `PrefetchingConcat` — which would collapse the parallel
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

-- Correctness on the full-replacement shape: the lazy-FINAL aggregation-in-order result must be
-- identical to the plain FINAL result (both directions of EXCEPT must be empty).
SELECT 'correctness_full_replacement';
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

-- Now add an overlapping-key part: parts become intersecting, and under read-in-order the
-- partial split is disabled — the plan must keep the regular FINAL read with no `InputSelector`
-- (the set-building phase would defeat the reading order and early exit).
INSERT INTO t_lazy_final_prefetch SELECT number, 2, number * 10 FROM numbers(40000, 20000);

SELECT 'no_partial_split_under_read_in_order';
SELECT count() = 0 FROM (
    EXPLAIN actions = 0
    SELECT key, count() FROM t_lazy_final_prefetch FINAL WHERE value > 0 GROUP BY key ORDER BY key
    SETTINGS enable_parallel_replicas = 0, max_threads = 6,
             optimize_read_in_order = 1, optimize_aggregation_in_order = 1,
             query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000, min_filtered_ratio_for_lazy_final = 0
) WHERE explain LIKE '%InputSelector%';

-- Correctness with intersecting parts (regular FINAL kept under lazy ON): identical results.
SELECT 'correctness_intersecting';
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
