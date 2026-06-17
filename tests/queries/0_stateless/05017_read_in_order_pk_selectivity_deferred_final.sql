-- Test that the read-in-order PK-selectivity guard is NOT misfired by filters that are
-- deferred after FINAL (apply_row_policy_after_final / apply_prewhere_after_final).
--
-- Deferred filters are excluded from primary key / skip index analysis, so they never reduce
-- the number of selected granules. Treating such a deferred-only filter as a primary-key filter
-- would make the guard see an effective full scan (selected_marks_pk == total_marks_pk) and
-- wrongly disable read-in-order, replacing a low-memory streaming plan with parallel reading
-- plus a global MergeSortingTransform (a potential MEMORY_LIMIT_EXCEEDED regression).
-- `apply_row_policy_after_final` is enabled by default, so this case is reachable out of the box.

DROP TABLE IF EXISTS t_deferred_pk;
DROP ROW POLICY IF EXISTS rp_05017 ON t_deferred_pk;

-- Small index_granularity and several parts so there are many more marks than streams,
-- which is what makes the PK-selectivity guard eligible to fire.
CREATE TABLE t_deferred_pk (path String, value UInt64)
ENGINE = ReplacingMergeTree ORDER BY path
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES t_deferred_pk;

INSERT INTO t_deferred_pk SELECT concat('path/', toString(number % 1000), '/file.log'), number FROM numbers(0, 25000);
INSERT INTO t_deferred_pk SELECT concat('path/', toString(number % 1000), '/file.log'), number FROM numbers(25000, 25000);
INSERT INTO t_deferred_pk SELECT concat('path/', toString(number % 1000), '/file.log'), number FROM numbers(50000, 25000);
INSERT INTO t_deferred_pk SELECT concat('path/', toString(number % 1000), '/file.log'), number FROM numbers(75000, 25000);

SET max_threads = 4;

-- A row policy on a non-sorting-key column is deferred after FINAL (default
-- apply_row_policy_after_final = 1). The only filter is the deferred row policy, so index
-- analysis is a full scan, but read-in-order must stay enabled: no PartialSortingTransform.
-- The predicate is on `value` (not the sorting key `path`) so it is genuinely deferred;
-- it is always true here so the correctness count below stays deterministic.
CREATE ROW POLICY rp_05017 ON t_deferred_pk USING value < 1000000 TO ALL;

SELECT 'deferred_row_policy_keeps_in_order';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT * FROM t_deferred_pk FINAL
    ORDER BY path
    SETTINGS enable_parallel_replicas = 0, read_in_order_max_primary_key_ratio = 0.5, apply_row_policy_after_final = 1
) WHERE explain LIKE '%PartialSortingTransform%';

DROP ROW POLICY rp_05017 ON t_deferred_pk;

-- A PREWHERE on a non-sorting-key column, deferred after FINAL (apply_prewhere_after_final = 1),
-- is likewise excluded from index analysis and must not misfire the guard: no PartialSortingTransform.
SELECT 'deferred_prewhere_keeps_in_order';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT * FROM t_deferred_pk FINAL
    PREWHERE value != 0
    ORDER BY path
    SETTINGS enable_parallel_replicas = 0, read_in_order_max_primary_key_ratio = 0.5, apply_prewhere_after_final = 1
) WHERE explain LIKE '%PartialSortingTransform%';

-- Negative control: a real filter that is opaque to the primary key (and NOT deferred) still
-- disables read-in-order under FINAL. This is the case the optimization targets, so the guard
-- must keep firing here (a full sort with PartialSortingTransform is expected).
SELECT 'poor_pk_selectivity_final_full_sort';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT * FROM t_deferred_pk FINAL
    WHERE path LIKE '%file.log'
    ORDER BY path
    SETTINGS enable_parallel_replicas = 0, read_in_order_max_primary_key_ratio = 0.5
) WHERE explain LIKE '%PartialSortingTransform%';

-- Correctness: with the deferred row policy active and read-in-order kept, the result is
-- the 1000 distinct sorting-key values after FINAL deduplication, in sorted order.
CREATE ROW POLICY rp_05017 ON t_deferred_pk USING value < 1000000 TO ALL;
SELECT 'correctness';
SELECT count() FROM (
    SELECT * FROM t_deferred_pk FINAL
    ORDER BY path
    SETTINGS enable_parallel_replicas = 0, read_in_order_max_primary_key_ratio = 0.5, apply_row_policy_after_final = 1
);
DROP ROW POLICY rp_05017 ON t_deferred_pk;

DROP TABLE t_deferred_pk;
