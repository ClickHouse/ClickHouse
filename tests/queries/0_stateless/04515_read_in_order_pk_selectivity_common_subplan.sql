-- Tags: no-random-settings, no-random-merge-tree-settings
-- Regression coverage for the `read_in_order_max_primary_key_ratio` PK-selectivity guard when a
-- `ReadFromMergeTree` step is cloned by `materializeQueryPlanReferences`.
--
-- With `correlated_subqueries_use_in_memory_buffer = 0`, decorrelating a correlated subquery wraps the
-- outer relation in a `CommonSubplanStep` and references it from the subquery side; `optimizeTree` then
-- clones that subplan via `ReadFromMergeTree::clone` (before `optimizeReadInOrder` runs). `clone` must
-- propagate `index_analysis_had_filter`, otherwise a cloned poor-primary-key read would silently treat
-- itself as an unfiltered full scan and mis-evaluate the guard. This test exercises that clone path with
-- a poorly selective primary key and asserts the query stays correct and ratio-invariant in results.

DROP TABLE IF EXISTS t_read_in_order_pk_common_subplan;

-- Small index_granularity to produce enough marks for the guard to be able to fire, and several parts to
-- mirror the motivating multi-part case.
CREATE TABLE t_read_in_order_pk_common_subplan (path String, value UInt64)
ENGINE = MergeTree ORDER BY path
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES t_read_in_order_pk_common_subplan;

INSERT INTO t_read_in_order_pk_common_subplan SELECT concat('path/', toString(number % 1000), '/file.log'), number FROM numbers(0, 25000);
INSERT INTO t_read_in_order_pk_common_subplan SELECT concat('path/', toString(number % 1000), '/file.log'), number FROM numbers(25000, 25000);
INSERT INTO t_read_in_order_pk_common_subplan SELECT concat('path/', toString(number % 1000), '/file.log'), number FROM numbers(50000, 25000);

-- Correlated subqueries are only supported by the analyzer.
SET enable_analyzer = 1;
SET max_threads = 4, enable_parallel_replicas = 0;
SET query_plan_read_in_order = 1;
-- Force the correlated subquery to be materialized as a common subplan and cloned through
-- `materializeQueryPlanReferences` instead of being buffered in memory.
SET correlated_subqueries_use_in_memory_buffer = 0;

-- The correlated `EXISTS` is always true (numbers(10) always contains a value != n.value), so the query
-- is equivalent to a poor-primary-key `ORDER BY path` over all rows; it just forces the decorrelation +
-- subplan clone. `endsWith(path, 'file.log')` cannot use the primary key `ORDER BY path`, so this is the
-- poor-selectivity regime the guard targets.

-- All rows match, regardless of the ratio.
SELECT 'count_ratio_0';
SELECT count() FROM (
    SELECT path FROM t_read_in_order_pk_common_subplan n
    WHERE EXISTS (SELECT * FROM numbers(10) WHERE number != n.value) AND endsWith(path, 'file.log')
    ORDER BY path
    SETTINGS read_in_order_max_primary_key_ratio = 0.0
);

SELECT 'count_ratio_1';
SELECT count() FROM (
    SELECT path FROM t_read_in_order_pk_common_subplan n
    WHERE EXISTS (SELECT * FROM numbers(10) WHERE number != n.value) AND endsWith(path, 'file.log')
    ORDER BY path
    SETTINGS read_in_order_max_primary_key_ratio = 1.0
);

-- The guard only changes the plan (read-in-order vs parallel read + sort), never the result. Assert the
-- fully ordered projections are byte-identical whether the guard is forced on (ratio 0.0) or off (ratio
-- 1.0), so a cloned read that mis-evaluated the guard could not silently corrupt the ordering.
SELECT 'results_ratio_invariant';
SELECT
(
    SELECT groupArray(path) FROM (
        SELECT path FROM t_read_in_order_pk_common_subplan n
        WHERE EXISTS (SELECT * FROM numbers(10) WHERE number != n.value) AND endsWith(path, 'file.log')
        ORDER BY path, value
        SETTINGS read_in_order_max_primary_key_ratio = 0.0
    )
)
=
(
    SELECT groupArray(path) FROM (
        SELECT path FROM t_read_in_order_pk_common_subplan n
        WHERE EXISTS (SELECT * FROM numbers(10) WHERE number != n.value) AND endsWith(path, 'file.log')
        ORDER BY path, value
        SETTINGS read_in_order_max_primary_key_ratio = 1.0
    )
);

DROP TABLE t_read_in_order_pk_common_subplan;
