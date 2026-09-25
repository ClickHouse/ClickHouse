-- Tags: no-random-settings, no-random-merge-tree-settings
-- The `prefer_multiple_streams` opt-out must reach the child reads underneath an `ENGINE = Merge`
-- table. `ReadFromMerge` hides the real `ReadFromMergeTree` steps in child plans, so without
-- forwarding the opt-out the child would still let `PrefetchingConcatProcessor` collapse a
-- single-part filtered read into one stream and serialize exactly the per-stream
-- `LIMIT BY` / `DISTINCT` / aggregation work that the flag is meant to keep parallel.
--
-- As in `04054_read_in_order_prefetching_concat`, the pipeline shape depends on many interacting
-- `MergeTree` and query-plan settings, so randomization is disabled to keep the test deterministic.
--
-- Note: plain `EXPLAIN PIPELINE` attributes no processors to `ReadFromMerge` (the child pipelines
-- are built separately), so the child reads are invisible there. `graph = 1` dumps the real
-- processor graph, child pipelines included, which is what these assertions need.
SET read_in_order_two_level_merge_threshold = 100;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1;
SET parallel_replicas_local_plan = 1;
SET enable_parallel_replicas = 0;
SET max_threads = 4;
SET optimize_read_in_order = 1;

DROP TABLE IF EXISTS t_merge_prefetching_concat;
DROP TABLE IF EXISTS merge_prefetching_concat;

CREATE TABLE t_merge_prefetching_concat (path String, value UInt64)
ENGINE = MergeTree ORDER BY path
SETTINGS index_granularity = 8192
AS SELECT concat('path/', toString(number % 1000), '/file.log'), number FROM numbers(1000000);

OPTIMIZE TABLE t_merge_prefetching_concat FINAL;

CREATE TABLE merge_prefetching_concat (path String, value UInt64)
ENGINE = Merge(currentDatabase(), '^t_merge_prefetching_concat$');

-- Baseline: without any per-stream work above the read, the child read of the `Merge` table does
-- use PrefetchingConcat - the opt-out must not be applied unconditionally.
SELECT 'baseline_prefetching_used';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE graph = 1 SELECT path FROM merge_prefetching_concat
    PREWHERE path LIKE '%file.log'
    ORDER BY path
) WHERE explain LIKE '%PrefetchingConcat%';

-- `LIMIT BY` on a sorting-key prefix without `ORDER BY`: `LimitByStep` drives read-in-order and
-- runs a per-stream `LimitBySortedStreamTransform` pre-filter. No PrefetchingConcat.
SELECT 'no_prefetching_limit_by';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE graph = 1 SELECT path FROM merge_prefetching_concat
    PREWHERE path LIKE '%file.log'
    LIMIT 1 BY path
) WHERE explain LIKE '%PrefetchingConcat%';

-- Same with an `ORDER BY`, which pushes the `LIMIT BY` into the sort as a per-stream pre-filter.
SELECT 'no_prefetching_order_by_limit_by';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE graph = 1 SELECT path FROM merge_prefetching_concat
    PREWHERE path LIKE '%file.log'
    ORDER BY path
    LIMIT 1 BY path
) WHERE explain LIKE '%PrefetchingConcat%';

-- Distinct-in-order runs a parallel pre-distinct transform per stream. No PrefetchingConcat.
SELECT 'no_prefetching_distinct';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE graph = 1 SELECT DISTINCT path FROM merge_prefetching_concat
    PREWHERE path LIKE '%file.log'
    ORDER BY path
) WHERE explain LIKE '%PrefetchingConcat%';

-- Aggregation-in-order needs multiple streams for parallel aggregation. No PrefetchingConcat.
SELECT 'no_prefetching_aggregation_in_order';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE graph = 1 SELECT path, count() FROM merge_prefetching_concat
    PREWHERE path LIKE '%file.log'
    GROUP BY path
    SETTINGS optimize_aggregation_in_order = 1
) WHERE explain LIKE '%PrefetchingConcat%';

-- A residual `WHERE` above the read is per-row CPU work that must stay parallel.
SELECT 'no_prefetching_residual_filter';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE graph = 1 SELECT path FROM merge_prefetching_concat
    PREWHERE path LIKE '%file.log'
    WHERE value % 3 = 0
    ORDER BY path
) WHERE explain LIKE '%PrefetchingConcat%';

-- Correctness of the results is unaffected either way.
SELECT 'correctness';
SELECT
    (SELECT sum(cityHash64(path)) FROM (
        SELECT path FROM merge_prefetching_concat PREWHERE path LIKE '%file.log' ORDER BY path LIMIT 1 BY path
    ))
    ==
    (SELECT sum(cityHash64(path)) FROM (
        SELECT path FROM t_merge_prefetching_concat PREWHERE path LIKE '%file.log' ORDER BY path LIMIT 1 BY path
        SETTINGS max_threads = 1
    ));

DROP TABLE merge_prefetching_concat;
DROP TABLE t_merge_prefetching_concat;
