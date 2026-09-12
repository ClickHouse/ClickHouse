-- Tags: no-random-settings, no-random-merge-tree-settings
-- An `ORDER BY ... LIMIT BY ...` opts out of `PrefetchingConcatProcessor` only when the read
-- actually provides the full sort order, because only then does `SortingStep` attach the
-- per-stream `LimitBySortedStreamTransform` pre-filter that the opt-out protects. When the read
-- provides just a prefix of the sort order, the sort still has to run `finishSorting` and
-- deliberately skips the per-stream pre-filter (applying `LIMIT BY` before the final order is
-- known could drop the wrong rows), so there is nothing to keep parallel and
-- `PrefetchingConcatProcessor` must still be used.
--
-- As in `04054_read_in_order_prefetching_concat`, the pipeline shape depends on many `MergeTree`
-- and query-plan settings, so randomization is disabled and the relevant ones are pinned.
SET read_in_order_two_level_merge_threshold = 100;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SET optimize_move_to_prewhere = 0;
SET query_plan_optimize_prewhere = 0;
SET parallel_replicas_local_plan = 1;
SET enable_parallel_replicas = 0;
SET max_threads = 4;
SET optimize_read_in_order = 1;
SET query_plan_push_limit_by_into_sort = 1;

DROP TABLE IF EXISTS t_prefetching_concat_limit_by_prefix;

-- The table is ordered by `path` only, so `ORDER BY path, value` can be read in order only up to
-- the `path` prefix.
CREATE TABLE t_prefetching_concat_limit_by_prefix (path String, value UInt64)
ENGINE = MergeTree ORDER BY path
SETTINGS index_granularity = 8192
AS SELECT concat('path/', toString(number % 100000), '/file.log'), number FROM numbers(1000000);

OPTIMIZE TABLE t_prefetching_concat_limit_by_prefix FINAL;

-- The read provides the full sort order: the per-stream pre-filter is attached, so the streams
-- must stay parallel and `PrefetchingConcat` must not be used.
SELECT 'full_order_no_prefetching';
SELECT
    countIf(explain LIKE '%PrefetchingConcat%') AS has_prefetching_concat,
    countIf(explain LIKE '%LimitBySortedStreamTransform%') >= 2 AS has_per_stream_prefilter
FROM (
    EXPLAIN PIPELINE SELECT path FROM t_prefetching_concat_limit_by_prefix
    PREWHERE path LIKE '%file.log'
    ORDER BY path
    LIMIT 1 BY path
);

-- The read provides only the `path` prefix of `ORDER BY path, value`, so the sort runs
-- `finishSorting` and installs no per-stream pre-filter. `PrefetchingConcat` stays applicable.
SELECT 'prefix_order_has_prefetching';
SELECT
    countIf(explain LIKE '%PrefetchingConcat%') AS has_prefetching_concat,
    countIf(explain LIKE '%LimitBySortedStreamTransform%') AS per_stream_prefilters
FROM (
    EXPLAIN PIPELINE SELECT path, value FROM t_prefetching_concat_limit_by_prefix
    PREWHERE path LIKE '%file.log'
    ORDER BY path, value
    LIMIT 1 BY path
);

SELECT 'prefix_order_correctness';
SELECT
    countIf((path, value) < (prev_path, prev_value)) = 0 AS is_sorted,
    count() = (SELECT uniqExact(path) FROM t_prefetching_concat_limit_by_prefix WHERE path LIKE '%file.log') AS count_matches,
    sum(value) = (SELECT sum(m) FROM (SELECT min(value) AS m FROM t_prefetching_concat_limit_by_prefix WHERE path LIKE '%file.log' GROUP BY path)) AS keeps_first_row_of_group
FROM (
    SELECT
        path,
        value,
        lagInFrame(path, 1, '') OVER (ORDER BY rowNumberInAllBlocks()) AS prev_path,
        lagInFrame(value, 1, 0) OVER (ORDER BY rowNumberInAllBlocks()) AS prev_value
    FROM (
        SELECT path, value FROM t_prefetching_concat_limit_by_prefix
        PREWHERE path LIKE '%file.log'
        ORDER BY path, value
        LIMIT 1 BY path
    )
);

DROP TABLE t_prefetching_concat_limit_by_prefix;
