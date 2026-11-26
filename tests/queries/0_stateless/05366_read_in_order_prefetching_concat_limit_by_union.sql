-- Tags: no-random-settings, no-random-merge-tree-settings
-- An `ORDER BY ... LIMIT BY ...` over a `UNION ALL` attaches the per-stream
-- `LimitBySortedStreamTransform` pre-filter above the union, and every union branch is read in
-- order. The opt-out from `PrefetchingConcatProcessor` must reach the reads of all branches,
-- otherwise each branch collapses its single-part read into one stream and serializes the
-- per-stream pre-filter that the opt-out protects.
--
-- As in `04656_read_in_order_prefetching_concat_limit_by_prefix`, the pipeline shape depends on
-- many `MergeTree` and query-plan settings, so randomization is disabled and the relevant ones are pinned.
SET read_in_order_two_level_merge_threshold = 100;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SET optimize_move_to_prewhere = 0;
SET query_plan_optimize_prewhere = 0;
SET parallel_replicas_local_plan = 1;
SET enable_parallel_replicas = 0;
SET max_threads = 4;
SET optimize_read_in_order = 1;
SET query_plan_push_limit_by_into_sort = 1;

DROP TABLE IF EXISTS t_prefetching_concat_limit_by_union_1;
DROP TABLE IF EXISTS t_prefetching_concat_limit_by_union_2;

CREATE TABLE t_prefetching_concat_limit_by_union_1 (path String, value UInt64)
ENGINE = MergeTree ORDER BY path
SETTINGS index_granularity = 8192
AS SELECT concat('path/', toString(number % 100000), '/file.log'), number FROM numbers(1000000);

CREATE TABLE t_prefetching_concat_limit_by_union_2 (path String, value UInt64)
ENGINE = MergeTree ORDER BY path
SETTINGS index_granularity = 8192
AS SELECT concat('path/', toString(number % 100000), '/file.log'), number FROM numbers(1000000, 1000000);

OPTIMIZE TABLE t_prefetching_concat_limit_by_union_1 FINAL;
OPTIMIZE TABLE t_prefetching_concat_limit_by_union_2 FINAL;

SELECT 'union_no_prefetching';
SELECT
    countIf(explain LIKE '%PrefetchingConcat%') AS has_prefetching_concat,
    countIf(explain LIKE '%LimitBySortedStreamTransform%') >= 2 AS has_per_stream_prefilter
FROM (
    EXPLAIN PIPELINE SELECT path FROM
    (
        SELECT path FROM t_prefetching_concat_limit_by_union_1 PREWHERE path LIKE '%file.log'
        UNION ALL
        SELECT path FROM t_prefetching_concat_limit_by_union_2 PREWHERE path LIKE '%file.log'
    )
    ORDER BY path
    LIMIT 1 BY path
);

SELECT 'union_correctness';
SELECT
    countIf(path < prev_path) = 0 AS is_sorted,
    count() = (SELECT uniqExact(path) FROM t_prefetching_concat_limit_by_union_1) AS count_matches
FROM (
    SELECT
        path,
        lagInFrame(path, 1, '') OVER (ORDER BY rowNumberInAllBlocks()) AS prev_path
    FROM (
        SELECT path FROM
        (
            SELECT path FROM t_prefetching_concat_limit_by_union_1 PREWHERE path LIKE '%file.log'
            UNION ALL
            SELECT path FROM t_prefetching_concat_limit_by_union_2 PREWHERE path LIKE '%file.log'
        )
        ORDER BY path
        LIMIT 1 BY path
    )
);

DROP TABLE t_prefetching_concat_limit_by_union_1;
DROP TABLE t_prefetching_concat_limit_by_union_2;
