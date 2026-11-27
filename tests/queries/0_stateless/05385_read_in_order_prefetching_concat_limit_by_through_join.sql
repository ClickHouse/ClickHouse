-- Tags: no-random-settings, no-random-merge-tree-settings
-- An `ORDER BY ... LIMIT BY ...` or a `DISTINCT` above an order-preserving `LEFT JOIN` installs
-- per-stream work (the `LimitBySortedStreamTransform` pre-filter, the preliminary distinct) above
-- the join, and the probe side is read in order through it. The opt-out from
-- `PrefetchingConcatProcessor` must reach the probe-side read, otherwise the single-part read
-- collapses into one stream before the join and serializes that per-stream work.
--
-- The pipeline shape depends on many `MergeTree` and query-plan settings, so randomization is
-- disabled and the relevant ones are pinned.
SET read_in_order_two_level_merge_threshold = 100;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SET optimize_move_to_prewhere = 0;
SET query_plan_optimize_prewhere = 0;
SET enable_parallel_replicas = 0;
SET max_threads = 4;
SET optimize_read_in_order = 1;
SET query_plan_push_limit_by_into_sort = 1;
SET query_plan_read_in_order_through_join = 1;
SET join_algorithm = 'hash';

DROP TABLE IF EXISTS t_prefetching_concat_join_left;
DROP TABLE IF EXISTS t_prefetching_concat_join_right;

CREATE TABLE t_prefetching_concat_join_left (path String, value UInt64)
ENGINE = MergeTree ORDER BY path
SETTINGS index_granularity = 8192
AS SELECT concat('path/', toString(number % 100000), '/file.log'), number FROM numbers(1000000);

CREATE TABLE t_prefetching_concat_join_right (path String, x UInt64)
ENGINE = MergeTree ORDER BY path
AS SELECT concat('path/', toString(number), '/file.log'), number FROM numbers(1000);

OPTIMIZE TABLE t_prefetching_concat_join_left FINAL;

SELECT 'limit_by';
SELECT
    countIf(explain LIKE '%PrefetchingConcat%') AS has_prefetching_concat,
    countIf(explain LIKE '%LimitBySortedStreamTransform%') >= 2 AS has_per_stream_prefilter
FROM (
    EXPLAIN PIPELINE SELECT l.path
    FROM t_prefetching_concat_join_left AS l LEFT JOIN t_prefetching_concat_join_right AS r ON l.path = r.path
    PREWHERE l.path LIKE '%file.log'
    ORDER BY l.path
    LIMIT 1 BY l.path
);

SELECT 'distinct';
SELECT countIf(explain LIKE '%PrefetchingConcat%') AS has_prefetching_concat
FROM (
    EXPLAIN PIPELINE SELECT DISTINCT l.path
    FROM t_prefetching_concat_join_left AS l LEFT JOIN t_prefetching_concat_join_right AS r ON l.path = r.path
    PREWHERE l.path LIKE '%file.log'
    ORDER BY l.path
);

SELECT 'correctness';
SELECT
    countIf(path < prev_path) = 0 AS is_sorted,
    count() = 100000 AS count_matches
FROM (
    SELECT
        path,
        lagInFrame(path, 1, '') OVER (ORDER BY rowNumberInAllBlocks()) AS prev_path
    FROM (
        SELECT l.path AS path
        FROM t_prefetching_concat_join_left AS l LEFT JOIN t_prefetching_concat_join_right AS r ON l.path = r.path
        PREWHERE l.path LIKE '%file.log'
        ORDER BY l.path
        LIMIT 1 BY l.path
    )
);

DROP TABLE t_prefetching_concat_join_left;
DROP TABLE t_prefetching_concat_join_right;
