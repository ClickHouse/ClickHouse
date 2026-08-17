-- Tags: no-random-settings, no-random-merge-tree-settings

-- Test PrefetchingConcatProcessor behavior for read-in-order.
-- For single-part tables, PrefetchingConcat is NOT used because it would collapse
-- all streams into one before downstream transforms, destroying parallelism.
-- For multi-part tables, PrefetchingConcat is used per-part to enable parallel I/O
-- within each part while MergingSorted merges between parts.

-- Note on data sizes: this test uses small row counts and overrides
-- `merge_tree_min_rows_for_concurrent_read` / `merge_tree_min_read_task_size`
-- so per-part splitting still triggers. Otherwise the flaky check times out.

DROP TABLE IF EXISTS t_prefetching_concat;

CREATE TABLE t_prefetching_concat (path String, value UInt64)
ENGINE = MergeTree ORDER BY path
SETTINGS index_granularity = 1024
AS SELECT concat('path/', toString(number % 10000), '/file.log'), number FROM numbers(50000);

OPTIMIZE TABLE t_prefetching_concat FINAL;

-- PrefetchingConcat should NOT appear for a single-part table (it would destroy
-- downstream parallelism by collapsing all streams into one).
SELECT 'no_prefetching_single_part';
SELECT count() = 0 FROM (
    EXPLAIN PIPELINE SELECT * FROM t_prefetching_concat
    WHERE path LIKE '%file.log'
    ORDER BY path
    SETTINGS enable_parallel_replicas = 0, max_threads = 4, optimize_read_in_order = 1,
             merge_tree_min_rows_for_concurrent_read = 1024, merge_tree_min_bytes_for_concurrent_read = 0, merge_tree_min_read_task_size = 2
) WHERE explain LIKE '%PrefetchingConcat%';

-- Verify correctness: output must be sorted. We capture the pipeline output order
-- with `groupArray` (which preserves arrival order) and check that it is already
-- sorted. We must not re-sort here (e.g. with a window function `OVER (ORDER BY ...)`),
-- because that would mask any reordering introduced by `PrefetchingConcat`.
SELECT 'correctness';
SELECT groupArray(path) = arraySort(groupArray(path)) FROM (
    SELECT path FROM t_prefetching_concat WHERE path LIKE '%file.log' ORDER BY path
    SETTINGS max_threads = 4, optimize_read_in_order = 1,
             merge_tree_min_rows_for_concurrent_read = 1024, merge_tree_min_bytes_for_concurrent_read = 0, merge_tree_min_read_task_size = 2
);

-- PrefetchingConcat should NOT be used with LIMIT (read_limit != 0).
SELECT 'no_prefetching_with_limit';
SELECT count() = 0 FROM (
    EXPLAIN PIPELINE SELECT * FROM t_prefetching_concat
    ORDER BY path
    LIMIT 10
    SETTINGS enable_parallel_replicas = 0, max_threads = 4, optimize_read_in_order = 1,
             merge_tree_min_rows_for_concurrent_read = 1024, merge_tree_min_bytes_for_concurrent_read = 0, merge_tree_min_read_task_size = 2
) WHERE explain LIKE '%PrefetchingConcat%';

DROP TABLE t_prefetching_concat;

-- PrefetchingConcat should be used per-part for multi-part tables (with partitions).
-- Each part gets parallel streams with PrefetchingConcat, MergingSorted merges between parts.
DROP TABLE IF EXISTS t_prefetching_concat_multi;
CREATE TABLE t_prefetching_concat_multi (key UInt64, value String)
ENGINE = MergeTree PARTITION BY intDiv(key, 30000) ORDER BY key
SETTINGS index_granularity = 1024;
INSERT INTO t_prefetching_concat_multi SELECT number, toString(number) FROM numbers(90000);
OPTIMIZE TABLE t_prefetching_concat_multi FINAL;

SELECT 'prefetching_multi_part';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT * FROM t_prefetching_concat_multi
    WHERE value LIKE '%5%'
    ORDER BY key
    SETTINGS enable_parallel_replicas = 0, max_threads = 6, optimize_read_in_order = 1,
             merge_tree_min_rows_for_concurrent_read = 1024, merge_tree_min_bytes_for_concurrent_read = 0, merge_tree_min_read_task_size = 2
) WHERE explain LIKE '%PrefetchingConcat%';

-- Per-part splitting must keep the total number of read streams bounded by `max_threads`.
-- We sum the `× N` multipliers on `MergeTreeSelect` lines from `EXPLAIN PIPELINE`.
SELECT 'multi_part_streams_within_cap';
SELECT sum(streams) <= 6 FROM (
    SELECT
        if(match(explain, 'MergeTreeSelect.*× (\\d+)'),
           toUInt32(extract(explain, 'MergeTreeSelect.*× (\\d+)')),
           1) AS streams
    FROM (
        EXPLAIN PIPELINE SELECT * FROM t_prefetching_concat_multi
        WHERE value LIKE '%5%'
        ORDER BY key
        SETTINGS enable_parallel_replicas = 0, max_threads = 6, optimize_read_in_order = 1,
                 merge_tree_min_rows_for_concurrent_read = 1024, merge_tree_min_bytes_for_concurrent_read = 0, merge_tree_min_read_task_size = 2
    )
    WHERE explain LIKE '%MergeTreeSelect%'
);

-- Correctness: output must be sorted across partitions.
SELECT 'multi_part_correctness';
SELECT groupArray(key) = arraySort(groupArray(key)) FROM (
    SELECT key FROM t_prefetching_concat_multi WHERE value LIKE '%5%' ORDER BY key
    SETTINGS max_threads = 6, optimize_read_in_order = 1,
             merge_tree_min_rows_for_concurrent_read = 1024, merge_tree_min_bytes_for_concurrent_read = 0, merge_tree_min_read_task_size = 2
);

-- Multi-part reverse `ORDER BY key DESC`: per-part `PrefetchingConcat` is enabled
-- and must reverse the pipe order so that the merge produces correctly descending output.
SELECT 'reverse_prefetching_multi_part';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT * FROM t_prefetching_concat_multi
    WHERE value LIKE '%5%'
    ORDER BY key DESC
    SETTINGS enable_parallel_replicas = 0, max_threads = 6, optimize_read_in_order = 1,
             merge_tree_min_rows_for_concurrent_read = 1024, merge_tree_min_bytes_for_concurrent_read = 0, merge_tree_min_read_task_size = 2
) WHERE explain LIKE '%PrefetchingConcat%';

SELECT 'reverse_multi_part_correctness';
SELECT groupArray(key) = arrayReverseSort(groupArray(key)) FROM (
    SELECT key FROM t_prefetching_concat_multi WHERE value LIKE '%5%' ORDER BY key DESC
    SETTINGS max_threads = 6, optimize_read_in_order = 1,
             merge_tree_min_rows_for_concurrent_read = 1024, merge_tree_min_bytes_for_concurrent_read = 0, merge_tree_min_read_task_size = 2
);

DROP TABLE t_prefetching_concat_multi;

-- PrefetchingConcat should NOT appear for reverse-order reads on single-part tables.
DROP TABLE IF EXISTS t_prefetching_concat_reverse;
CREATE TABLE t_prefetching_concat_reverse (key UInt64, value String)
ENGINE = MergeTree ORDER BY key
SETTINGS index_granularity = 1024;
INSERT INTO t_prefetching_concat_reverse SELECT number, toString(number) FROM numbers(50000);
OPTIMIZE TABLE t_prefetching_concat_reverse FINAL;

SELECT 'reverse_no_prefetching_single_part';
SELECT count() = 0 FROM (
    EXPLAIN PIPELINE SELECT * FROM t_prefetching_concat_reverse
    WHERE value LIKE '%5%'
    ORDER BY key DESC
    SETTINGS enable_parallel_replicas = 0, max_threads = 4, optimize_read_in_order = 1,
             merge_tree_min_rows_for_concurrent_read = 1024, merge_tree_min_bytes_for_concurrent_read = 0, merge_tree_min_read_task_size = 2
) WHERE explain LIKE '%PrefetchingConcat%';

-- Correctness: output must be in descending order.
SELECT 'reverse_correctness';
SELECT groupArray(key) = arrayReverseSort(groupArray(key)) FROM (
    SELECT key FROM t_prefetching_concat_reverse WHERE value LIKE '%5%' ORDER BY key DESC
    SETTINGS max_threads = 4, optimize_read_in_order = 1,
             merge_tree_min_rows_for_concurrent_read = 1024, merge_tree_min_bytes_for_concurrent_read = 0, merge_tree_min_read_task_size = 2
);

DROP TABLE t_prefetching_concat_reverse;

-- When there are more parts than `num_streams`, the per-part path cannot stay
-- within the budget (it needs at least one stream per part), so it must be
-- rejected and execution must fall back to the original distribute-by-streams
-- loop. We verify this by asserting that `PrefetchingConcat` does not appear
-- in the pipeline.
DROP TABLE IF EXISTS t_prefetching_concat_cap;
CREATE TABLE t_prefetching_concat_cap (key UInt64, value String)
ENGINE = MergeTree PARTITION BY (key % 16) ORDER BY key
SETTINGS index_granularity = 1024;
INSERT INTO t_prefetching_concat_cap SELECT number, toString(number) FROM numbers(50000);
OPTIMIZE TABLE t_prefetching_concat_cap FINAL;

SELECT 'no_prefetching_with_more_parts_than_streams';
SELECT count() = 0 FROM (
    EXPLAIN PIPELINE SELECT * FROM t_prefetching_concat_cap ORDER BY key
    SETTINGS enable_parallel_replicas = 0, max_threads = 4, optimize_read_in_order = 1,
             merge_tree_min_rows_for_concurrent_read = 1024, merge_tree_min_bytes_for_concurrent_read = 0, merge_tree_min_read_task_size = 2
) WHERE explain LIKE '%PrefetchingConcat%';

SELECT 'no_prefetching_with_max_streams_setting';
SELECT count() = 0 FROM (
    EXPLAIN PIPELINE SELECT * FROM t_prefetching_concat_cap ORDER BY key
    SETTINGS enable_parallel_replicas = 0, max_threads = 16, max_streams_for_merge_tree_reading = 4,
             allow_asynchronous_read_from_io_pool_for_merge_tree = 0, optimize_read_in_order = 1,
             merge_tree_min_rows_for_concurrent_read = 1024, merge_tree_min_bytes_for_concurrent_read = 0, merge_tree_min_read_task_size = 2
) WHERE explain LIKE '%PrefetchingConcat%';

DROP TABLE t_prefetching_concat_cap;

-- `LIMIT BY` drives read-in-order without an outer `ORDER BY`: `LimitByStep` runs a
-- per-stream `LimitBySortedStreamTransform` prefilter, then merges the streams with a final
-- `resize(1)` + `LimitByTransform`, so it benefits from multiple parallel input streams.
-- The read-in-order input order carries `limit = 0`, so on a multi-part table the per-part
-- `PrefetchingConcat` path (`can_use_per_part_prefetching`) would otherwise be taken and
-- collapse the parallel prefilter into one stream per part. `setPreferMultipleStreams`
-- disables it, so `PrefetchingConcat` must NOT appear for a `LIMIT BY` read, while the read
-- still goes in order (proven by the streaming `LimitBySortedStreamTransform`, which is only
-- used on the in-order `LIMIT BY` path — this guards against a vacuous pass where the read
-- simply did not go in order).
DROP TABLE IF EXISTS t_prefetching_concat_limit_by;
CREATE TABLE t_prefetching_concat_limit_by (grp UInt64, key UInt64, value String)
ENGINE = MergeTree PARTITION BY intDiv(key, 30000) ORDER BY (grp, key)
SETTINGS index_granularity = 1024;
INSERT INTO t_prefetching_concat_limit_by SELECT number % 100, number, toString(number) FROM numbers(90000);
OPTIMIZE TABLE t_prefetching_concat_limit_by FINAL;

SELECT 'limit_by_reads_in_order';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT * FROM t_prefetching_concat_limit_by LIMIT 3 BY grp
    SETTINGS enable_parallel_replicas = 0, max_threads = 6, optimize_read_in_order = 1,
             merge_tree_min_rows_for_concurrent_read = 1024, merge_tree_min_bytes_for_concurrent_read = 0, merge_tree_min_read_task_size = 2
) WHERE explain LIKE '%LimitBySortedStreamTransform%';

SELECT 'limit_by_multi_part_no_prefetching';
SELECT count() = 0 FROM (
    EXPLAIN PIPELINE SELECT * FROM t_prefetching_concat_limit_by LIMIT 3 BY grp
    SETTINGS enable_parallel_replicas = 0, max_threads = 6, optimize_read_in_order = 1,
             merge_tree_min_rows_for_concurrent_read = 1024, merge_tree_min_bytes_for_concurrent_read = 0, merge_tree_min_read_task_size = 2
) WHERE explain LIKE '%PrefetchingConcat%';

-- Correctness: exactly 3 rows per group, all groups present, and every returned row is
-- self-consistent (`key % 100` is the group it was placed in). `LIMIT BY` without an outer
-- `ORDER BY` does not fix *which* 3 rows are kept, so we assert order-independent invariants.
SELECT 'limit_by_correctness';
SELECT count() = 300 AND uniqExact(grp) = 100 AND countIf(key % 100 != grp) = 0 FROM (
    SELECT grp, key FROM t_prefetching_concat_limit_by LIMIT 3 BY grp
    SETTINGS max_threads = 6, optimize_read_in_order = 1,
             merge_tree_min_rows_for_concurrent_read = 1024, merge_tree_min_bytes_for_concurrent_read = 0, merge_tree_min_read_task_size = 2
);

SELECT 'limit_by_per_group_count';
SELECT min(c) = 3 AND max(c) = 3 FROM (
    SELECT count() AS c FROM (
        SELECT grp, key FROM t_prefetching_concat_limit_by LIMIT 3 BY grp
        SETTINGS max_threads = 6, optimize_read_in_order = 1,
                 merge_tree_min_rows_for_concurrent_read = 1024, merge_tree_min_bytes_for_concurrent_read = 0, merge_tree_min_read_task_size = 2
    ) GROUP BY grp
);

-- `ORDER BY ... LIMIT BY ...` reaches the same per-stream prefilter through a different route:
-- `pushLimitByIntoSort` hands the `LIMIT BY` hint to the `SortingStep`, which attaches
-- `LimitBySortedStreamTransform` to every input stream before merging them. Read-in-order makes
-- that sort a `FinishSorting` whose prefix covers the whole `ORDER BY`, so the prefilter really
-- does run per stream and scales with their number. `PrefetchingConcat` must therefore stay
-- disabled here too, exactly as for the bare `LIMIT BY` above.
-- Non-vacuous: the very same query with `query_plan_push_limit_by_into_sort = 0` (no hint, so no
-- per-stream prefilter to protect) keeps `PrefetchingConcat`, as does a plain `ORDER BY` without
-- `LIMIT BY` — see `order_by_limit_by_prefetching_without_hint` below.
SELECT 'order_by_limit_by_reads_in_order';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT * FROM t_prefetching_concat_limit_by ORDER BY grp, key LIMIT 3 BY grp
    SETTINGS enable_parallel_replicas = 0, max_threads = 6, optimize_read_in_order = 1,
             merge_tree_min_rows_for_concurrent_read = 1024, merge_tree_min_bytes_for_concurrent_read = 0, merge_tree_min_read_task_size = 2
) WHERE explain LIKE '%LimitBySortedStreamTransform × %';

SELECT 'order_by_limit_by_multi_part_no_prefetching';
SELECT count() = 0 FROM (
    EXPLAIN PIPELINE SELECT * FROM t_prefetching_concat_limit_by ORDER BY grp, key LIMIT 3 BY grp
    SETTINGS enable_parallel_replicas = 0, max_threads = 6, optimize_read_in_order = 1,
             merge_tree_min_rows_for_concurrent_read = 1024, merge_tree_min_bytes_for_concurrent_read = 0, merge_tree_min_read_task_size = 2
) WHERE explain LIKE '%PrefetchingConcat%';

SELECT 'order_by_limit_by_prefetching_without_hint';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT * FROM t_prefetching_concat_limit_by ORDER BY grp, key LIMIT 3 BY grp
    SETTINGS enable_parallel_replicas = 0, max_threads = 6, optimize_read_in_order = 1, query_plan_push_limit_by_into_sort = 0,
             merge_tree_min_rows_for_concurrent_read = 1024, merge_tree_min_bytes_for_concurrent_read = 0, merge_tree_min_read_task_size = 2
) WHERE explain LIKE '%PrefetchingConcat%';

-- When a suffix of the `ORDER BY` is not covered by the read-in-order prefix, `SortingStep` cannot
-- run the prefilter per stream (the suffix keys decide which rows come first inside a group), so
-- there is nothing to protect and the per-part prefetching stays enabled.
SELECT 'order_by_limit_by_with_sort_suffix_keeps_prefetching';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT * FROM t_prefetching_concat_limit_by ORDER BY grp, key, value LIMIT 3 BY grp
    SETTINGS enable_parallel_replicas = 0, max_threads = 6, optimize_read_in_order = 1,
             merge_tree_min_rows_for_concurrent_read = 1024, merge_tree_min_bytes_for_concurrent_read = 0, merge_tree_min_read_task_size = 2
) WHERE explain LIKE '%PrefetchingConcat%';

-- With an outer `ORDER BY` the result is fully deterministic: the 3 smallest keys of every group.
SELECT 'order_by_limit_by_correctness';
-- Every group `g` holds the keys `g`, `g + 100`, `g + 200`, ...; the three smallest are the first
-- three of those, so the expected sum is `sum(3 * g + 300)` over `g` in `[0, 100)` = 44850.
SELECT count() = 300 AND uniqExact(grp) = 100 AND sum(key) = 44850 FROM (
    SELECT grp, key FROM t_prefetching_concat_limit_by ORDER BY grp, key LIMIT 3 BY grp
    SETTINGS max_threads = 6, optimize_read_in_order = 1,
             merge_tree_min_rows_for_concurrent_read = 1024, merge_tree_min_bytes_for_concurrent_read = 0, merge_tree_min_read_task_size = 2
);

DROP TABLE t_prefetching_concat_limit_by;
