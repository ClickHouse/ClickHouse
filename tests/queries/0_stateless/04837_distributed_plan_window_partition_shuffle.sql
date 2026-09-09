-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- A PARTITION BY window under make_distributed_plan=1 is parallelized: the "any" scatter feeding the
-- window's sort is retargeted to hash by the partition columns and the window runs per bucket below a
-- sorted gather (`tryPushWindowBelowSortedGather`). The reference pins the full distributed EXPLAIN,
-- and the two result lines at the end (distributed and local) must show the same value. The delivered
-- global order is asserted at scale by 04836_distributed_plan_window_partition_order.

DROP TABLE IF EXISTS t_window_shuffle;

CREATE TABLE t_window_shuffle (a UInt32, v UInt32)
ENGINE = MergeTree ORDER BY (a, v) SETTINGS index_granularity = 256;

-- The plan snapshot below includes part and granule counts, so the parts must not merge mid-test.
SYSTEM STOP MERGES t_window_shuffle;

-- Several parts and many partitions so the per-bucket partitioned sort fans out across threads.
INSERT INTO t_window_shuffle SELECT number % 10, number FROM numbers(40);
INSERT INTO t_window_shuffle SELECT number % 10, number + 1000000 FROM numbers(40);
INSERT INTO t_window_shuffle SELECT number % 10, number + 2000000 FROM numbers(40);

-- optimize_sorting_by_input_stream_properties decides whether the ORDER BY sort above the window
-- becomes a `FinishSorting`, which the pinned plan shape includes. max_rows_to_group_by must be 0,
-- otherwise make_distributed_plan declines plans with an aggregation.
SET make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 1,
    distributed_plan_max_rows_to_broadcast = 0, enable_join_runtime_filters = 0,
    distributed_plan_default_shuffle_join_bucket_count = 8, distributed_plan_default_reader_bucket_count = 8,
    optimize_read_in_order = 0, optimize_sorting_by_input_stream_properties = 1,
    distributed_plan_optimize_exchanges = 1, max_threads = 8, max_rows_to_group_by = 0;

-- The window runs per bucket: below the sorted gather, above the exchange keyed by the partition
-- column ("scatter by (a)", collapsed with the read gather into a shuffle).
SELECT '-- distributed plan: window below the sorted gather, above a partition-keyed exchange';
EXPLAIN SELECT a, v, sum(v) OVER (PARTITION BY a ORDER BY v) AS s FROM t_window_shuffle ORDER BY a, v;

-- Negative control: without the exchange optimization the rule does not run, so the plan is still
-- distributed (it has a gather) but has no keyed scatter. The checking query itself runs local.
SELECT 'window stays gathered without exchange optimization:',
    countIf(explain LIKE '%scatter by%') = 0 AND countIf(explain LIKE '%GatherExchange%') > 0
FROM
(
    EXPLAIN SELECT a, v, sum(v) OVER (PARTITION BY a ORDER BY v) AS s FROM t_window_shuffle ORDER BY a, v
    SETTINGS make_distributed_plan = 1, distributed_plan_optimize_exchanges = 0
)
SETTINGS make_distributed_plan = 0;

-- The same windows computed distributed and local; both lines must show the same value.
SELECT sum(cityHash64(a, v, s, roll, rn)) FROM
(
    SELECT a, v,
        sum(v) OVER (PARTITION BY a ORDER BY v) AS s,
        sum(v) OVER (PARTITION BY a ORDER BY v ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS roll,
        row_number() OVER (PARTITION BY a ORDER BY v) AS rn
    FROM t_window_shuffle
);
SELECT sum(cityHash64(a, v, s, roll, rn)) FROM
(
    SELECT a, v,
        sum(v) OVER (PARTITION BY a ORDER BY v) AS s,
        sum(v) OVER (PARTITION BY a ORDER BY v ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS roll,
        row_number() OVER (PARTITION BY a ORDER BY v) AS rn
    FROM t_window_shuffle
) SETTINGS make_distributed_plan = 0;

DROP TABLE t_window_shuffle;
