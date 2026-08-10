-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- A PARTITION BY window under make_distributed_plan=1 is parallelized: the "any" scatter feeding the
-- window's sort is retargeted to hash by the partition columns and the window runs per bucket below a
-- sorted gather (`tryPushWindowBelowSortedGather`). The reference pins the full distributed EXPLAIN;
-- result content is checked as a fingerprint against the non-distributed plan. The delivered global
-- order is asserted at scale by 04836_distributed_plan_window_partition_order.
--
-- The settings each plan or result depends on are pinned per query; all other settings stay
-- randomized. The checking queries themselves run without make_distributed_plan, because they read
-- from EXPLAIN and scalar subqueries, which are not serializable for remote execution.

DROP TABLE IF EXISTS t_window_shuffle;

CREATE TABLE t_window_shuffle (a UInt32, v UInt32)
ENGINE = MergeTree ORDER BY (a, v) SETTINGS index_granularity = 256;

-- The plan snapshot below includes part and granule counts, so the parts must not merge mid-test.
SYSTEM STOP MERGES t_window_shuffle;

-- Several parts and many partitions so the per-bucket partitioned sort fans out across threads.
INSERT INTO t_window_shuffle SELECT number % 10, number FROM numbers(40);
INSERT INTO t_window_shuffle SELECT number % 10, number + 1000000 FROM numbers(40);
INSERT INTO t_window_shuffle SELECT number % 10, number + 2000000 FROM numbers(40);

-- The window runs per bucket: below the sorted gather, above the exchange keyed by the partition
-- column ("scatter by (a)", collapsed with the read gather into a shuffle).
-- optimize_sorting_by_input_stream_properties is pinned because it decides whether the ORDER BY sort
-- above the window becomes a `FinishSorting`, which the pinned plan shape includes.
SELECT '-- distributed plan: window below the sorted gather, above a partition-keyed exchange';
EXPLAIN SELECT a, v, sum(v) OVER (PARTITION BY a ORDER BY v) AS s FROM t_window_shuffle ORDER BY a, v
SETTINGS make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 1,
    distributed_plan_max_rows_to_broadcast = 0, enable_join_runtime_filters = 0,
    distributed_plan_default_shuffle_join_bucket_count = 8, distributed_plan_default_reader_bucket_count = 8,
    optimize_read_in_order = 0, optimize_sorting_by_input_stream_properties = 1,
    distributed_plan_optimize_exchanges = 1;

-- Negative control: without the exchange optimization the rule does not run, so the keyed-scatter
-- signature must be absent from the plan. This also proves the signature detects the pre-rule shape.
SELECT 'window stays gathered without exchange optimization:', countIf(explain LIKE '%scatter by%') = 0
FROM
(
    EXPLAIN SELECT a, v, sum(v) OVER (PARTITION BY a ORDER BY v) AS s FROM t_window_shuffle ORDER BY a, v
    SETTINGS make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 1,
        distributed_plan_max_rows_to_broadcast = 0, enable_join_runtime_filters = 0,
        distributed_plan_default_shuffle_join_bucket_count = 8, distributed_plan_default_reader_bucket_count = 8,
        optimize_read_in_order = 0, distributed_plan_optimize_exchanges = 0
);

-- Order-insensitive content fingerprint of the distributed result vs the non-distributed plan.
-- max_threads is pinned so the per-bucket partitioned sort fans out and the order-preserving
-- merge paths are exercised. optimize_sorting_by_input_stream_properties and
-- distributed_plan_optimize_exchanges are pinned so this executes the same pushed-down shape as
-- the plan snapshot above, including the serialized `FinishSorting`.
SELECT 'distributed result matches non-distributed:',
(
    SELECT sum(cityHash64(a, v, s, roll, rn))
    FROM
    (
        SELECT a, v,
            sum(v) OVER (PARTITION BY a ORDER BY v) AS s,
            sum(v) OVER (PARTITION BY a ORDER BY v ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS roll,
            row_number() OVER (PARTITION BY a ORDER BY v) AS rn
        FROM t_window_shuffle
        SETTINGS make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 1,
            distributed_plan_max_rows_to_broadcast = 0, enable_join_runtime_filters = 0,
            distributed_plan_default_shuffle_join_bucket_count = 8, distributed_plan_default_reader_bucket_count = 8,
            optimize_read_in_order = 0, max_threads = 8, optimize_sorting_by_input_stream_properties = 1,
            distributed_plan_optimize_exchanges = 1
    )
) =
(
    SELECT sum(cityHash64(a, v, s, roll, rn))
    FROM
    (
        SELECT a, v,
            sum(v) OVER (PARTITION BY a ORDER BY v) AS s,
            sum(v) OVER (PARTITION BY a ORDER BY v ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS roll,
            row_number() OVER (PARTITION BY a ORDER BY v) AS rn
        FROM t_window_shuffle
    )
);

DROP TABLE t_window_shuffle;
