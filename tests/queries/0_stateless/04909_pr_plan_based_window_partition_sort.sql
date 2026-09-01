-- Tags: no-random-merge-tree-settings

-- A window function's pre-sort must not be shipped under plan-based parallel replicas
-- (`parallel_replicas_plan_based`).
--
-- The split is lifted through a `SortingStep` (see 04836_pr_plan_based_read_in_order), but a window pre-sort
-- is a partitioned sort: it scatters by the `PARTITION BY` keys and skips the final merge, so `WindowStep`
-- above it runs one `WindowTransform` per stream. The `MergingSorted` step the lift puts on the initiator
-- cannot express that, so shipping such a sort collapses the sort and the window to a single stream. The sort
-- therefore stays above the split, the same as with classic parallel replicas.
-- See https://github.com/ClickHouse/ClickHouse/issues/115174

DROP TABLE IF EXISTS t_pr_window;

CREATE TABLE t_pr_window (a UInt64, p UInt64, v UInt64)
ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 128;

INSERT INTO t_pr_window SELECT number, number % 4, number * 7 % 1000 FROM numbers(20000);
OPTIMIZE TABLE t_pr_window FINAL;

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_local_plan = 1;
SET parallel_replicas_plan_based = 1;
-- Pin the manual mode: CI randomizes `automatic_parallel_replicas_mode` to 2, and the cost model may then
-- decide against parallel replicas, so the plan-based split would never engage.
SET automatic_parallel_replicas_mode = 0;
-- The partition scatter is only inserted with more than one thread, and CI randomizes `max_threads`.
SET max_threads = 4;

-- The window results must match non-parallel execution. `row_number()` restarts per partition, so these
-- aggregates change as soon as the window is computed over the wrong scope - a partition spread over several
-- streams, or rows of one partition arriving out of order.
SELECT '--- row_number() OVER (PARTITION BY p ORDER BY a), local ---';
SELECT p, count(), sum(rn), max(rn) FROM
    (SELECT p, row_number() OVER (PARTITION BY p ORDER BY a) AS rn FROM t_pr_window)
GROUP BY p ORDER BY p SETTINGS enable_parallel_replicas = 0;
SELECT '--- row_number() OVER (PARTITION BY p ORDER BY a), plan_based = 1 ---';
SELECT p, count(), sum(rn), max(rn) FROM
    (SELECT p, row_number() OVER (PARTITION BY p ORDER BY a) AS rn FROM t_pr_window)
GROUP BY p ORDER BY p;

-- A running sum depends on the order inside each partition, not only on the set of rows.
SELECT '--- running sum within a partition, local ---';
SELECT p, a, s FROM
    (SELECT p, a, sum(v) OVER (PARTITION BY p ORDER BY a) AS s FROM t_pr_window)
ORDER BY p, a LIMIT 3 BY p LIMIT 12 SETTINGS enable_parallel_replicas = 0;
SELECT '--- running sum within a partition, plan_based = 1 ---';
SELECT p, a, s FROM
    (SELECT p, a, sum(v) OVER (PARTITION BY p ORDER BY a) AS s FROM t_pr_window)
ORDER BY p, a LIMIT 3 BY p LIMIT 12;

-- The read is still distributed, but the partitioned sort is kept above the split: no per-replica "partial"
-- sort and no "merge sorted streams from replicas" on the initiator.
SELECT '--- explain: has_remote_read, sort_not_shipped ---';
SELECT
    countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 AS has_remote_read,
    countIf(explain LIKE '%merge sorted streams from replicas%') = 0 AS sort_not_shipped
FROM (EXPLAIN pretty = 0 SELECT p, row_number() OVER (PARTITION BY p ORDER BY a) FROM t_pr_window);

-- ... so the window keeps its per-partition streams: the pipeline still scatters by the partition keys and
-- runs one `WindowTransform` per stream instead of merging everything into one.
SELECT '--- explain pipeline: scatter_by_partition, parallel_window ---';
SELECT
    countIf(explain LIKE '%ScatterByPartitionTransform%') > 0 AS scatter_by_partition,
    countIf(explain LIKE '%WindowTransform × 4%') > 0 AS parallel_window
FROM (EXPLAIN PIPELINE SELECT p, row_number() OVER (PARTITION BY p ORDER BY a) FROM t_pr_window);

DROP TABLE t_pr_window;
