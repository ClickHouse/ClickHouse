-- Tags: no-random-settings, no-random-merge-tree-settings

-- Per-block virtual rows (`read_in_order_use_virtual_row_per_block`) are incompatible with
-- the per-part `PrefetchingConcat`: the prefetching processor pulls eagerly from every input
-- with no virtual-row stop logic, so it would read past the announced boundary. The guard in
-- `ReadFromMergeTree` disables `PrefetchingConcat` whenever the setting is on and the step
-- carries `virtual_row_conversion`.
--
-- This test pins that the guard also holds for a `ReadFromMergeTree` step that is
-- *reconstructed* from another one (`createLocalParallelReplicasReadingStep` under
-- `parallel_replicas_local_plan = 1`). The reconstructed step must keep the full
-- read-in-order contract, including `virtual_row_conversion`
-- (see `copyReadInOrderContractFrom`), so `PrefetchingConcat` must stay absent
-- and the virtual-row optimization must stay visible on the rebuilt step.

DROP TABLE IF EXISTS t_vrow_pb_pr_local;

CREATE TABLE t_vrow_pb_pr_local (key UInt64, value String)
ENGINE = MergeTree PARTITION BY intDiv(key, 30000) ORDER BY key
SETTINGS index_granularity = 1024;

INSERT INTO t_vrow_pb_pr_local SELECT number, toString(number) FROM numbers(90000);
OPTIMIZE TABLE t_vrow_pb_pr_local FINAL;

SET optimize_read_in_order = 1;
SET read_in_order_use_virtual_row = 1;
SET read_in_order_use_virtual_row_per_block = 1;
SET max_threads = 4;
SET merge_tree_min_rows_for_concurrent_read = 1024, merge_tree_min_bytes_for_concurrent_read = 0, merge_tree_min_read_task_size = 2;

-- Baseline, purely local read: multiple parts would normally use per-part
-- `PrefetchingConcat`, but per-block virtual rows must disable it.
SELECT 'no_prefetching_local';
SELECT count() = 0 FROM (
    EXPLAIN PIPELINE SELECT * FROM t_vrow_pb_pr_local
    WHERE value LIKE '%5%'
    ORDER BY key
    SETTINGS enable_parallel_replicas = 0
) WHERE explain LIKE '%PrefetchingConcat%';

SET enable_analyzer = 1;
SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_local_plan = 1;

-- The local part of the parallel-replicas plan is a reconstructed `ReadFromMergeTree`.
-- `PrefetchingConcat` must stay absent there too, and the read must stay in order.
SELECT 'no_prefetching_parallel_replicas_local_plan';
SELECT
    count() FILTER (WHERE explain LIKE '%PrefetchingConcat%') = 0,
    count() FILTER (WHERE explain LIKE '%MergeTreeSelect(pool: ReadPoolParallelReplicasInOrder%') >= 1
FROM (EXPLAIN PIPELINE SELECT * FROM t_vrow_pb_pr_local WHERE value LIKE '%5%' ORDER BY key);

-- The rebuilt local step must still carry the virtual-row conversions.
SELECT 'virtual_row_on_rebuilt_step';
SELECT count() >= 1 FROM (
    EXPLAIN actions = 1 SELECT * FROM t_vrow_pb_pr_local WHERE value LIKE '%5%' ORDER BY key
) WHERE explain LIKE '%Virtual row conversions%';

-- Correctness: the output must arrive already sorted (do not re-sort here — that
-- would mask reordering), and cover every row exactly once.
SELECT 'correctness';
SELECT groupArray(key) = arraySort(groupArray(key)) FROM (
    SELECT key FROM t_vrow_pb_pr_local WHERE value LIKE '%5%' ORDER BY key
);
SELECT count() = 90000, sum(key) = 4049955000 FROM (
    SELECT key FROM t_vrow_pb_pr_local ORDER BY key
);

DROP TABLE t_vrow_pb_pr_local;
