-- Tags: no-random-merge-tree-settings

-- Test that in-order reading with parallel replicas creates multiple splits
-- (reading streams) within each replica for intra-replica parallelism.
-- Uses a single merged part to ensure parallelism comes from splits, not from parts.

DROP TABLE IF EXISTS t_in_order_splits;

CREATE TABLE t_in_order_splits (a UInt64, b UInt64)
ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 128;

INSERT INTO t_in_order_splits SELECT number, number FROM numbers_mt(1e6);
OPTIMIZE TABLE t_in_order_splits FINAL;

SET enable_analyzer = 1;
SET optimize_read_in_order = 1;
SET max_threads = 4;
SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_local_plan = 1;

-- With a single part and 4 threads, the initiator should split the part's ranges
-- into 4 splits. On master (without split support), there would be only 1 source.
-- Count MergeTreeSelect sources to avoid EXPLAIN PIPELINE formatting differences.
SELECT
    'num_merging_sorted',
    count() FILTER (WHERE explain LIKE '%MergingSorted%') >= 1,
    'num_sources',
    sumIf(
        toUInt64OrDefault(extract(explain, '× (\d+)'), toUInt64(1)),
        explain LIKE '%MergeTreeSelect(pool: ReadPoolParallelReplicasInOrder%'
    ) AS num_sources,
    num_sources >= 4
FROM (EXPLAIN PIPELINE SELECT a FROM t_in_order_splits ORDER BY a);

DROP TABLE t_in_order_splits;
