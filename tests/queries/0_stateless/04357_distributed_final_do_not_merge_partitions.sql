-- Tags: no-darwin, no-old-analyzer
-- Distributed FINAL must reproduce per-partition deduplication. When FINAL does not merge across
-- partitions, a primary-key value may repeat in several partitions and both copies must survive. The
-- distributed read splits a FINAL into primary-key-range layers per partition so a layer never merges
-- keys from different partitions, matching local FINAL.

SET enable_parallel_replicas = 0, max_rows_to_group_by = 0, distributed_plan_default_reader_bucket_count = 4;

-- Partition key is not part of the sort key, with do_not_merge_across_partitions_select_final = 1: every
-- key appears in both partitions and both copies must survive. Merging across partitions (the bug) would
-- keep only one copy per key, halving the row count.
DROP TABLE IF EXISTS t_dm_unrelated;
CREATE TABLE t_dm_unrelated (k UInt64, p UInt8, v UInt64, ver UInt64) ENGINE = ReplacingMergeTree(ver)
PARTITION BY p ORDER BY k SETTINGS index_granularity = 256;
SYSTEM STOP MERGES t_dm_unrelated;
INSERT INTO t_dm_unrelated SELECT number, 0, number, 1 FROM numbers(80000);
INSERT INTO t_dm_unrelated SELECT number, 0, number + 5, 3 FROM numbers(80000);
INSERT INTO t_dm_unrelated SELECT number, 1, number + 1000000, 2 FROM numbers(80000);

SELECT 'unrelated local', count(), sum(v) FROM t_dm_unrelated FINAL
SETTINGS make_distributed_plan = 0, do_not_merge_across_partitions_select_final = 1;
SELECT 'unrelated distributed', count(), sum(v) FROM t_dm_unrelated FINAL
SETTINGS make_distributed_plan = 1, do_not_merge_across_partitions_select_final = 1;

-- The read must distribute (one PK-range layer per partition), not fall back to a serial read.
-- A bare SELECT (no aggregation to distribute on its own) is distributed only if the read is.
SELECT 'unrelated read distributes', countIf(explain LIKE '%ReadFromDistributedPlanSource%') > 0
FROM (EXPLAIN PIPELINE SELECT k, v FROM t_dm_unrelated FINAL
      SETTINGS make_distributed_plan = 1, do_not_merge_across_partitions_select_final = 1);

DROP TABLE t_dm_unrelated;

-- Partition key is a function of the sort key, so a key is confined to one partition. The automatic
-- across-partition decision applies (default settings); merging is equivalent here and distributed FINAL
-- must still match local FINAL while distributing across partitions.
DROP TABLE IF EXISTS t_dm_derived;
CREATE TABLE t_dm_derived (k UInt64, v UInt64, ver UInt64) ENGINE = ReplacingMergeTree(ver)
PARTITION BY intDiv(k, 20000) ORDER BY k SETTINGS index_granularity = 256;
SYSTEM STOP MERGES t_dm_derived;
INSERT INTO t_dm_derived SELECT number, number, 1 FROM numbers(80000);
INSERT INTO t_dm_derived SELECT number, number + 7, 2 FROM numbers(80000);

SELECT 'derived local', count(), sum(v) FROM t_dm_derived FINAL SETTINGS make_distributed_plan = 0;
SELECT 'derived distributed', count(), sum(v) FROM t_dm_derived FINAL SETTINGS make_distributed_plan = 1;
SELECT 'derived read distributes', countIf(explain LIKE '%ReadFromDistributedPlanSource%') > 0
FROM (EXPLAIN PIPELINE SELECT k, v FROM t_dm_derived FINAL SETTINGS make_distributed_plan = 1);

DROP TABLE t_dm_derived;
