-- Tags: no-darwin, no-old-analyzer
-- Distributed FINAL skips the merge for non-intersecting primary-key ranges. Each table below has one
-- level>0 (merged, deduplicated) part covering the whole key range plus a small overlapping level-0 part,
-- so FINAL reads the non-overlapping tail without a merge (only the engine sign/is_deleted filter) and
-- merges the overlapping head. The distributed result must equal local FINAL for every engine whose
-- non-intersecting path needs a filter: plain Replacing, Replacing + is_deleted, and Collapsing.

-- Keep the data small (the OPTIMIZE FINAL merges dominate the runtime) but force bucketing regardless of
-- size (distributed_plan_max_rows_to_broadcast = 0, max_final_threads = 1) so the distributed FINAL still
-- splits into the intersecting and non-intersecting lanes this test targets instead of being broadcast.
SET enable_parallel_replicas = 0, max_rows_to_group_by = 0, distributed_plan_default_reader_bucket_count = 4,
    distributed_plan_max_rows_to_broadcast = 0, max_final_threads = 1;

DROP TABLE IF EXISTS t_ni_rep;
CREATE TABLE t_ni_rep (k UInt64, v UInt64, ver UInt64) ENGINE = ReplacingMergeTree(ver) ORDER BY k SETTINGS index_granularity = 256;
INSERT INTO t_ni_rep SELECT number, number, 1 FROM numbers(20000);
INSERT INTO t_ni_rep SELECT number, number, 1 FROM numbers(20000);
OPTIMIZE TABLE t_ni_rep FINAL;
SYSTEM STOP MERGES t_ni_rep;
INSERT INTO t_ni_rep SELECT number, number + 5, 2 FROM numbers(4000);

SELECT 'rep local', count(), sum(v) FROM t_ni_rep FINAL SETTINGS make_distributed_plan = 0;
SELECT 'rep distributed', count(), sum(v) FROM t_ni_rep FINAL SETTINGS make_distributed_plan = 1;
SELECT 'rep read distributes', countIf(explain LIKE '%ReadFromDistributedPlanSource%') > 0
FROM (EXPLAIN PIPELINE SELECT k, v FROM t_ni_rep FINAL SETTINGS make_distributed_plan = 1);
DROP TABLE t_ni_rep;

DROP TABLE IF EXISTS t_ni_del;
CREATE TABLE t_ni_del (k UInt64, v UInt64, ver UInt64, is_deleted UInt8) ENGINE = ReplacingMergeTree(ver, is_deleted) ORDER BY k SETTINGS index_granularity = 256;
INSERT INTO t_ni_del SELECT number, number, 1, number % 7 = 0 FROM numbers(20000);
INSERT INTO t_ni_del SELECT number, number, 1, number % 7 = 0 FROM numbers(20000);
OPTIMIZE TABLE t_ni_del FINAL;
SYSTEM STOP MERGES t_ni_del;
INSERT INTO t_ni_del SELECT number, number + 5, 2, 0 FROM numbers(4000);

SELECT 'del local', count(), sum(v) FROM t_ni_del FINAL SETTINGS make_distributed_plan = 0;
SELECT 'del distributed', count(), sum(v) FROM t_ni_del FINAL SETTINGS make_distributed_plan = 1;
DROP TABLE t_ni_del;

DROP TABLE IF EXISTS t_ni_col;
CREATE TABLE t_ni_col (k UInt64, v UInt64, sign Int8) ENGINE = CollapsingMergeTree(sign) ORDER BY k SETTINGS index_granularity = 256;
INSERT INTO t_ni_col SELECT number, number, 1 FROM numbers(20000);
INSERT INTO t_ni_col SELECT number, number, -1 FROM numbers(10000);
OPTIMIZE TABLE t_ni_col FINAL;
SYSTEM STOP MERGES t_ni_col;
INSERT INTO t_ni_col SELECT number, number, 1 FROM numbers(4000);

SELECT 'col local', count(), sum(v) FROM t_ni_col FINAL SETTINGS make_distributed_plan = 0;
SELECT 'col distributed', count(), sum(v) FROM t_ni_col FINAL SETTINGS make_distributed_plan = 1;
DROP TABLE t_ni_col;
