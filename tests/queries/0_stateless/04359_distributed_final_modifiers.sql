-- Tags: no-darwin, no-old-analyzer
-- Distributed parallel FINAL via primary-key-range layers must equal local FINAL when combined with a
-- WHERE, a PREWHERE, or a lightweight DELETE, and must still distribute the read.

SET enable_parallel_replicas = 0, max_rows_to_group_by = 0, distributed_plan_default_reader_bucket_count = 4;

DROP TABLE IF EXISTS t_mod;
CREATE TABLE t_mod (k UInt64, v UInt64, ver UInt64) ENGINE = ReplacingMergeTree(ver) ORDER BY k SETTINGS index_granularity = 256;
SYSTEM STOP MERGES t_mod;
INSERT INTO t_mod SELECT number, number, 1 FROM numbers(80000);
INSERT INTO t_mod SELECT number, number + 5, 2 FROM numbers(80000);

SELECT 'WHERE local', count(), sum(v) FROM t_mod FINAL WHERE k % 3 = 0 SETTINGS make_distributed_plan = 0;
SELECT 'WHERE distributed', count(), sum(v) FROM t_mod FINAL WHERE k % 3 = 0 SETTINGS make_distributed_plan = 1;
SELECT 'WHERE read distributes', countIf(explain LIKE '%ReadFromDistributedPlanSource%') > 0
FROM (EXPLAIN PIPELINE SELECT k, v FROM t_mod FINAL WHERE k % 3 = 0 SETTINGS make_distributed_plan = 1);

SELECT 'PREWHERE local', count(), sum(v) FROM t_mod FINAL PREWHERE k % 3 = 0 SETTINGS make_distributed_plan = 0;
SELECT 'PREWHERE distributed', count(), sum(v) FROM t_mod FINAL PREWHERE k % 3 = 0 SETTINGS make_distributed_plan = 1;
SELECT 'PREWHERE read distributes', countIf(explain LIKE '%ReadFromDistributedPlanSource%') > 0
FROM (EXPLAIN PIPELINE SELECT k, v FROM t_mod FINAL PREWHERE k % 3 = 0 SETTINGS make_distributed_plan = 1);

DROP TABLE t_mod;

-- Lightweight DELETE applied on the fly (read-time _row_exists mask, parts kept stable by STOP MERGES).
DROP TABLE IF EXISTS t_del;
CREATE TABLE t_del (k UInt64, v UInt64, ver UInt64) ENGINE = ReplacingMergeTree(ver) ORDER BY k SETTINGS index_granularity = 256;
SYSTEM STOP MERGES t_del;
INSERT INTO t_del SELECT number, number, 1 FROM numbers(80000);
INSERT INTO t_del SELECT number, number + 5, 2 FROM numbers(80000);
SET apply_mutations_on_fly = 1, lightweight_deletes_sync = 0;
DELETE FROM t_del WHERE k % 5 = 0;

SELECT 'delete local', count(), sum(v) FROM t_del FINAL SETTINGS make_distributed_plan = 0;
SELECT 'delete distributed', count(), sum(v) FROM t_del FINAL SETTINGS make_distributed_plan = 1;
SELECT 'delete read distributes', countIf(explain LIKE '%ReadFromDistributedPlanSource%') > 0
FROM (EXPLAIN PIPELINE SELECT k, v FROM t_del FINAL SETTINGS make_distributed_plan = 1);

DROP TABLE t_del;
