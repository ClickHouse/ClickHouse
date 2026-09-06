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

-- The lazy FINAL rewrite replaces the reading step with reads that carry no per-bucket mark state, so
-- it must decline on a bucketed distributed read or every reader task returns the whole deduplicated
-- result. A larger table is needed here: on 80000 rows the FINAL read is not split into buckets, so
-- the arms below would pass without exercising the interaction at all.
-- `optimize_aggregation_in_order` is randomized by the test runner and makes the lazy FINAL rewrite
-- roughly two orders of magnitude slower on this shape (about 39s versus 0.2s here) without changing
-- any result. That is pre-existing behaviour of the rewrite and reproduces on master with
-- `make_distributed_plan = 0`, so pin it here to keep this file fast rather than assert around it.
SET optimize_aggregation_in_order = 0;

DROP TABLE IF EXISTS t_lazy;
CREATE TABLE t_lazy (k UInt64, v UInt64, ver UInt64) ENGINE = ReplacingMergeTree(ver) ORDER BY k SETTINGS index_granularity = 256;
SYSTEM STOP MERGES t_lazy;
INSERT INTO t_lazy SELECT number, number, 1 FROM numbers(400000);
INSERT INTO t_lazy SELECT number, number + 5, 2 FROM numbers(400000);

SELECT 'lazy WHERE local', count(), sum(v) FROM t_lazy FINAL WHERE k % 3 = 0 SETTINGS make_distributed_plan = 0;
SELECT 'lazy WHERE distributed buckets 2', count(), sum(v) FROM t_lazy FINAL WHERE k % 3 = 0
SETTINGS make_distributed_plan = 1, distributed_plan_default_reader_bucket_count = 2, query_plan_optimize_lazy_final = 1, min_filtered_ratio_for_lazy_final = 0.0;
SELECT 'lazy WHERE distributed buckets 4', count(), sum(v) FROM t_lazy FINAL WHERE k % 3 = 0
SETTINGS make_distributed_plan = 1, distributed_plan_default_reader_bucket_count = 4, query_plan_optimize_lazy_final = 1, min_filtered_ratio_for_lazy_final = 0.0;
SELECT 'lazy WHERE distributed buckets 8', count(), sum(v) FROM t_lazy FINAL WHERE k % 3 = 0
SETTINGS make_distributed_plan = 1, distributed_plan_default_reader_bucket_count = 8, query_plan_optimize_lazy_final = 1, min_filtered_ratio_for_lazy_final = 0.0;
SELECT 'lazy WHERE read distributes', countIf(explain LIKE '%ReadFromDistributedPlanSource%') > 0
FROM (EXPLAIN PIPELINE SELECT k, v FROM t_lazy FINAL WHERE k % 3 = 0
SETTINGS make_distributed_plan = 1, query_plan_optimize_lazy_final = 1, min_filtered_ratio_for_lazy_final = 0.0);

SELECT 'lazy PREWHERE distributed', count(), sum(v) FROM t_lazy FINAL PREWHERE k % 3 = 0
SETTINGS make_distributed_plan = 1, query_plan_optimize_lazy_final = 1, min_filtered_ratio_for_lazy_final = 0.0;
SELECT 'lazy PREWHERE read distributes', countIf(explain LIKE '%ReadFromDistributedPlanSource%') > 0
FROM (EXPLAIN PIPELINE SELECT k, v FROM t_lazy FINAL PREWHERE k % 3 = 0
SETTINGS make_distributed_plan = 1, query_plan_optimize_lazy_final = 1, min_filtered_ratio_for_lazy_final = 0.0);

-- Without a filter the rewrite declines on its own, so this arm must be unaffected by the guard.
SELECT 'lazy no filter local', count(), sum(v) FROM t_lazy FINAL SETTINGS make_distributed_plan = 0;
SELECT 'lazy no filter distributed', count(), sum(v) FROM t_lazy FINAL
SETTINGS make_distributed_plan = 1, query_plan_optimize_lazy_final = 1, min_filtered_ratio_for_lazy_final = 0.0;

-- The non-lazy distributed path must keep working: bucketing itself is correct.
SELECT 'lazy off distributed', count(), sum(v) FROM t_lazy FINAL WHERE k % 3 = 0
SETTINGS make_distributed_plan = 1, query_plan_optimize_lazy_final = 0;

DROP TABLE t_lazy;

-- The guard keys on the bucket count and not on the setting, so a local FINAL must still be rewritten
-- by lazy FINAL. A small table is enough and is used deliberately: the rewrite reads every part twice
-- (once to build the primary-key set, once to read the surviving rows), so running it over the larger
-- table above would multiply this file's runtime for no extra coverage.
DROP TABLE IF EXISTS t_lazy_local;
CREATE TABLE t_lazy_local (k UInt64, v UInt64, ver UInt64) ENGINE = ReplacingMergeTree(ver) ORDER BY k SETTINGS index_granularity = 256;
SYSTEM STOP MERGES t_lazy_local;
INSERT INTO t_lazy_local SELECT number, number, 1 FROM numbers(80000);
INSERT INTO t_lazy_local SELECT number, number + 5, 2 FROM numbers(80000);

SELECT 'lazy local still rewrites', countIf(explain LIKE '%LazyFinalKeyAnalysis%' OR explain LIKE '%LazyReadReplacingFinal%' OR explain LIKE '%LazilyUnordered%') > 0
FROM (EXPLAIN PLAN SELECT count() FROM t_lazy_local FINAL WHERE k % 3 = 0
SETTINGS make_distributed_plan = 0, query_plan_optimize_lazy_final = 1, min_filtered_ratio_for_lazy_final = 0.0);
SELECT 'lazy local result', count(), sum(v) FROM t_lazy_local FINAL WHERE k % 3 = 0
SETTINGS make_distributed_plan = 0, query_plan_optimize_lazy_final = 1, min_filtered_ratio_for_lazy_final = 0.0;

DROP TABLE t_lazy_local;
