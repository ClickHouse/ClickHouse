-- Tags: no-darwin, no-old-analyzer
-- Parallel FINAL via primary-key-range layers in the distributed query plan must equal local FINAL,
-- including when duplicate keys straddle layer borders. make_distributed_plan splits a FINAL read into
-- PK-range layers; each layer is deduplicated independently and the layers concatenate.

SET enable_parallel_replicas = 0, max_rows_to_group_by = 0, distributed_plan_default_reader_bucket_count = 4;

DROP TABLE IF EXISTS t_final_layers_rep;
CREATE TABLE t_final_layers_rep (k UInt64, v UInt64, ver UInt64) ENGINE = ReplacingMergeTree(ver) ORDER BY k
SETTINGS index_granularity = 256;
SYSTEM STOP MERGES t_final_layers_rep;
-- Partial overlap with small granules: keys [40000, 80000) appear in both parts (newer version wins),
-- so duplicate keys land near and across layer borders.
INSERT INTO t_final_layers_rep SELECT number, number, 1 FROM numbers(80000);
INSERT INTO t_final_layers_rep SELECT number + 40000, number + 2000000, 2 FROM numbers(80000);

SELECT 'Replacing local', count(), sum(v) FROM t_final_layers_rep FINAL SETTINGS make_distributed_plan = 0;
SELECT 'Replacing distributed', count(), sum(v) FROM t_final_layers_rep FINAL SETTINGS make_distributed_plan = 1;

-- The FINAL read itself must distribute (split into PK-range layers), not fall back to a serial read.
-- A bare SELECT (no aggregation to distribute on its own) is distributed only if the read is.
SELECT 'Replacing read distributes', countIf(explain LIKE '%ReadFromDistributedPlanSource%') > 0
FROM (EXPLAIN PIPELINE SELECT k, v FROM t_final_layers_rep FINAL SETTINGS make_distributed_plan = 1);

DROP TABLE IF EXISTS t_final_layers_col;
CREATE TABLE t_final_layers_col (k UInt64, v UInt64, sign Int8) ENGINE = CollapsingMergeTree(sign) ORDER BY k;
SYSTEM STOP MERGES t_final_layers_col;
INSERT INTO t_final_layers_col SELECT number, number, 1 FROM numbers(100000);
INSERT INTO t_final_layers_col SELECT number, number, -1 FROM numbers(50000);

SELECT 'Collapsing local', count(), sum(v) FROM t_final_layers_col FINAL SETTINGS make_distributed_plan = 0;
SELECT 'Collapsing distributed', count(), sum(v) FROM t_final_layers_col FINAL SETTINGS make_distributed_plan = 1;

DROP TABLE IF EXISTS t_final_layers_sum;
CREATE TABLE t_final_layers_sum (k UInt64, v UInt64) ENGINE = SummingMergeTree ORDER BY k;
SYSTEM STOP MERGES t_final_layers_sum;
INSERT INTO t_final_layers_sum SELECT number, 10 FROM numbers(100000);
INSERT INTO t_final_layers_sum SELECT number, 5 FROM numbers(100000);

SELECT 'Summing local', count(), sum(v) FROM t_final_layers_sum FINAL SETTINGS make_distributed_plan = 0;
SELECT 'Summing distributed', count(), sum(v) FROM t_final_layers_sum FINAL SETTINGS make_distributed_plan = 1;

DROP TABLE t_final_layers_rep;
DROP TABLE t_final_layers_col;
DROP TABLE t_final_layers_sum;
