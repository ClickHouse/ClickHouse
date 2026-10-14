-- Tags: no-darwin, no-old-analyzer
-- Distributed parallel FINAL via primary-key-range layers must equal local FINAL for the merging engines
-- with extra state columns -- aggregate states (AggregatingMergeTree) and sign + version
-- (VersionedCollapsingMergeTree) -- and must distribute the read, not fall back to a serial read.

SET enable_parallel_replicas = 0, max_rows_to_group_by = 0, distributed_plan_default_reader_bucket_count = 4;

-- AggregatingMergeTree: each key carries partial aggregate states that FINAL merges across the parts.
DROP TABLE IF EXISTS t_agg;
CREATE TABLE t_agg (k UInt64, s AggregateFunction(sum, UInt64), m AggregateFunction(max, UInt64))
ENGINE = AggregatingMergeTree ORDER BY k SETTINGS index_granularity = 256;
SYSTEM STOP MERGES t_agg;
INSERT INTO t_agg SELECT number, sumState(toUInt64(number)), maxState(toUInt64(number)) FROM numbers(80000) GROUP BY number;
INSERT INTO t_agg SELECT number, sumState(toUInt64(number + 1)), maxState(toUInt64(number + 1)) FROM numbers(80000) GROUP BY number;

SELECT 'Aggregating local', count(), sum(finalizeAggregation(s)), sum(finalizeAggregation(m)) FROM t_agg FINAL SETTINGS make_distributed_plan = 0;
SELECT 'Aggregating distributed', count(), sum(finalizeAggregation(s)), sum(finalizeAggregation(m)) FROM t_agg FINAL SETTINGS make_distributed_plan = 1;
SELECT 'Aggregating read distributes', countIf(explain LIKE '%ReadFromDistributedPlanSource%') > 0
FROM (EXPLAIN PIPELINE SELECT k FROM t_agg FINAL SETTINGS make_distributed_plan = 1);

DROP TABLE t_agg;

-- VersionedCollapsingMergeTree: sign cancels rows and version orders them within the merge.
DROP TABLE IF EXISTS t_vc;
CREATE TABLE t_vc (k UInt64, v UInt64, sign Int8, version UInt64) ENGINE = VersionedCollapsingMergeTree(sign, version)
ORDER BY k SETTINGS index_granularity = 256;
SYSTEM STOP MERGES t_vc;
INSERT INTO t_vc SELECT number, number, 1, 1 FROM numbers(80000);
INSERT INTO t_vc SELECT number, number, -1, 1 FROM numbers(40000);
INSERT INTO t_vc SELECT number, number + 100, 1, 2 FROM numbers(40000);

SELECT 'Versioned local', count(), sum(v) FROM t_vc FINAL SETTINGS make_distributed_plan = 0;
SELECT 'Versioned distributed', count(), sum(v) FROM t_vc FINAL SETTINGS make_distributed_plan = 1;
SELECT 'Versioned read distributes', countIf(explain LIKE '%ReadFromDistributedPlanSource%') > 0
FROM (EXPLAIN PIPELINE SELECT k, v FROM t_vc FINAL SETTINGS make_distributed_plan = 1);

DROP TABLE t_vc;
