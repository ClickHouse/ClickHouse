-- Tags: no-parallel-replicas
-- no-parallel-replicas: the test checks EXPLAIN of plans with reading steps replaced by prepared sources.

-- `min`/`max` skip `NaN` and return it only when every value is `NaN`, while a part whose values are
-- all `NaN` reports `NaN` as both of its extremes. Check that folding the per-part extrema of the
-- min/max aggregation shortcut follows the same rule, so that an all-`NaN` part does not poison the
-- answer of the parts around it.

DROP TABLE IF EXISTS t_nan_stats;

-- The default value of `auto_statistics_types` is spelled out instead of being left out, because the
-- test runner randomizes every MergeTree setting a test does not set itself.
CREATE TABLE t_nan_stats (key UInt64, f32 Float32, f64 Float64)
ENGINE = MergeTree ORDER BY key
SETTINGS auto_statistics_types = 'basic, uniq_v2', materialize_statistics_on_merge = 1;

SET optimize_use_projections = 1, optimize_use_implicit_projections = 1;
SET use_statistics_for_min_max_aggregation = 1;
SET materialize_statistics_on_insert = 1;
-- Pin every setting the optimization's eligibility depends on: the test runner randomizes
-- optimize_aggregation_in_order, and aggregation-in-order (like aggregate_functions_null_for_empty)
-- disables the projection optimization entirely, turning the EXPLAIN checks below into 0.
SET enable_analyzer = 1;
SET optimize_aggregation_in_order = 0, force_aggregation_in_order = 0;
SET aggregate_functions_null_for_empty = 0;

-- One part where every value is `NaN`, inserted first so that it is folded first,
-- and one part with finite values.
INSERT INTO t_nan_stats SELECT number, nan, nan FROM numbers(1000);
INSERT INTO t_nan_stats SELECT 1000 + number, toFloat32(number) + 1, toFloat64(number) + 1 FROM numbers(1000);

SELECT 'all parts are covered by statistics';
SELECT count() FROM (EXPLAIN actions = 1 SELECT min(f32), max(f32), min(f64), max(f64) FROM t_nan_stats) WHERE explain LIKE '%_statistics_min_max_projection%';

SELECT 'an all-NaN part next to a finite one';
SELECT min(f32), max(f32), min(f64), max(f64) FROM t_nan_stats;
SELECT 'the same values without the optimization';
SELECT min(f32), max(f32), min(f64), max(f64) FROM t_nan_stats SETTINGS use_statistics_for_min_max_aggregation = 0;

-- The reverse insertion order: the finite part is folded first and the all-`NaN` part second.
INSERT INTO t_nan_stats SELECT 2000 + number, nan, nan FROM numbers(1000);

SELECT 'an all-NaN part folded last';
SELECT min(f32), max(f32), min(f64), max(f64) FROM t_nan_stats;
SELECT 'the same values without the optimization';
SELECT min(f32), max(f32), min(f64), max(f64) FROM t_nan_stats SETTINGS use_statistics_for_min_max_aggregation = 0;

DROP TABLE t_nan_stats;

-- Every part is all-`NaN`: the answer must be `NaN`, as it is for a normal read.
CREATE TABLE t_nan_stats (key UInt64, f32 Float32, f64 Float64)
ENGINE = MergeTree ORDER BY key
SETTINGS auto_statistics_types = 'basic, uniq_v2', materialize_statistics_on_merge = 1;

INSERT INTO t_nan_stats SELECT number, nan, nan FROM numbers(1000);
INSERT INTO t_nan_stats SELECT 1000 + number, nan, nan FROM numbers(1000);

SELECT 'every part is all-NaN';
SELECT min(f32), max(f32), min(f64), max(f64) FROM t_nan_stats;
SELECT 'the same values without the optimization';
SELECT min(f32), max(f32), min(f64), max(f64) FROM t_nan_stats SETTINGS use_statistics_for_min_max_aggregation = 0;

-- A part without statistics (`materialize_statistics_on_insert` off) is read normally and merged
-- into the states produced from the covered all-`NaN` parts.
INSERT INTO t_nan_stats SELECT 2000 + number, toFloat32(number) + 1, toFloat64(number) + 1 FROM numbers(1000)
SETTINGS materialize_statistics_on_insert = 0;

SELECT 'a not covered finite part merged with covered all-NaN parts';
SELECT min(f32), max(f32), min(f64), max(f64) FROM t_nan_stats;
SELECT 'the same values without the optimization';
SELECT min(f32), max(f32), min(f64), max(f64) FROM t_nan_stats SETTINGS use_statistics_for_min_max_aggregation = 0;

DROP TABLE t_nan_stats;

-- The per-part statistics are not built at once: `MergeTask` and `MutateTask` feed the collectors
-- one output chunk at a time. Check a single part whose first chunks are all `NaN` and whose later
-- chunks are finite, so that the accumulator is not stuck at `NaN` in the materialized statistics.
DROP TABLE IF EXISTS t_nan_stats_merged;

CREATE TABLE t_nan_stats_merged (key UInt64, f32 Float32, f64 Float64)
ENGINE = MergeTree ORDER BY key
SETTINGS auto_statistics_types = 'basic, uniq_v2', materialize_statistics_on_merge = 1;

-- More rows than `merge_max_block_size`, so that the merged part really starts with all-`NaN` chunks.
INSERT INTO t_nan_stats_merged SELECT number, nan, nan FROM numbers(100000);
INSERT INTO t_nan_stats_merged SELECT 100000 + number, toFloat32(number) + 1, toFloat64(number) + 1 FROM numbers(100000);

OPTIMIZE TABLE t_nan_stats_merged FINAL;

SELECT 'the merged part is a single part covered by statistics';
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_nan_stats_merged' AND active;
SELECT count() FROM (EXPLAIN actions = 1 SELECT min(f32), max(f32), min(f64), max(f64) FROM t_nan_stats_merged) WHERE explain LIKE '%_statistics_min_max_projection%';

SELECT 'one part merged from an all-NaN prefix and a finite suffix';
SELECT min(f32), max(f32), min(f64), max(f64) FROM t_nan_stats_merged;
SELECT 'the same values without the optimization';
SELECT min(f32), max(f32), min(f64), max(f64) FROM t_nan_stats_merged SETTINGS use_statistics_for_min_max_aggregation = 0;

DROP TABLE t_nan_stats_merged;
