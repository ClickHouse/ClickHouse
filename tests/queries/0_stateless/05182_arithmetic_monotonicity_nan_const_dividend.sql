-- The `NaN`-endpoint guard on `divide`/`multiply` monotonicity also covers the `const / variable` and
-- `const * variable` branches, where the constant is the left operand. `inf / k` over a range whose right
-- end is `+inf` maps that endpoint to `NaN`; the transform is decreasing, so `KeyCondition` inverts the
-- range and the `NaN` becomes the left bound - the same shape that made index and statistics pruning drop
-- every part and granule and silently return no rows.

-- `EXPLAIN indexes = 1` below reads the local plan, so keep the plan local when the test runner enables
-- parallel replicas, and pin the plan printer the `%Statistics%` / `Granules:` matches parse.
SET parallel_replicas_local_plan = 1;
SET explain_query_plan_default = 'legacy';

-- Statistics pruning reads the column min/max that a `basic` statistic holds, and the test runner
-- randomizes both `auto_statistics_types` and `materialize_statistics_on_insert`. Pin them, otherwise a
-- run without materialized `basic` statistics never reaches the statistics pruner and the
-- `use_primary_key = 0` queries below cover nothing.
SET materialize_statistics_on_insert = 1;

DROP TABLE IF EXISTS t_monotonicity_nan_const_dividend;
CREATE TABLE t_monotonicity_nan_const_dividend (k Float64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = 'basic';
INSERT INTO t_monotonicity_nan_const_dividend VALUES (1), (2), (3), (inf);

SELECT 'the statistics pruner analyzes a constant dividend over this key';
-- `100 / k` over `[1, +inf]` transforms to `[0, 100]`: finite at both ends, so the guard does not fire and
-- the transform stays monotonic. `> 200` cannot match, so the part must be pruned and `EXPLAIN` must report
-- a `Statistics` entry. Without this, a `const / variable` chain that the statistics pruner stopped
-- analyzing would make every `use_primary_key = 0` query below pass by full scan.
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_monotonicity_nan_const_dividend WHERE 100 / k > 200) WHERE explain LIKE '%Statistics%' SETTINGS use_primary_key = 0;

SELECT 'and keeps every part once an endpoint maps to NaN';
-- The pruner records an entry only when it drops a part, so the guard holding means no `Statistics` entry.
SELECT count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_monotonicity_nan_const_dividend WHERE inf / k = inf) WHERE explain LIKE '%Statistics%' SETTINGS use_primary_key = 0;

SELECT 'a constant dividend whose range endpoint maps to NaN';
SELECT count() FROM t_monotonicity_nan_const_dividend WHERE inf / k = inf;
SELECT count() FROM t_monotonicity_nan_const_dividend WHERE inf / k = inf SETTINGS use_primary_key = 0, use_statistics_for_part_pruning = 0;

SELECT 'each pruning layer on its own';
SELECT count() FROM t_monotonicity_nan_const_dividend WHERE inf / k = inf SETTINGS use_statistics_for_part_pruning = 0;
SELECT count() FROM t_monotonicity_nan_const_dividend WHERE inf / k = inf SETTINGS use_primary_key = 0;

SELECT 'a constant multiplier that maps the inf endpoint to NaN';
SELECT count() FROM t_monotonicity_nan_const_dividend WHERE 0 * k = 0;
SELECT count() FROM t_monotonicity_nan_const_dividend WHERE 0 * k = 0 SETTINGS use_primary_key = 0, use_statistics_for_part_pruning = 0;

SELECT 'pruning still applies with a constant dividend and a finite range';
DROP TABLE IF EXISTS t_monotonicity_const_dividend_finite;
CREATE TABLE t_monotonicity_const_dividend_finite (k Float64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1, auto_statistics_types = 'basic';
INSERT INTO t_monotonicity_const_dividend_finite SELECT number + 1 FROM numbers(100);
SELECT count() FROM t_monotonicity_const_dividend_finite WHERE 100 / k > 2;
SELECT count() FROM t_monotonicity_const_dividend_finite WHERE 100 / k > 2 SETTINGS use_primary_key = 0, use_statistics_for_part_pruning = 0;
-- Granule counts depend on settings the test runner randomizes, so compare the two numbers rather than
-- pinning them: the point is that some granules are still skipped when no endpoint becomes `NaN`.
SELECT toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')) < toUInt64OrZero(extract(explain, 'Granules: \\d+/(\\d+)')) AS granules_pruned
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_monotonicity_const_dividend_finite WHERE 100 / k > 2)
WHERE explain LIKE '%Granules: %/%';

SELECT 'the statistics pruner analyzes a constant multiplier over this key';
-- The `multiply` counterpart of the probe above needs a range without `+inf`, since `0 * inf` is exactly
-- the `NaN` the guard declines. Over `[1, 100]`, `0 * k` is the constant 0, so `= 5` cannot match and the
-- part must be pruned. (A non-zero multiplier would not do: the overflow check in
-- `FunctionBinaryArithmetic::getMonotonicityForRange` has no `Float` case and so always declines.)
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_monotonicity_const_dividend_finite WHERE 0 * k = 5) WHERE explain LIKE '%Statistics%' SETTINGS use_primary_key = 0;
SELECT count() FROM t_monotonicity_const_dividend_finite WHERE 0 * k = 5 SETTINGS use_primary_key = 0;
SELECT count() FROM t_monotonicity_const_dividend_finite WHERE 0 * k = 5 SETTINGS use_primary_key = 0, use_statistics_for_part_pruning = 0;

DROP TABLE t_monotonicity_const_dividend_finite;
DROP TABLE t_monotonicity_nan_const_dividend;
