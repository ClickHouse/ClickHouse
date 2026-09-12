-- The `NaN`-endpoint guard on `divide`/`multiply` monotonicity also covers the `const / variable` and
-- `const * variable` branches, where the constant is the left operand. `inf / k` over a range whose right
-- end is `+inf` maps that endpoint to `NaN`; the transform is decreasing, so `KeyCondition` inverts the
-- range and the `NaN` becomes the left bound - the same shape that made index and statistics pruning drop
-- every part and granule and silently return no rows.

-- `EXPLAIN indexes = 1` below reads the local plan, so keep the plan local when the test runner enables
-- parallel replicas.
SET parallel_replicas_local_plan = 1;

-- Statistics pruning reads the column min/max that a `basic` statistic holds, and the test runner
-- randomizes both `auto_statistics_types` and `materialize_statistics_on_insert`. Pin them, otherwise a
-- run without materialized `basic` statistics never reaches the statistics pruner and the
-- `use_primary_key = 0` queries below cover nothing.
SET materialize_statistics_on_insert = 1;

DROP TABLE IF EXISTS t_monotonicity_nan_const_dividend;
CREATE TABLE t_monotonicity_nan_const_dividend (k Float64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = 'basic';
INSERT INTO t_monotonicity_nan_const_dividend VALUES (1), (2), (3), (inf);

SELECT 'the statistics pruner is live for this table';
-- A predicate that falls outside the part's `[1, +inf]` statistics range does prune the part, so the
-- statistics layer is reachable here; the queries below then show it stops pruning once an endpoint of the
-- transformed range becomes `NaN`.
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_monotonicity_nan_const_dividend WHERE k < 0) WHERE explain LIKE '%Statistics%' SETTINGS use_primary_key = 0;

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

DROP TABLE t_monotonicity_const_dividend_finite;
DROP TABLE t_monotonicity_nan_const_dividend;
