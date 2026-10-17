-- Regression test for the -Sparkbar combinator cross-variant merge path
-- (mergeStateFromDifferentVariant). A state produced in a window context
-- (SELECT ...SparkbarState(...) OVER ()) stores a different aggregate function
-- variant than the one used by ...SparkbarMerge; finalizing it must go through
-- mergeStateFromDifferentVariant for every per-bucket sub-state.
-- Mirrors 04079_crosstab_window_state_variant_cast.sql for the combinator.

SET enable_analyzer = 1;

-- Use cramersV as the nested function: it returns a numeric (Float64) result and
-- has a non-trivial state with a distinct window variant, so the per-bucket
-- cross-variant merge is exercised.
-- cramersVSparkbar(width, begin_x, end_x)(x_col, a, b): x_col is the bucket key,
-- a and b are forwarded to cramersV. The (width, begin_x, end_x) parameters are
-- structural (they determine the per-bucket state layout) and must be repeated on
-- -State and -Merge, the same way -Merge re-specifies a quantile level.

DROP TABLE IF EXISTS test_sparkbar_variant;
CREATE TABLE test_sparkbar_variant (s AggregateFunction(cramersVSparkbar(5, 0, 4), UInt8, UInt8, UInt8)) ENGINE = MergeTree ORDER BY tuple();

-- Window-context state stored into the AggregateFunction column.
INSERT INTO test_sparkbar_variant
SELECT cramersVSparkbarState(5, 0, 4)(toUInt8(number % 5), toUInt8(number % 10), toUInt8(number % 6)) OVER ()
FROM numbers(100) LIMIT 1;

-- Finalize via the cross-variant merge path.
SELECT 'window state -> Merge:';
SELECT cramersVSparkbarMerge(5, 0, 4)(s) FROM test_sparkbar_variant;

-- The result must match direct aggregation.
SELECT 'direct aggregation:';
SELECT cramersVSparkbar(5, 0, 4)(toUInt8(number % 5), toUInt8(number % 10), toUInt8(number % 6)) FROM numbers(100);

-- AggregatingMergeTree: merge of two parts, both inserted from window states.
DROP TABLE IF EXISTS test_sparkbar_amt;
CREATE TABLE test_sparkbar_amt (k UInt8, s AggregateFunction(cramersVSparkbar(5, 0, 4), UInt8, UInt8, UInt8)) ENGINE = AggregatingMergeTree ORDER BY k;
INSERT INTO test_sparkbar_amt SELECT 1, cramersVSparkbarState(5, 0, 4)(toUInt8(number % 5), toUInt8(number % 10), toUInt8(number % 6)) OVER () FROM numbers(100) LIMIT 1;
INSERT INTO test_sparkbar_amt SELECT 1, cramersVSparkbarState(5, 0, 4)(toUInt8(number % 5), toUInt8(number % 8), toUInt8(number % 3)) OVER () FROM numbers(50) LIMIT 1;
OPTIMIZE TABLE test_sparkbar_amt FINAL;
SELECT 'AggregatingMergeTree merge of two window-state parts:';
SELECT cramersVSparkbarMerge(5, 0, 4)(s) FROM test_sparkbar_amt;

DROP TABLE test_sparkbar_variant;
DROP TABLE test_sparkbar_amt;
