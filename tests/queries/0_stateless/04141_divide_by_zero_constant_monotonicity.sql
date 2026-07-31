-- Regression test: `divide(0, x)` must not claim monotonicity when the key range includes 0.
-- `0 / x` evaluates to 0 for `x` != 0 but is undefined at `x` = 0, so the function is not
-- monotonic across that boundary.
-- Previously caused LOGICAL_ERROR `Invalid binary search result in MergeTreeSetIndex` in
-- debug builds when an `IN`/`NOT IN` expression on the primary key wraps the key in `divide`
-- with a constant zero numerator. In release builds, `MergeTreeSetIndex::checkInRange` falls
-- back to the `{true, true}` BoolMask via the `#ifndef NDEBUG`/`#else` branch in
-- `src/Interpreters/Set.cpp`, so the user-visible query result and pruning effectiveness are
-- unchanged. The `Bugfix validation (functional tests)` job runs against a release master
-- binary and therefore cannot reproduce the original `LOGICAL_ERROR`.
-- https://github.com/ClickHouse/ClickHouse/issues/90461

DROP TABLE IF EXISTS t_divide_zero_mono;

CREATE TABLE t_divide_zero_mono (a UInt64, b String) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
INSERT INTO t_divide_zero_mono SELECT number, toString(number) FROM numbers(10);

-- Scalar form: range [0, 9] includes 0, so the chain `divide(0, a)` is non-monotonic.
SELECT count() FROM t_divide_zero_mono WHERE divide(0, a) NOT IN (1.0, 2.0);

-- Tuple form (matches the original AST fuzzer query that surfaced this bug).
SELECT count() FROM t_divide_zero_mono WHERE (divide(0, a), b) NOT IN ((1, 'x'), (2, 'y'));

-- The fold-from-constants form `divide(divide(0, c), a)` collapses to `divide(0, a)` and must
-- still be safe.
SELECT count() FROM t_divide_zero_mono WHERE (divide(divide(isNull(-2), assumeNotNull(7)), a), b)
    NOT IN ((9223372036854775806, '0.500000'), (2147483646, 'y'));

-- Sanity check: the legitimate non-zero case (`divide(1, a)`) must keep working
-- (here `a` ranges over [0, 9] which crosses 0, so the chain reports non-monotonic
-- and the binary search is skipped). `divide(1, 1)` = 1.0 excludes one row.
SELECT count() FROM t_divide_zero_mono WHERE divide(1, a) NOT IN (1.0, 2.0);

-- `intDiv(0, a)` regression: the monotonicity branch in
-- `FunctionBinaryArithmetic::getMonotonicityForRange` is shared between `divide` and
-- `intDiv`, but `intDiv` has different runtime semantics — `intDiv(0, 0)` raises
-- `ILLEGAL_DIVISION` instead of returning `NaN`/`Inf`. The index-analysis path is the
-- same, so the same fix applies. Pre-fix this query tripped the `Invalid binary
-- search result in MergeTreeSetIndex` exception at index-analysis time on debug
-- builds. Post-fix the index analysis safely reports non-monotonic and the runtime
-- evaluation of `intDiv(0, 0)` on row `a = 0` legitimately throws `ILLEGAL_DIVISION`.
SELECT count() FROM t_divide_zero_mono WHERE intDiv(0, a) NOT IN (1, 2); -- { serverError ILLEGAL_DIVISION }

-- Tuple form for `intDiv` (parallel with the `divide` tuple test above).
SELECT count() FROM t_divide_zero_mono WHERE (intDiv(0, a), b) NOT IN ((1, 'x'), (2, 'y')); -- { serverError ILLEGAL_DIVISION }

-- Sanity check: an entirely positive range must still allow the legitimate `c / x` monotonic
-- inference. `divide(2, a)` over [1, 10] is strictly decreasing, so KeyCondition can use it.
DROP TABLE t_divide_zero_mono;
CREATE TABLE t_divide_zero_mono (a UInt64, b String) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
INSERT INTO t_divide_zero_mono SELECT number + 1, toString(number) FROM numbers(10);
SELECT count() FROM t_divide_zero_mono WHERE divide(2, a) NOT IN (1.0, 2.0);

-- Sanity check: `intDiv(0, a)` over the strictly positive range [1, 10] is the legitimate
-- monotonic case (the function is constant 0 on a range that excludes 0). KeyCondition can
-- use it; `intDiv(0, [1..10])` is 0 for all rows, none equal `1` or `2`, all 10 rows match.
SELECT count() FROM t_divide_zero_mono WHERE intDiv(0, a) NOT IN (1, 2);

DROP TABLE t_divide_zero_mono;
