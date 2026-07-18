-- Regression for the comparison key of the -ArgMin / -ArgMax combinators when they are themselves wrapped by an outer
-- combinator that appends its own trailing argument, such as -If (a condition column) or -Resample (a resampling key).
-- The key of the -ArgMin / -ArgMax combinator is the last argument of that combinator's own call, which is no longer
-- the last top-level argument once such an outer combinator adds a trailing one: in sumArgMaxIf(v, k, cond) the real
-- -ArgMax key is k, not cond. The lossy Float64 fallback (enabled by the base function's is_float_promoting flag) must
-- be suppressed for k and not for cond -- otherwise a numeric-mix Variant key collapses distinct integers above 2^53
-- to the same Float64 and returns the wrong argMax / argMin row instead of being rejected.

SET allow_experimental_variant_type = 1;
SET allow_suspicious_variant_types = 1;

DROP TABLE IF EXISTS t_argminmax_outer_key;
-- Two keys that are distinct as Int64 but collapse to the same value as Float64 (2^53 and 2^53 + 1).
CREATE TABLE t_argminmax_outer_key (v UInt64, k Variant(Int64, Float64), cond UInt8) ENGINE = Memory;
INSERT INTO t_argminmax_outer_key VALUES (10, CAST(9007199254740992 AS Int64), 1), (20, CAST(9007199254740993 AS Int64), 1);

-- The -ArgMax / -ArgMin key is k (the second-to-last top-level argument), so the numeric-mix key must be rejected even
-- though an -If condition follows it. Without the fix the fallback would hit k, collapse both keys to the same Float64
-- and sum both rows (30) instead of erroring.
SELECT sumArgMaxIf(v, k, cond) FROM t_argminmax_outer_key; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT sumArgMinIf(v, k, cond) FROM t_argminmax_outer_key; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
-- -Resample also appends a trailing (resampling key) argument after the -ArgMax key, so k is likewise protected.
SELECT sumArgMaxResample(0, 2, 1)(v, k, 0) FROM t_argminmax_outer_key; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
DROP TABLE t_argminmax_outer_key;

-- A key with a lossless common supertype (UInt8, UInt64 -> UInt64) is still adapted and compared exactly through the
-- outer -If combinator, so the forms resolve and return the value for the true max / min key among the filtered rows.
DROP TABLE IF EXISTS t_argminmax_outer_key_lossless;
CREATE TABLE t_argminmax_outer_key_lossless (v UInt64, k Variant(UInt8, UInt64), cond UInt8) ENGINE = Memory;
INSERT INTO t_argminmax_outer_key_lossless VALUES (10, CAST(1 AS UInt8), 1), (20, CAST(300 AS UInt64), 1), (30, CAST(5 AS UInt8), 0);
SELECT 'sumArgMaxIf lossless key', sumArgMaxIf(v, k, cond) FROM t_argminmax_outer_key_lossless;
SELECT 'sumArgMinIf lossless key', sumArgMinIf(v, k, cond) FROM t_argminmax_outer_key_lossless;
DROP TABLE t_argminmax_outer_key_lossless;

-- The fix is scoped to the key: a float-promoting Variant value argument is still promoted to Float64 and summed under
-- the -If combinator, with an ordinary exact key and a following condition.
DROP TABLE IF EXISTS t_argminmax_outer_value;
CREATE TABLE t_argminmax_outer_value (v Variant(Int64, Float64), k UInt64, cond UInt8) ENGINE = Memory;
INSERT INTO t_argminmax_outer_value VALUES (CAST(10 AS Int64), 1, 1), (CAST(2.5 AS Float64), 3, 1), (CAST(100 AS Int64), 2, 0);
SELECT 'sumArgMaxIf float-promoting value', sumArgMaxIf(v, k, cond) AS r, toTypeName(r) FROM t_argminmax_outer_value;
DROP TABLE t_argminmax_outer_value;
