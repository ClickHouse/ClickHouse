-- Regression for the comparison key of the -ArgMin / -ArgMax combinators applied to a float-promoting base function
-- (sumArgMax, avgArgMin, corrArgMax, ...). tryGetProperties strips the combinator suffix, so these forms inherit the
-- base function's is_float_promoting flag. The Float64 fallback that this flag enables (a numeric mix with no lossless
-- common supertype is aggregated in Float64) must NOT be applied to the comparison key, which is compared exactly:
-- a lossy Float64 cast of a numeric-mix Variant key would silently collapse distinct integers above 2^53 and return
-- the wrong argMax/argMin row. A key with no lossless supertype must be rejected (like the base argMin / argMax),
-- while a key with a lossless supertype is still adapted, and a float-promoting Variant value argument is still
-- promoted to Float64.

SET allow_experimental_variant_type = 1;
SET allow_suspicious_variant_types = 1;

DROP TABLE IF EXISTS t_argminmax_key;
CREATE TABLE t_argminmax_key (v UInt64, k Variant(Int64, Float64)) ENGINE = Memory;
-- Two keys that are distinct as Int64 but collapse to the same value as Float64 (2^53 and 2^53 + 1).
INSERT INTO t_argminmax_key VALUES (10, CAST(9007199254740992 AS Int64)), (20, CAST(9007199254740993 AS Int64));

-- The numeric-mix key has no lossless common supertype, so it must be rejected rather than lossily cast to Float64.
-- Without the fix sumArgMax would collapse both keys to the same Float64 and sum both rows (30) instead of erroring.
SELECT sumArgMax(v, k) FROM t_argminmax_key; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT avgArgMin(v, k) FROM t_argminmax_key; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
-- The peel through an outer combinator (-State here) still suppresses the fallback for the key.
SELECT sumArgMaxState(v, k) FROM t_argminmax_key; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
-- The base argMax already rejects such a key; the combinator forms now behave consistently.
SELECT argMax(v, k) FROM t_argminmax_key; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
DROP TABLE t_argminmax_key;

-- A key with a lossless common supertype (UInt8, UInt64 -> UInt64) is still adapted and compared exactly, so the
-- combinator forms resolve and return the value for the true max / min key.
DROP TABLE IF EXISTS t_argminmax_key_lossless;
CREATE TABLE t_argminmax_key_lossless (v UInt64, k Variant(UInt8, UInt64)) ENGINE = Memory;
INSERT INTO t_argminmax_key_lossless VALUES (10, CAST(1 AS UInt8)), (20, CAST(300 AS UInt64)), (30, CAST(5 AS UInt8));
SELECT 'sumArgMax lossless key', sumArgMax(v, k) FROM t_argminmax_key_lossless;
SELECT 'sumArgMin lossless key', sumArgMin(v, k) FROM t_argminmax_key_lossless;
DROP TABLE t_argminmax_key_lossless;

-- The fix is scoped to the key: a float-promoting Variant value argument is still promoted to Float64 and summed,
-- with an ordinary exact key.
DROP TABLE IF EXISTS t_argminmax_value;
CREATE TABLE t_argminmax_value (v Variant(Int64, Float64), k UInt64) ENGINE = Memory;
INSERT INTO t_argminmax_value VALUES (CAST(10 AS Int64), 1), (CAST(2.5 AS Float64), 3), (CAST(100 AS Int64), 2);
SELECT 'sumArgMax float-promoting value', sumArgMax(v, k) AS r, toTypeName(r) FROM t_argminmax_value;
DROP TABLE t_argminmax_value;
