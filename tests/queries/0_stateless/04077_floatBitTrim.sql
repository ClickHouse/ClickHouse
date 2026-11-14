SELECT 'Identity: trimming zero bits does nothing';
SELECT floatBitTrim(1.234::Float64, 0) = 1.234::Float64;
SELECT floatBitTrim(1.234::Float32, 0) = 1.234::Float32;
SELECT floatBitTrim(1.5::BFloat16, 0) = 1.5::BFloat16;

SELECT 'Low bits of the bit representation are zero after trimming.';
SELECT bin(reinterpretAsUInt64(floatBitTrim(1.234567890123::Float64, 30)));
SELECT bin(reinterpretAsUInt32(floatBitTrim(1.234::Float32, 10)));
SELECT bitAnd(reinterpretAsUInt16(floatBitTrim(1.234::BFloat16, 4)), bitShiftLeft(1::UInt16, 4) - 1);

SELECT 'Sign and exponent are preserved (clamped to mantissa width).';
SELECT floatBitTrim(3.14::Float64, 100) = floatBitTrim(3.14::Float64, 52);
SELECT floatBitTrim(-3.14::Float64, 100) = floatBitTrim(-3.14::Float64, 52);
SELECT floatBitTrim(3.14::Float32, 100) = floatBitTrim(3.14::Float32, 23);
SELECT floatBitTrim(-3.14::Float32, 100) = floatBitTrim(-3.14::Float32, 23);
SELECT floatBitTrim(3.14::BFloat16, 100) = floatBitTrim(3.14::BFloat16, 7);
SELECT floatBitTrim(-3.14::BFloat16, 100) = floatBitTrim(-3.14::BFloat16, 7);

SELECT 'Result type matches input float type.';
SELECT toTypeName(floatBitTrim(1.0::Float64, 5));
SELECT toTypeName(floatBitTrim(1.0::Float32, 5));
SELECT toTypeName(floatBitTrim(1.0::BFloat16, 3));

SELECT 'Selected n from 0 to past the mantissa width (Float64).';
SELECT 0 AS n, bin(reinterpretAsUInt64(floatBitTrim(1.9999999999999998::Float64, 0)));
SELECT 1 AS n, bin(reinterpretAsUInt64(floatBitTrim(1.9999999999999998::Float64, 1)));
SELECT 2 AS n, bin(reinterpretAsUInt64(floatBitTrim(1.9999999999999998::Float64, 2)));
SELECT 8 AS n, bin(reinterpretAsUInt64(floatBitTrim(1.9999999999999998::Float64, 8)));
SELECT 16 AS n, bin(reinterpretAsUInt64(floatBitTrim(1.9999999999999998::Float64, 16)));
SELECT 30 AS n, bin(reinterpretAsUInt64(floatBitTrim(1.9999999999999998::Float64, 30)));
SELECT 52 AS n, bin(reinterpretAsUInt64(floatBitTrim(1.9999999999999998::Float64, 52)));
SELECT 100 AS n, bin(reinterpretAsUInt64(floatBitTrim(1.9999999999999998::Float64, 100)));

SELECT 'Selected n from 0 to past the mantissa width (Float32).';
SELECT 0 AS n, bin(reinterpretAsUInt32(floatBitTrim(1.9999998807907104::Float32, 0)));
SELECT 1 AS n, bin(reinterpretAsUInt32(floatBitTrim(1.9999998807907104::Float32, 1)));
SELECT 2 AS n, bin(reinterpretAsUInt32(floatBitTrim(1.9999998807907104::Float32, 2)));
SELECT 8 AS n, bin(reinterpretAsUInt32(floatBitTrim(1.9999998807907104::Float32, 8)));
SELECT 16 AS n, bin(reinterpretAsUInt32(floatBitTrim(1.9999998807907104::Float32, 16)));
SELECT 20 AS n, bin(reinterpretAsUInt32(floatBitTrim(1.9999998807907104::Float32, 20)));
SELECT 23 AS n, bin(reinterpretAsUInt32(floatBitTrim(1.9999998807907104::Float32, 23)));
SELECT 100 AS n, bin(reinterpretAsUInt32(floatBitTrim(1.9999998807907104::Float32, 100)));

SELECT 'Selected n from 0 to past the mantissa width (BFloat16).';
SELECT 0 AS n, bin(reinterpretAsUInt16(floatBitTrim(1.9921875::BFloat16, 0)));
SELECT 1 AS n, bin(reinterpretAsUInt16(floatBitTrim(1.9921875::BFloat16, 1)));
SELECT 2 AS n, bin(reinterpretAsUInt16(floatBitTrim(1.9921875::BFloat16, 2)));
SELECT 4 AS n, bin(reinterpretAsUInt16(floatBitTrim(1.9921875::BFloat16, 4)));
SELECT 6 AS n, bin(reinterpretAsUInt16(floatBitTrim(1.9921875::BFloat16, 6)));
SELECT 7 AS n, bin(reinterpretAsUInt16(floatBitTrim(1.9921875::BFloat16, 7)));
SELECT 8 AS n, bin(reinterpretAsUInt16(floatBitTrim(1.9921875::BFloat16, 8)));
SELECT 100 AS n, bin(reinterpretAsUInt16(floatBitTrim(1.9921875::BFloat16, 100)));

-- Errors.
SELECT floatBitTrim('a', 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT floatBitTrim(1.0::Float64, 'a'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
-- A signed `n` is accepted by type, but a negative value is rejected during analysis.
SELECT floatBitTrim(1.0::Float64, -1); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT floatBitTrim(1.0::Float64, -1::Int64); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT floatBitTrim(materialize(1.0::Float64), 10 - 20); -- { serverError ARGUMENT_OUT_OF_BOUND }

SELECT 'NaN passes through unchanged.';
SELECT isNaN(floatBitTrim(nan::Float64, 0));
SELECT isNaN(floatBitTrim(nan::Float32, 0));
SELECT bin(reinterpretAsUInt16(floatBitTrim(nan::BFloat16, 0)));

SELECT 'NaN must not collapse to Inf when trimmed.';
SELECT isNaN(floatBitTrim(reinterpretAsFloat64(reinterpretAsUInt64(nan::Float64) + 1), 52));
SELECT isNaN(floatBitTrim(reinterpretAsFloat32(reinterpretAsUInt32(nan::Float32) + 1), 23));
-- Document that the Float32 -> BFloat16 cast actually produced the NaN bit
SELECT hex(reinterpretAsUInt16(CAST(reinterpretAsFloat32(toUInt32(0x7F810000)) AS BFloat16)));
SELECT bin(reinterpretAsUInt16(floatBitTrim(
    CAST(reinterpretAsFloat32(toUInt32(0x7F810000)) AS BFloat16), 7
)));

SELECT 'Sign bit of NaN is preserved.';
SELECT reinterpretAsUInt64(floatBitTrim(-nan::Float64, 52)) = reinterpretAsUInt64(-nan::Float64);
SELECT reinterpretAsUInt32(floatBitTrim(-nan::Float32, 23)) = reinterpretAsUInt32(-nan::Float32);

SELECT 'Full payload is preserved.';
SELECT reinterpretAsUInt64(floatBitTrim(reinterpretAsFloat64(toUInt64(0x7FF8000000000001)), 52)) = 0x7FF8000000000001;
SELECT reinterpretAsUInt32(floatBitTrim(reinterpretAsFloat32(toUInt32(0x7FC00001)), 23)) = 0x7FC00001;
SELECT reinterpretAsUInt32(floatBitTrim(reinterpretAsFloat32(toUInt32(0x7F800001)), 23)) = 0x7F800001;
SELECT hex(reinterpretAsUInt16(CAST(reinterpretAsFloat32(toUInt32(0x7FC10000)) AS BFloat16)));
SELECT hex(reinterpretAsUInt16(floatBitTrim(
    CAST(reinterpretAsFloat32(toUInt32(0x7FC10000)) AS BFloat16), 7
)));

SELECT 'Inf stays Inf.';
SELECT isInfinite(floatBitTrim(inf::Float64, 52));
SELECT isInfinite(floatBitTrim(-inf::Float32, 23));
SELECT hex(reinterpretAsUInt16(floatBitTrim(inf::BFloat16, 7)));

SELECT 'Large UInt64 n is clamped.';
SELECT floatBitTrim(materialize(1.0::Float64), 18446744073709551615) = floatBitTrim(1.0::Float64, 18446744073709551615);
SELECT floatBitTrim(materialize(1.0::Float64), 18446744073709551615) = floatBitTrim(1.0::Float64, 52);
SELECT floatBitTrim(materialize(1.234::Float32), 9223372036854775808) = floatBitTrim(1.234::Float32, 23);
SELECT floatBitTrim(materialize(1.5::BFloat16), 18446744073709551615) = floatBitTrim(1.5::BFloat16, 7);

SELECT 'Every native integer type is accepted for n.';
SELECT floatBitTrim(materialize(1.234::Float64), 20::UInt8) = floatBitTrim(1.234::Float64, 20);
SELECT floatBitTrim(materialize(1.234::Float64), 20::UInt16) = floatBitTrim(1.234::Float64, 20);
SELECT floatBitTrim(materialize(1.234::Float64), 20::UInt32) = floatBitTrim(1.234::Float64, 20);
SELECT floatBitTrim(materialize(1.234::Float64), 20::UInt64) = floatBitTrim(1.234::Float64, 20);
SELECT floatBitTrim(materialize(1.234::Float64), 20::Int8) = floatBitTrim(1.234::Float64, 20);
SELECT floatBitTrim(materialize(1.234::Float64), 20::Int16) = floatBitTrim(1.234::Float64, 20);
SELECT floatBitTrim(materialize(1.234::Float64), 20::Int32) = floatBitTrim(1.234::Float64, 20);
SELECT floatBitTrim(materialize(1.234::Float64), 20::Int64) = floatBitTrim(1.234::Float64, 20);

SELECT 'A non-constant n is rejected.';
SELECT floatBitTrim(materialize(1.234::Float64), materialize(20::UInt8)); -- { serverError ILLEGAL_COLUMN }
SELECT floatBitTrim(v, n) FROM (SELECT 1.0::Float64 AS v, number::UInt8 AS n FROM numbers(3)); -- { serverError ILLEGAL_COLUMN }
CREATE VIEW 04077_v AS SELECT floatBitTrim(x, n) FROM (SELECT 1.0::Float64 AS x, 1::UInt8 AS n); -- { serverError ILLEGAL_COLUMN }
-- A `NULL` constant is resolved to `Nullable(Nothing)` without the function being asked for a return
-- type, so this one is rejected at execution time rather than during analysis.
SELECT floatBitTrim(1.0::Float64, materialize(NULL)); -- { serverError ILLEGAL_COLUMN }

SELECT 'A Dynamic first argument resolves the function at execution time.';
-- The `Dynamic` adaptor drops the argument columns and hands the function the full non-constant
-- column, so a non-constant `n` must be reported there the same way as on the plain path --
-- including when its first row would pass the non-negative check.
SELECT floatBitTrim(d, n) FROM (SELECT number::Float64::Dynamic AS d, number::UInt8 AS n FROM numbers(3)); -- { serverError ILLEGAL_COLUMN }
SELECT floatBitTrim(d, n) FROM (SELECT number::Float64::Dynamic AS d, (-1 - number)::Int32 AS n FROM numbers(3)); -- { serverError ILLEGAL_COLUMN }
SELECT floatBitTrim(d, -1) FROM (SELECT number::Float64::Dynamic AS d FROM numbers(3)); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT floatBitTrim(d, 20)::Float64 = floatBitTrim(1.234::Float64, 20) FROM (SELECT 1.234::Float64::Dynamic AS d);

SELECT 'An expression that folds to a constant is accepted.';
SELECT floatBitTrim(materialize(1.234::Float64), 10 + 10) = floatBitTrim(1.234::Float64, 20);
SELECT floatBitTrim(materialize(1.234::Float64), 30 - 10) = floatBitTrim(1.234::Float64, 20);

SELECT 'A pruned branch of `if` hides an invalid n.';
-- A branch of `if` with a constant false condition is pruned before the function is resolved,
-- so the negative `n` is never seen and no exception is thrown.
SELECT if(0, floatBitTrim(materialize(1.0::Float64), -1), 1.0::Float64);

SELECT 'A NULL argument gives NULL.';
SELECT floatBitTrim(1.0::Float64, NULL), toTypeName(floatBitTrim(1.0::Float64, NULL));
SELECT floatBitTrim(NULL, 20), toTypeName(floatBitTrim(NULL, 20));
SELECT floatBitTrim(materialize(1.0::Float64), CAST(NULL, 'Nullable(UInt64)')),
       toTypeName(floatBitTrim(materialize(1.0::Float64), CAST(NULL, 'Nullable(UInt64)')));
SELECT floatBitTrim(CAST(NULL, 'Nullable(Float64)'), 20);
SELECT number, floatBitTrim(if(number % 2, NULL, 1.234::Float64), 20) FROM numbers(4) ORDER BY number;

SELECT 'Nullable arguments agree with the non-nullable path.';
SELECT toTypeName(floatBitTrim(toNullable(1.0::Float64), 20));
SELECT floatBitTrim(toNullable(1.234::Float64), 20) = floatBitTrim(1.234::Float64, 20);
SELECT floatBitTrim(toNullable(materialize(1.234::Float64)), toNullable(20::UInt64)) = floatBitTrim(1.234::Float64, 20);
SELECT floatBitTrim(materialize(1.234::Float32), toNullable(10::UInt64)) = floatBitTrim(1.234::Float32, 10);
SELECT floatBitTrim(materialize(1.5::BFloat16), toNullable(3::UInt64)) = floatBitTrim(1.5::BFloat16, 3);
SELECT floatBitTrim(toLowCardinality(toNullable(1.234::Float64)), 20) = floatBitTrim(1.234::Float64, 20);
SELECT floatBitTrim(materialize(1.0::Float64), toNullable(18446744073709551615::UInt64)) = floatBitTrim(1.0::Float64, 52);

SELECT 'NaN passthrough across many rows.';
SELECT sum(isNaN(floatBitTrim(materialize(nan::Float64), 30))) FROM numbers(100);
SELECT sum(isNaN(floatBitTrim(materialize(nan::Float32), 15))) FROM numbers(100);

SELECT 'Subnormals behavior.';
SELECT floatBitTrim(reinterpretAsFloat64(toUInt64(1)), 1) = 0.0;
SELECT floatBitTrim(reinterpretAsFloat64(toUInt64(3)), 1) = reinterpretAsFloat64(toUInt64(2));
SELECT floatBitTrim(reinterpretAsFloat32(toUInt32(1)), 1) = 0.0::Float32;

-- The vectorized kernel is checked against an independent bitmask oracle over many rows,
-- covering finite values, NaN, Inf and subnormals.
SELECT 'Vectorized kernel matches a bitmask oracle (Float64).';
SELECT count(), countIf(reinterpretAsUInt64(floatBitTrim(v, 20)) != if(
    isNaN(v), reinterpretAsUInt64(v), bitAnd(reinterpretAsUInt64(v), 0xFFFFFFFFFFF00000)))
FROM (
    SELECT arrayJoin([
        nan::Float64,
        -nan::Float64,
        inf::Float64,
        -inf::Float64,
        reinterpretAsFloat64(toUInt64(0x7FF8000000000001)),
        reinterpretAsFloat64(toUInt64(1)),
        reinterpretAsFloat64(toUInt64(0x000FFFFFFFFFFFFF)),
        0.0::Float64,
        -0.0::Float64,
        1.234::Float64,
        -3.14159::Float64
    ]) AS v
    FROM numbers(100)
);

SELECT 'Vectorized kernel matches a bitmask oracle (Float32).';
SELECT count(), countIf(reinterpretAsUInt32(floatBitTrim(v, 10)) != if(
    isNaN(v), reinterpretAsUInt32(v), bitAnd(reinterpretAsUInt32(v), 0xFFFFFC00)))
FROM (
    SELECT arrayJoin([
        nan::Float32,
        -nan::Float32,
        inf::Float32,
        -inf::Float32,
        reinterpretAsFloat32(toUInt32(0x7FC00001)),
        reinterpretAsFloat32(toUInt32(1)),
        reinterpretAsFloat32(toUInt32(0x007FFFFF)),
        0.0::Float32,
        -0.0::Float32,
        1.234::Float32,
        -3.14159::Float32
    ]) AS v
    FROM numbers(100)
);

SELECT 'Vectorized kernel matches a bitmask oracle (BFloat16).';
SELECT count(), countIf(reinterpretAsUInt16(floatBitTrim(v, 3)) != if(
    isNaN(v), reinterpretAsUInt16(v), bitAnd(reinterpretAsUInt16(v), 0xFFF8)))
FROM (
    SELECT arrayJoin([
        nan::BFloat16,
        -nan::BFloat16,
        inf::BFloat16,
        -inf::BFloat16,
        CAST(reinterpretAsFloat32(toUInt32(0x7F810000)) AS BFloat16),
        CAST(reinterpretAsFloat32(toUInt32(0x00010000)) AS BFloat16),
        CAST(reinterpretAsFloat32(toUInt32(0x007F0000)) AS BFloat16),
        0.0::BFloat16,
        -0.0::BFloat16,
        1.5::BFloat16,
        -3.25::BFloat16
    ]) AS v
    FROM numbers(100)
);

SELECT 'The primary key is usable through floatBitTrim.';
DROP TABLE IF EXISTS 04077_pk;
CREATE TABLE 04077_pk (x Float64) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 512;
INSERT INTO 04077_pk SELECT (number - 10000) / 777 FROM numbers(20000);
-- `NaN`, the infinities, both zeros and a subnormal live in the key itself.
INSERT INTO 04077_pk SELECT arrayJoin([nan, -nan, inf, -inf, 0.0, -0.0, reinterpretAsFloat64(toUInt64(1))]);

-- `force_primary_key` throws unless the condition is resolved through the primary key, which
-- requires the function to declare monotonicity.
SELECT count() FROM 04077_pk WHERE floatBitTrim(x, 20) > 3.5 SETTINGS force_primary_key = 1;
SELECT count() FROM 04077_pk WHERE floatBitTrim(x, 20) < -10 SETTINGS force_primary_key = 1;
SELECT count() FROM 04077_pk WHERE floatBitTrim(x, 0) > 0 SETTINGS force_primary_key = 1;

-- Trimming is not strictly monotonic, so the pruned result must still agree with a full scan.
SELECT
    (SELECT count() FROM 04077_pk WHERE floatBitTrim(x, 20) > 3.5) = countIf(floatBitTrim(x, 20) > 3.5),
    (SELECT count() FROM 04077_pk WHERE floatBitTrim(x, 20) < -10) = countIf(floatBitTrim(x, 20) < -10),
    (SELECT count() FROM 04077_pk WHERE floatBitTrim(x, 52) = 0) = countIf(floatBitTrim(x, 52) = 0),
    (SELECT count() FROM 04077_pk WHERE floatBitTrim(x, 5) BETWEEN -1 AND 1) = countIf(floatBitTrim(x, 5) BETWEEN -1 AND 1)
FROM 04077_pk;

-- A `NaN` range endpoint carries no ordering information, so the key must not be used for it.
SELECT count() FROM 04077_pk WHERE floatBitTrim(x, 20) > nan;
SELECT count() FROM 04077_pk WHERE floatBitTrim(x, 20) > nan SETTINGS force_primary_key = 1; -- { serverError INDEX_NOT_USED }
DROP TABLE 04077_pk;

SELECT 'Trimming zero bits returns the argument bit for bit.';
-- Exercises the identity fast path, which hands back the argument column instead of building a new
-- one. The comparison is on the bit patterns, so `NaN`, the infinities and subnormals count too.
SELECT
    (SELECT countIf(reinterpretAsUInt64(floatBitTrim(v, 0)) != reinterpretAsUInt64(v))
     FROM (SELECT reinterpret(rand64(number), 'Float64') AS v FROM numbers(10000))),
    (SELECT countIf(reinterpretAsUInt32(floatBitTrim(v, 0)) != reinterpretAsUInt32(v))
     FROM (SELECT reinterpret(toUInt32(rand(number)), 'Float32') AS v FROM numbers(10000))),
    (SELECT countIf(reinterpretAsUInt16(floatBitTrim(v, 0)) != reinterpretAsUInt16(v))
     FROM (SELECT reinterpret(toUInt16(number), 'BFloat16') AS v FROM numbers(65536)));

SELECT 'Materialized LowCardinality and sparse columns.';
-- Both are rewritten by the default implementations before the function runs, so the identity path
-- must hand back a column that still matches the declared result type.
SELECT toTypeName(floatBitTrim(materialize(toLowCardinality(1.234::Float64)), 0));
SELECT floatBitTrim(materialize(toLowCardinality(1.234::Float64)), 0) = 1.234::Float64;
SELECT floatBitTrim(materialize(toLowCardinality(1.234::Float64)), 20) = floatBitTrim(1.234::Float64, 20);

-- A real dictionary, so the column the function receives is the ~100 distinct values rather than the
-- 20006 rows of the block. `NaN`, both infinities and a subnormal are among them.
SET allow_suspicious_low_cardinality_types = 1;
DROP TABLE IF EXISTS 04077_lc;
CREATE TABLE 04077_lc (x LowCardinality(Float64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO 04077_lc SELECT (number % 100) / 7 FROM numbers(20000);
INSERT INTO 04077_lc SELECT arrayJoin([nan, inf, -inf, 0.0, -0.0, reinterpretAsFloat64(toUInt64(1))]);
SELECT count(), uniqExact(x) FROM 04077_lc;
SELECT DISTINCT toTypeName(floatBitTrim(x, 20)) FROM 04077_lc;
SELECT countIf(reinterpretAsUInt64(floatBitTrim(x, 0)) != reinterpretAsUInt64(toFloat64(x))) FROM 04077_lc;
SELECT countIf(reinterpretAsUInt64(floatBitTrim(x, 20)) != if(isNaN(toFloat64(x)),
    reinterpretAsUInt64(toFloat64(x)), bitAnd(reinterpretAsUInt64(toFloat64(x)), 0xFFFFFFFFFFF00000))) FROM 04077_lc;
SELECT countIf(isNaN(floatBitTrim(x, 52))), countIf(isInfinite(floatBitTrim(x, 52))) FROM 04077_lc;
DROP TABLE 04077_lc;

DROP TABLE IF EXISTS 04077_sparse;
CREATE TABLE 04077_sparse (x Float64) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS ratio_of_defaults_for_sparse_serialization = 0.1, min_bytes_for_wide_part = 0;
INSERT INTO 04077_sparse SELECT if(number % 20 = 0, number / 7, 0) FROM numbers(20000);
SELECT DISTINCT serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = '04077_sparse' AND column = 'x' AND active;
SELECT countIf(reinterpretAsUInt64(floatBitTrim(x, 0)) != reinterpretAsUInt64(x)) FROM 04077_sparse;
SELECT countIf(reinterpretAsUInt64(floatBitTrim(x, 20)) != bitAnd(reinterpretAsUInt64(x), 0xFFFFFFFFFFF00000)) FROM 04077_sparse;
DROP TABLE 04077_sparse;
