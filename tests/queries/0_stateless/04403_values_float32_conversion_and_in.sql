-- Tests for https://github.com/ClickHouse/ClickHouse/issues/43144
-- The `values` table function (and other non-strict Field conversions) must accept decimal
-- literals for floating-point columns. Most decimals are not exactly representable in a
-- narrower float, so requiring exact equality made `values` reject them; the conversion to the
-- nearest representable value is expected (like `CAST`), not an error.

SELECT '-- values() with Float32: lossy decimal literals are accepted (issue #43144)';
SELECT x, y FROM values('x Int8, y Float32', (0, 0.1), (1, 0.34), (2, 0.88), (3, -1.23), (4, -3.3), (5, 5.4)) ORDER BY x;

SELECT '-- single Float32 column';
SELECT x FROM values('x Float32', 0.1, 0.5, 1) ORDER BY x;

SELECT '-- the converted value matches CAST to Float32';
SELECT x = CAST(0.1, 'Float32') FROM values('x Float32', 0.1);

SELECT '-- BFloat16 and Float64 columns';
SELECT x FROM values('x BFloat16', 0.5);
SELECT x FROM values('x Float64', 0.1);

SELECT '-- out-of-range values are still rejected (no silent overflow to inf or wrap-around)';
SELECT x FROM values('x Float32', 1e300); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT x FROM values('x Int8', 256); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT x FROM values('x UInt8', -1); -- { serverError ARGUMENT_OUT_OF_BOUND }

SELECT '-- IN keeps exact value semantics, consistent with the = operator';
-- The Float64 literal 0.1 is not representable in Float32, so it is excluded from the set,
-- exactly like `toFloat32(0.1) = 0.1` is false. The strict conversion used by IN is unchanged.
SELECT toFloat32(0.1) IN (0.1), toFloat32(0.1) = 0.1;
SELECT toFloat32(0.1) IN (toFloat32(0.1)), toFloat32(0.1) = toFloat32(0.1);
SELECT 0.1 IN (toFloat32(0.1)), 0.1 = toFloat32(0.1);
-- 0.5 is exactly representable in Float32, so it matches in both IN and =.
SELECT toFloat32(0.5) IN (0.5), toFloat32(0.5) = 0.5;

-- A strict IN set built for a `Variant(Float64)` target must not round a value that is not
-- exactly representable in Float64 (e.g. the maximum UInt64). The exact value cannot be a set
-- element, so this errors instead of silently matching the rounded Float64: the strict
-- conversion used by IN must not fall back to a lossy alternative for Variant targets either.
SELECT CAST(toFloat64(toUInt64(-1)), 'Variant(Float64)') IN (toUInt64(-1)); -- { serverError TYPE_MISMATCH }

SELECT '-- same IN result without the analyzer';
SELECT toFloat32(0.1) IN (0.1) SETTINGS allow_experimental_analyzer = 0;
SELECT toFloat32(0.5) IN (0.5) SETTINGS allow_experimental_analyzer = 0;

SELECT '-- conversion to a Variant prefers a lossless alternative, falling back to lossy only when needed';
-- The exactly-representable Int64 values stay in the Array(Int64) alternative instead of being
-- stored lossily in the earlier-listed (sorted) Array(Float64) alternative.
-- These cases need the analyzer: the old query analysis eagerly computes a common type for the
-- array literal (mixing huge Int64 with Float64) and rejects it with NO_COMMON_TYPE before the
-- value reaches the Variant column type.
SELECT x FROM values('x Array(Variant(Array(Int64), Array(Float64)))', [[9223372036854775807, -9223372036854775808], [0.9999, 7]]) SETTINGS allow_experimental_analyzer = 1;
-- When no alternative can represent the value exactly, the nearest representable value is used.
SELECT x FROM values('x Array(Variant(Array(Float32), Array(String)))', [[0.1, 0.2]]) SETTINGS allow_experimental_analyzer = 1;
