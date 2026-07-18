-- The CrossTab family (contingency, cramersV, cramersVBiasCorrected, theilsU) hashes its arguments through
-- UniqVariadicHash / IColumn::getDataAt, which ColumnVariant does not implement, so before this fix a Variant
-- argument was accepted at resolution and only failed later during execution. The family now rejects Variant (and
-- Dynamic) at resolution, so AggregateFunctionVariantAdapter aggregates over the least common supertype of the
-- variants: a lossless supertype (e.g. Variant(UInt8, UInt64) -> UInt64) is aggregated over that concrete type,
-- while a mix with no lossless common supertype keeps reporting ILLEGAL_TYPE_OF_ARGUMENT at resolution (these
-- functions are not float-promoting, so there is no Float64 fallback).

SET allow_experimental_variant_type = 1;
SET allow_suspicious_variant_types = 1;
SET allow_experimental_dynamic_type = 1;

-- A lossless-supertype Variant matches the same aggregation over the concrete supertype.
SELECT
    round(contingency(v, b), 6) = round(contingency(toUInt64(a), b), 6),
    round(cramersV(v, b), 6) = round(cramersV(toUInt64(a), b), 6),
    round(cramersVBiasCorrected(v, b), 6) = round(cramersVBiasCorrected(toUInt64(a), b), 6),
    round(theilsU(v, b), 6) = round(theilsU(toUInt64(a), b), 6)
FROM (SELECT number % 10 AS a, CAST(number % 10 AS Variant(UInt8, UInt64)) AS v, number % 6 AS b FROM numbers(100));

-- Both arguments Variant over a lossless supertype.
SELECT round(contingency(v1, v2), 6) = round(contingency(toUInt64(a), toUInt64(b)), 6)
FROM (SELECT number % 10 AS a, number % 6 AS b, CAST(number % 10 AS Variant(UInt8, UInt64)) AS v1, CAST(number % 6 AS Variant(UInt8, UInt64)) AS v2 FROM numbers(100));

-- The adapted state carries the least common supertype, wrapped in Nullable.
SELECT toTypeName(contingencyState(v, b))
FROM (SELECT CAST(number % 10 AS Variant(UInt8, UInt64)) AS v, number % 6 AS b FROM numbers(100));

-- A state built over the adapted supertype merges to the same value as the direct aggregation.
SELECT round((SELECT contingencyMerge(s) FROM (SELECT contingencyState(v, b) AS s FROM (SELECT CAST(number % 10 AS Variant(UInt8, UInt64)) AS v, number % 6 AS b FROM numbers(100)))), 6)
     = round((SELECT contingency(toUInt64(number % 10), number % 6) FROM numbers(100)), 6);

-- A lossless supertype that is a floating-point type is aggregated over that concrete type as well.
SELECT round(cramersV(v, b), 6) = round(cramersV(CAST(a AS Float64), b), 6)
FROM (SELECT toInt32(number % 10) AS a, CAST(toInt32(number % 10) AS Variant(Int32, Float64)) AS v, number % 6 AS b FROM numbers(100));

-- A Variant with no lossless common supertype reports a clean error at resolution (not a resolve-then-execute
-- failure): these functions are not float-promoting, so there is no Float64 fallback.
SELECT contingency(v, 1) FROM (SELECT CAST(number AS Variant(String, UInt64)) AS v FROM numbers(10)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT cramersV(v, 1) FROM (SELECT CAST(number AS Variant(String, UInt64)) AS v FROM numbers(10)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT cramersVBiasCorrected(v, 1) FROM (SELECT CAST(number AS Variant(String, UInt64)) AS v FROM numbers(10)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT theilsU(v, 1) FROM (SELECT CAST(number AS Variant(String, UInt64)) AS v FROM numbers(10)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Dynamic is rejected as well.
SELECT contingency(d, 1) FROM (SELECT number::Dynamic AS d FROM numbers(10)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
