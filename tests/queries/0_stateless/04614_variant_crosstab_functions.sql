-- The CrossTab family (contingency, cramersV, cramersVBiasCorrected, theilsU) hashes its arguments through
-- UniqVariadicHash / IColumn::getDataAt, which ColumnVariant does not implement, so before this fix a Variant
-- argument was accepted at resolution and only failed later during execution. The family now rejects Variant (and
-- Dynamic) at resolution. It is not adapted through the least common supertype of the variants either: the result
-- keys on the exact identity of each category, and `1::UInt8` and `1::UInt64` are different categories of a
-- `Variant(UInt8, UInt64)` that a cast to `UInt64` would collapse into one.

SET allow_suspicious_variant_types = 1;
SET allow_lossy_numeric_supertype = 1;

-- Even a Variant with a lossless common supertype is rejected at resolution.
SELECT contingency(v, b) FROM (SELECT CAST(number % 10 AS Variant(UInt8, UInt64)) AS v, number % 6 AS b FROM numbers(100)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT cramersV(v, b) FROM (SELECT CAST(number % 10 AS Variant(UInt8, UInt64)) AS v, number % 6 AS b FROM numbers(100)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT cramersVBiasCorrected(v, b) FROM (SELECT CAST(number % 10 AS Variant(UInt8, UInt64)) AS v, number % 6 AS b FROM numbers(100)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT theilsU(v, b) FROM (SELECT CAST(number % 10 AS Variant(UInt8, UInt64)) AS v, number % 6 AS b FROM numbers(100)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT contingency(b, v) FROM (SELECT CAST(number % 10 AS Variant(UInt8, UInt64)) AS v, number % 6 AS b FROM numbers(100)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- The same under combinators, in window functions, and for a declared state type.
SELECT contingencyIf(v, b, b > 1) FROM (SELECT CAST(number % 10 AS Variant(UInt8, UInt64)) AS v, number % 6 AS b FROM numbers(100)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT contingencyState(v, b) FROM (SELECT CAST(number % 10 AS Variant(UInt8, UInt64)) AS v, number % 6 AS b FROM numbers(100)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT cramersV(v, b) OVER () FROM (SELECT CAST(number % 10 AS Variant(UInt8, UInt64)) AS v, number % 6 AS b FROM numbers(100)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT CAST(NULL AS Nullable(AggregateFunction(cramersV, Variant(UInt8, UInt64), UInt8))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- A Variant with no lossless common supertype and Dynamic are rejected as well.
SELECT contingency(v, 1) FROM (SELECT CAST(number AS Variant(String, UInt64)) AS v FROM numbers(10)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT contingency(d, 1) FROM (SELECT number::Dynamic AS d FROM numbers(10)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- An explicit cast to the supertype keeps working, with the categories it defines.
SELECT round(contingency(CAST(v AS UInt64), b), 6) = round(contingency(toUInt64(number % 10), b), 6)
FROM (SELECT number, CAST(number % 10 AS Variant(UInt8, UInt64)) AS v, number % 6 AS b FROM numbers(100));
