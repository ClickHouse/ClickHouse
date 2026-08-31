-- The set of aggregate functions that accept a Variant argument natively (count, any, uniq, groupArray, argMin's
-- returned "arg", ...) is declared by AggregateFunctionProperties::support_variant_argument.
-- AggregateFunctionFactory::get() consults it to decide whether a Variant argument is resolved natively or wrapped
-- in AggregateFunctionVariantAdapter, without attempting native resolution and catching its failure to find out.
-- This test locks in that routing: a native function keeps its Variant-native result over a Variant with a clean
-- supertype, while a non-native function (sum) is adapted to the Nullable(supertype) form.

SET allow_experimental_variant_type = 1;
SET allow_suspicious_variant_types = 1;

DROP TABLE IF EXISTS t_variant_native_prop;
CREATE TABLE t_variant_native_prop (v Variant(UInt8, Float64), k UInt64) ENGINE = Memory;
INSERT INTO t_variant_native_prop VALUES (1, 10), (2.5, 20), (3, 30), (NULL, 40);

-- Functions that accept a Variant natively keep the Variant in their result type: they are NOT rerouted through
-- the supertype adapter, which would collapse the argument to Nullable(Float64).
SELECT 'any', toTypeName(any(v)) FROM t_variant_native_prop;
SELECT 'anyLast', toTypeName(anyLast(v)) FROM t_variant_native_prop;
SELECT 'groupArray', toTypeName(groupArray(v)) FROM t_variant_native_prop;
SELECT 'groupUniqArray', toTypeName(groupUniqArray(v)) FROM t_variant_native_prop;
SELECT 'topK', toTypeName(topK(v)) FROM t_variant_native_prop;
SELECT 'argMin arg keeps Variant', toTypeName(argMin(v, k)) FROM t_variant_native_prop;
SELECT 'argMax arg keeps Variant', toTypeName(argMax(v, k)) FROM t_variant_native_prop;

-- count / the uniq family return their own numeric result type (native). Native resolution preserves the
-- standard NULL-skipping contract (AggregateFunctionVariantNull): count(expr) counts the not-NULL values of
-- its argument (see 04652), and the uniq family does not count the NULL of the Variant as a distinct value,
-- exactly as it skips the NULL values of a Nullable argument.
SELECT 'count', count(v) FROM t_variant_native_prop;
SELECT 'uniq', uniq(v) FROM t_variant_native_prop;
SELECT 'uniqExact', uniqExact(v) FROM t_variant_native_prop;
SELECT 'uniqCombined', uniqCombined(v) FROM t_variant_native_prop;

-- A function that does NOT accept a Variant natively (sum) is adapted over the least common supertype Float64,
-- wrapped in Nullable. This is the other side of the property boundary.
SELECT 'sum adapted', toTypeName(sum(v)) FROM t_variant_native_prop;

-- Combinators resolve the base function's property (tryGetProperties strips the suffix), so a native function
-- stays native through -If and sum stays adapted.
SELECT 'anyIf native', toTypeName(anyIf(v, k > 15)) FROM t_variant_native_prop;
SELECT 'sumIf adapted', toTypeName(sumIf(v, k > 15)) FROM t_variant_native_prop;

-- groupFormat accepts an argument of any type natively and formats it using the original argument types it
-- captures at creation, so a schema-carrying format must expose the real Variant type. It must not be rerouted
-- through the supertype adapter, which would collapse the argument to Nullable(supertype) in the formatted schema.
SELECT 'groupFormat keeps Variant schema',
       groupFormat('JSONCompactEachRowWithNamesAndTypes')(CAST(1 AS Variant(UInt8, UInt64))) LIKE '%Variant(%';
-- The same must hold for the state / merge forms: the stored state type keeps the Variant, so a round trip
-- through the state formats with the real Variant schema rather than the adapter's Nullable(supertype).
SELECT 'groupFormatMerge keeps Variant schema',
       groupFormatMerge('JSONCompactEachRowWithNamesAndTypes')(s) LIKE '%Variant(%'
FROM (SELECT groupFormatState('JSONCompactEachRowWithNamesAndTypes')(CAST(1 AS Variant(UInt8, UInt64))) AS s);

-- estimateCompressionRatio accepts an argument of any type and measures the compression ratio of the column's
-- real wire layout, so it keeps the genuine Variant argument instead of measuring the adapter's
-- Nullable(supertype) re-encoding. The state type shows which argument type the function was resolved with, and
-- a Variant with no common supertype (which the adapter could not handle at all) still resolves natively.
SELECT 'estimateCompressionRatio keeps Variant', toTypeName(estimateCompressionRatioState(v)) FROM t_variant_native_prop;
SELECT 'estimateCompressionRatio no-supertype Variant', estimateCompressionRatio(CAST(number AS Variant(String, UInt64))) > 0 FROM numbers(1000);

-- The NULL rows of a `Variant` must stay visible to it: the wire layout it measures includes the discriminators
-- of those rows, so they are part of the size it reports. If the function were ever rerouted through
-- AggregateFunctionVariantNull (which skips the NULL rows of a Variant argument, as count/uniq do above), an
-- all-NULL Variant column would produce no bytes at all and the ratio would collapse to 0 instead of the ratio
-- of the highly compressible all-NULL discriminator stream.
DROP TABLE IF EXISTS t_variant_all_null;
CREATE TABLE t_variant_all_null (v Variant(String, UInt64)) ENGINE = Memory;
INSERT INTO t_variant_all_null SELECT CAST(NULL AS Variant(String, UInt64)) FROM numbers(1000);
SELECT 'estimateCompressionRatio sees Variant NULLs', estimateCompressionRatio(v) > 0 FROM t_variant_all_null;
DROP TABLE t_variant_all_null;

-- singleValueOrNull is excluded from the adapter (is_distinctness_sensitive): its contract is "the value if
-- there is exactly one distinct non-NULL value, otherwise NULL", and the cast to Nullable(supertype) collapses
-- Variant values that are distinct because their alternative types differ (1::UInt8 vs 1::UInt64), which would
-- silently change the result. A Variant argument keeps failing with the function's original error, unchanged
-- from before the adapter existed.
SELECT singleValueOrNull(v) FROM t_variant_native_prop; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT singleValueOrNull(CAST(1 AS Variant(UInt8, UInt64))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT singleValueOrNull(CAST('x' AS Variant(String, UInt64))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

DROP TABLE t_variant_native_prop;
