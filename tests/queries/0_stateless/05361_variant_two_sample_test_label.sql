-- The second argument of a two-sample test (`studentTTest`, `welchTTest`, `meanZTest`, `mannWhitneyUTest`,
-- `kolmogorovSmirnovTest`) is the sample label, read as an unsigned integer. The lossy `Float64` fallback of the
-- `Variant` adapter, which is allowed for the sample data, must not be applied to the label: a `Variant` label with
-- no lossless common supertype is rejected instead of being cast to `Float64` and truncated into sample 0 or 1.

SET allow_suspicious_variant_types = 1;
SET allow_lossy_numeric_supertype = 1;

-- The sample data may still be adapted through the lossy fallback.
SELECT studentTTest(x, g) = studentTTest(CAST(x AS Nullable(Float64)), g)
FROM (SELECT CAST(if(number % 2, toInt64(number), toFloat64(number) + 0.5) AS Variant(Int64, Float64)) AS x, toUInt8(number % 3 = 0) AS g FROM numbers(20));

-- A label of a Variant type with no lossless common supertype is rejected.
SELECT studentTTest(x, CAST(g AS Variant(Int64, Float64))) FROM (SELECT toFloat64(number) AS x, toInt64(number % 2) AS g FROM numbers(10)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT welchTTest(x, CAST(g AS Variant(Int64, Float64))) FROM (SELECT toFloat64(number) AS x, toInt64(number % 2) AS g FROM numbers(10)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT meanZTest(1., 1., 0.95)(x, CAST(g AS Variant(Int64, Float64))) FROM (SELECT toFloat64(number) AS x, toInt64(number % 2) AS g FROM numbers(10)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT mannWhitneyUTest(x, CAST(g AS Variant(Int64, Float64))) FROM (SELECT toFloat64(number) AS x, toInt64(number % 2) AS g FROM numbers(10)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT, NOT_IMPLEMENTED }
SELECT kolmogorovSmirnovTest(x, CAST(g AS Variant(Int64, Float64))) FROM (SELECT toFloat64(number) AS x, toInt64(number % 2) AS g FROM numbers(10)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT, NOT_IMPLEMENTED }
-- The same under a combinator that appends its own argument after the label.
SELECT studentTTestIf(x, CAST(g AS Variant(Int64, Float64)), x > 1) FROM (SELECT toFloat64(number) AS x, toInt64(number % 2) AS g FROM numbers(10)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- A label with a lossless common supertype is adapted as before.
SELECT studentTTest(x, CAST(g AS Variant(UInt8, UInt64))) = studentTTest(x, g)
FROM (SELECT toFloat64(number) AS x, toUInt8(number % 2) AS g FROM numbers(10));
