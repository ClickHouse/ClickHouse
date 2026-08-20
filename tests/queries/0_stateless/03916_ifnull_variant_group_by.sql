-- https://github.com/ClickHouse/ClickHouse/issues/96664

SET enable_analyzer = 1; -- use_variant_as_common_type for if() requires the analyzer

-- ifNull with Variant produced by if() with incompatible types, used in GROUP BY (the original exception)
SELECT ifNull(if(0, '1', 1), 1::Int8) GROUP BY 1 SETTINGS use_variant_as_common_type = 1, allow_suspicious_types_in_group_by = 1;

-- Check the result type: should be Variant(Int8, String, UInt8) since ifNull must include the alternative type
SELECT toTypeName(ifNull(if(0, '1', 1), 1::Int8)) SETTINGS use_variant_as_common_type = 1;

-- ifNull where the alternative type is already present in the Variant
SELECT toTypeName(ifNull(if(0, '1', 1), 'hello')) SETTINGS use_variant_as_common_type = 1;

-- ifNull with Variant and a completely different alternative type
SELECT toTypeName(ifNull(if(0, '1', 1), toFloat64(3.14))) SETTINGS use_variant_as_common_type = 1;

-- coalesce with Variant (same underlying issue)
SELECT coalesce(if(0, '1', 1), 1::Int8) GROUP BY 1 SETTINGS use_variant_as_common_type = 1, allow_suspicious_types_in_group_by = 1;

-- coalesce result type with Variant
SELECT toTypeName(coalesce(if(0, '1', 1), 1::Int8)) SETTINGS use_variant_as_common_type = 1;

-- coalesce with multiple Variant-producing arguments
SELECT toTypeName(coalesce(if(0, '1', 1), if(0, toDate('2024-01-01'), 3.14))) SETTINGS use_variant_as_common_type = 1;

-- ifNull with non-nullable first arg (should be identity)
SELECT ifNull(42, 100);

-- ifNull with NULL first arg
SELECT ifNull(NULL, 'fallback');

-- ifNull with Nullable
SELECT ifNull(CAST(NULL AS Nullable(Int32)), 123);

-- coalesce basic cases still work
SELECT coalesce(NULL, NULL, 42);
SELECT coalesce(NULL, 'hello', 42);

-- With use_variant_as_common_type: ifNull and coalesce produce Variant from incompatible non-Variant types
SELECT toTypeName(ifNull(CAST(1 AS Nullable(Int8)), 'hello')) SETTINGS use_variant_as_common_type = 1;
SELECT toTypeName(coalesce(CAST(NULL AS Nullable(Int8)), 'hello')) SETTINGS use_variant_as_common_type = 1;

-- Without use_variant_as_common_type and no Variant input: incompatible types should throw
SELECT ifNull(CAST(1 AS Nullable(Int8)), 'hello') SETTINGS use_variant_as_common_type = 0; -- { serverError NO_COMMON_TYPE }
SELECT coalesce(CAST(NULL AS Nullable(Int8)), 'hello') SETTINGS use_variant_as_common_type = 0; -- { serverError NO_COMMON_TYPE }
