-- Regression test: FunctionVariantAdaptor should handle LowCardinality results
-- from nested functions correctly. The LowCardinality heuristic in getReturnType
-- differs between type computation (nullptr columns = non-const) and execution
-- (actual ColumnConst from scatter/filter), which could cause a type mismatch.

SET allow_experimental_variant_type = 1;
SET use_variant_as_common_type = 1;
SET allow_suspicious_variant_types = 1;

-- The key ingredients for reproduction:
-- 1. An array with incompatible types including LowCardinality to produce a Variant
--    (UInt256 and LowCardinality(Float64) can't unify, so Variant is created)
-- 2. A function (multiply) applied to the Variant with a constant non-Variant argument
-- 3. The constant argument becomes ColumnConst after scatter, changing LC heuristic

SELECT toDecimal128(5.5, 18) * arrayJoin([toUInt256(1), toLowCardinality(1.5)]);
