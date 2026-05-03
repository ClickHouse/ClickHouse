-- Test: exercises bounded behavior of the Bool-variant skip in `validateDataType`.
-- Covers: src/Interpreters/parseColumnsListForTableFunction.cpp:97-98 — `continue` skips ONLY
-- Bool-paired iterations, leaving non-Bool pairs to still be checked by `tryGetLeastSupertype`.
-- The PR's own test covers the positive 2-variant case `Variant(UInt32, Bool)`.
-- This test covers the negative 3-variant case where a non-Bool pair (`UInt32`,`Int64`)
-- inside a Variant that ALSO contains `Bool` must still be detected as suspicious.
SET allow_experimental_variant_type = 1;
SET allow_suspicious_variant_types = 0;
-- Bool first position with similar numeric pair must still throw
SELECT 42::UInt32::Variant(Bool, UInt32, Int64); -- { serverError ILLEGAL_COLUMN }
-- Bool middle position with similar numeric pair must still throw
SELECT 42::UInt32::Variant(UInt32, Bool, Int64); -- { serverError ILLEGAL_COLUMN }
-- Bool last position with similar numeric pair must still throw
SELECT 42::UInt32::Variant(UInt32, Int64, Bool); -- { serverError ILLEGAL_COLUMN }
-- Bool first allowed alone (smoke test for asymmetric `i`-arm of the OR)
SELECT 42::UInt32::Variant(Bool, UInt32);
