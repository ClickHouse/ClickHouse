-- Tags: no_asan, no_msan, no_tsan, no_ubsan

-- Test toDecimal512 function variants
SELECT '=== Basic toDecimal512 function tests ===';

SELECT toDecimal512('123.456', 3) AS basic_convert;
SELECT toDecimal512OrZero('invalid', 3) AS or_zero;
SELECT toDecimal512OrNull('invalid', 3) AS or_null;
SELECT toDecimal512OrDefault('invalid', 3, toDecimal512('999.999', 3)) AS or_default;

SELECT '=== Precision and scale validation tests ===';

-- Test with maximum precision (153 digits)
SELECT toDecimal512('123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789', 0) AS max_precision;

-- Test with minimum value (very small decimal)
SELECT toDecimal512('0.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001', 100) AS min_value;

-- Test with different scales
SELECT toDecimal512('123.456789', 6) AS scale_6;
SELECT toDecimal512('123.456789', 3) AS scale_3;
SELECT toDecimal512('123.456789', 0) AS scale_0;

SELECT '=== Type name verification ===';

SELECT toTypeName(toDecimal512('123.456', 3)) AS type_name;
SELECT toTypeName(toDecimal512('123.456789', 6)) AS type_name_scale_6;
SELECT toTypeName(toDecimal512('123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789', 0)) AS type_name_max_precision;
