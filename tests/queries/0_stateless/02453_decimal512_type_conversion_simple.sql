-- Tags: no_asan, no_msan, no_tsan, no_ubsan

-- Test toDecimal512 function variants
SELECT toDecimal512('123.456', 3) AS basic_convert;
SELECT toDecimal512OrZero('invalid', 3) AS or_zero;
SELECT toDecimal512OrNull('invalid', 3) AS or_null;
SELECT toDecimal512OrDefault('invalid', 3, toDecimal512('999.999', 3)) AS or_default;

-- Test with different scales
SELECT toDecimal512('123.456789', 6) AS scale_6;
SELECT toDecimal512('123.456789', 3) AS scale_3;
SELECT toDecimal512('123.456789', 0) AS scale_0;

-- Test type name verification
SELECT toTypeName(toDecimal512('123.456', 3)) AS type_name;

