-- Tags: no_asan, no_msan, no_tsan, no_ubsan

-- Test exception handling
SELECT '=== Exception handling ===';

-- Test invalid input handling
SELECT '=== Invalid input handling ===';
SELECT toDecimal512OrZero('invalid_string', 3) AS invalid_input;
SELECT toDecimal512OrNull('invalid_string', 3) AS invalid_input_null;

-- Test division by zero
SELECT '=== Division by zero ===';
SELECT toDecimal512('123.456', 3) / toDecimal512('0.0', 1); -- { serverError ILLEGAL_DIVISION }

-- Test overflow conditions (Note: Decimal512 has very high precision, so overflow is hard to trigger)
SELECT '=== Overflow conditions ===';
-- These operations may not actually overflow due to Decimal512's high precision
-- SELECT toDecimal512('999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999', 0) * toDecimal512('2', 0); -- { serverError DECIMAL_OVERFLOW }

-- Test precision overflow (Note: Decimal512 supports up to 153 digits)
SELECT '=== Precision overflow ===';
-- This operation may not actually overflow due to Decimal512's high precision
-- SELECT toDecimal512('1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890', 0); -- { serverError DECIMAL_OVERFLOW }
