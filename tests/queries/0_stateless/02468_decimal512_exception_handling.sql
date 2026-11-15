-- Tags: no_asan, no_msan, no_tsan, no_ubsan

-- Test invalid input handling
SELECT '=== Invalid input handling ===';
SELECT toDecimal512OrZero('invalid_string', 3) AS invalid_input;
SELECT toDecimal512OrNull('invalid_string', 3) AS invalid_input_null;

-- Test division by zero
SELECT '=== Division by zero ===';
SELECT toDecimal512('123.456', 3) / toDecimal512('0.0', 1); -- { serverError ILLEGAL_DIVISION }

-- Test scale out of bounds
SELECT '=== Scale out of bounds ===';
SELECT toDecimal512('1', 200); -- { serverError ARGUMENT_OUT_OF_BOUND }