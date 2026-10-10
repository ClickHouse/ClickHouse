-- Test for issue #88166: DateTimeTransforms UBSAN overflow
-- This test ensures that accurateCast with out-of-range values doesn't cause UBSAN errors

SET session_timezone = 'UTC';

-- Test with large positive value (should throw exception with proper error message)
SELECT accurateCast(100000000000000000000, 'DateTime') SETTINGS enable_context_typed_number_literals = 0; -- {serverError CANNOT_CONVERT_TYPE}
-- With context typed literals this literal is a wide integer instead of a `Float64`, so the `Float64` case needs an explicit type.
SELECT accurateCast(toFloat64(100000000000000000000), 'DateTime'); -- {serverError CANNOT_CONVERT_TYPE}

-- Test with large negative value
SELECT accurateCast(-100000000000000000000, 'DateTime') SETTINGS enable_context_typed_number_literals = 0; -- {serverError CANNOT_CONVERT_TYPE}
-- With context typed literals this literal is a wide integer instead of a `Float64`, so the `Float64` case needs an explicit type.
SELECT accurateCast(toFloat64(-100000000000000000000), 'DateTime'); -- {serverError CANNOT_CONVERT_TYPE}

-- Test with maximum valid DateTime value (should work)
SELECT accurateCast(4294967295, 'DateTime');

-- Test with minimum valid DateTime value (should work)
SELECT accurateCast(0, 'DateTime');

-- Test with value just above maximum (should throw exception)
SELECT accurateCast(4294967296, 'DateTime'); -- {serverError CANNOT_CONVERT_TYPE}

-- Test with value just below minimum (should throw exception)
SELECT accurateCast(-1, 'DateTime'); -- {serverError CANNOT_CONVERT_TYPE}
