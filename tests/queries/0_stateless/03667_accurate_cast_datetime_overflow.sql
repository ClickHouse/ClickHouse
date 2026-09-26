-- Test for issue #88166: DateTimeTransforms UBSAN overflow
-- This test ensures that accurateCast with out-of-range values doesn't cause UBSAN errors

SET session_timezone = 'UTC';

-- Test with large positive value (should throw exception with proper error message)
SELECT accurateCast(100000000000000000000, 'DateTime'); -- {serverError CANNOT_CONVERT_TYPE}

-- Test with large negative value
SELECT accurateCast(-100000000000000000000, 'DateTime'); -- {serverError CANNOT_CONVERT_TYPE}

-- Test with maximum valid DateTime value (should work)
SELECT accurateCast(4294967295, 'DateTime');

-- Test with minimum valid DateTime value (should work)
SELECT accurateCast(0, 'DateTime');

-- Test with value just above maximum (should throw exception)
SELECT accurateCast(4294967296, 'DateTime'); -- {serverError CANNOT_CONVERT_TYPE}

-- Test with value just below minimum (should throw exception)
SELECT accurateCast(-1, 'DateTime'); -- {serverError CANNOT_CONVERT_TYPE}
