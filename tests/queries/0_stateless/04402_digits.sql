-- { echo }

-- Positive and negative offset without length
SELECT digits(1234567890, 3), digits(1234567890, -3);

-- Positive offset and positive length
SELECT digits(1234567890, 3, 2), digits(1234567890, 9, 1);

-- Positive offset and negative length
SELECT digits(1234567890, 3, -2), digits(1234567890, 9, -1);

-- Negative offset and positive length
SELECT digits(1234567890, -3, 2), digits(1234567890, -9, 1);

-- Negative offset and negative length
SELECT digits(1234567890, -3, -2), digits(1234567890, -9, -1);

-- Offset exceeds the boundary
SELECT digits(1234567890, 11);

-- Zero length returns nothing
SELECT digits(1234567890, 3, 0);

-- Leading zeros are not preserved (return type is UInt64)
SELECT digits(1000, 2, 2), digits(1000, 2);

-- Full 20-digit UInt64 (exercises the count >= 20 branch)
SELECT digits(18446744073709551615, 1), digits(18446744073709551615, 5, 3);

-- Signed and negative inputs use the magnitude, including INT64_MIN
SELECT digits(-123, 1), digits(toInt64(-9223372036854775808), 1);

-- Narrow integer types
SELECT digits(toUInt8(255), 2), digits(toInt8(-128), 1);

-- Non-constant columns exercise the per-row execution path
SELECT n, digits(n, 2, 2) FROM (SELECT arrayJoin([12345, 67890, 1234567890]) AS n) ORDER BY n;

-- server errors
SELECT digits(1234567890, 0); -- {serverError ZERO_ARRAY_OR_TUPLE_INDEX}
SELECT digits(toString(1234567890), 1); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT digits(1234567890); -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}
SELECT digits(1234567890, 1, 2, 3); -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}