-- { echo }

-- Positive and negative offset without length
SELECT digits(1234567890, 3), digits(1234567890, -3);

-- Positive offset and positive length
SELECT digits(1234567890, 3, 2), digits(1234567890, 9, 1);

-- Positive offset and negative length
SELECT digits(1234567890, 3, -2), digits(1234567890, 9, -1);

-- Negative offset and positive length
SELECT digits(1234567890, -3, 2), digits(1234567890, -9, 1), digits(1234567890, -9223372036854775808, 1);

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

-- Nullable offset: NULL rows return NULL instead of throwing
SELECT digits(123, arrayJoin([NULL, 1]::Array(Nullable(Int8))));

-- LowCardinality offset
SELECT digits(123, toLowCardinality(number + 1)) FROM numbers(2);

-- Nullable value inside a constant column (Const(Nullable)) returns NULL
SELECT digits(123, CAST(NULL, 'Nullable(Int8)'));

-- Large UInt64 offset beyond INT64_MAX is past the end and returns 0 (not reinterpreted as negative)
SELECT digits(1234567890, toUInt64(9223372036854775808));

-- Large UInt64 length beyond INT64_MAX keeps all digits (not reinterpreted as -1)
SELECT digits(1234567891, 1, toUInt64(18446744073709551615));

-- server errors
SELECT digits(1234567890, 0); -- {serverError ZERO_ARRAY_OR_TUPLE_INDEX}
SELECT digits(toString(1234567890), 1); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT digits(1234567890); -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}
SELECT digits(1234567890, 1, 2, 3); -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}

-- NULL
SELECT digits(123, NULL);
SELECT digits(NULL, 1);
SELECT digits(arrayJoin([NULL, 123]::Array(Nullable(Int8))), 1);
SELECT digits(123, toLowCardinality(arrayJoin([NULL, 1]::Array(Nullable(Int8)))));
SELECT digits(123, 1, NULL);

SELECT digits(1234567890, toInt64(-9223372036854775808), toUInt64(9223372036854775808));
SELECT digits(1234567891, 1, toUInt64(18446744073709551615));
