-- Test for converting out-of-range float values to enum types
-- This should throw CANNOT_CONVERT_TYPE error instead of causing undefined behavior

-- Test Enum16 (Int16 range: -32768 to 32767)
SELECT CAST(1e38 AS Enum16('a' = 1, 'b' = 2)); -- { serverError CANNOT_CONVERT_TYPE }
SELECT CAST(-1e38 AS Enum16('a' = 1, 'b' = 2)); -- { serverError CANNOT_CONVERT_TYPE }
SELECT CAST(toFloat64(1e38) AS Enum16('a' = 1, 'b' = 2)); -- { serverError CANNOT_CONVERT_TYPE }
SELECT CAST(toFloat32(1e38) AS Enum16('a' = 1, 'b' = 2)); -- { serverError CANNOT_CONVERT_TYPE }

-- Test Enum8 (Int8 range: -128 to 127)
SELECT CAST(1e38 AS Enum8('a' = 1, 'b' = 2)); -- { serverError CANNOT_CONVERT_TYPE }
SELECT CAST(-1e38 AS Enum8('a' = 1, 'b' = 2)); -- { serverError CANNOT_CONVERT_TYPE }
SELECT CAST(1000.0 AS Enum8('a' = 1, 'b' = 2)); -- { serverError CANNOT_CONVERT_TYPE }
SELECT CAST(-1000.0 AS Enum8('a' = 1, 'b' = 2)); -- { serverError CANNOT_CONVERT_TYPE }

-- Test values just outside the range
SELECT CAST(32768.0 AS Enum16('a' = 1, 'b' = 2)); -- { serverError CANNOT_CONVERT_TYPE }
SELECT CAST(-32769.0 AS Enum16('a' = 1, 'b' = 2)); -- { serverError CANNOT_CONVERT_TYPE }
SELECT CAST(128.0 AS Enum8('a' = 1, 'b' = 2)); -- { serverError CANNOT_CONVERT_TYPE }
SELECT CAST(-129.0 AS Enum8('a' = 1, 'b' = 2)); -- { serverError CANNOT_CONVERT_TYPE }

-- Test valid conversions still work (value in range but not in enum - different error)
SELECT CAST(3.0 AS Enum8('a' = 1, 'b' = 2)); -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
SELECT CAST(3.0 AS Enum16('a' = 1, 'b' = 2)); -- { serverError UNKNOWN_ELEMENT_OF_ENUM }

-- Test valid conversions that should succeed
SELECT CAST(1.0 AS Enum8('a' = 1, 'b' = 2));
SELECT CAST(2.0 AS Enum16('a' = 1, 'b' = 2));
SELECT CAST(toFloat64(1) AS Enum8('a' = 1, 'b' = 2));
SELECT CAST(toFloat32(2) AS Enum16('a' = 1, 'b' = 2));

-- Test with truncation (1.5 truncates to 1, which is valid)
SELECT CAST(1.5 AS Enum8('a' = 1, 'b' = 2));
SELECT CAST(1.9 AS Enum16('a' = 1, 'b' = 2));
