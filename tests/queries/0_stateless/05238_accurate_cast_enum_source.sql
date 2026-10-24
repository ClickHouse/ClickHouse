-- accurateCast must throw, and accurateCastOrNull must return NULL, when the target type cannot
-- represent the source value. An Enum8/Enum16 value is stored as the underlying Int8/Int16, so in
-- every pair of columns below the Enum column must equal the integer column. Before the fix the
-- Enum column returned a wrapped or truncated value, for example 255 rather than NULL for
-- Enum8('v' = -1) converted to UInt8.

SET session_timezone = 'UTC';

SELECT '-- A negative Enum8 is not representable in any unsigned target';
WITH CAST('v', $$Enum8('v' = -1)$$) AS e, toInt8(-1) AS i
SELECT accurateCastOrNull(e, 'UInt8')  AS enum_uint8,  accurateCastOrNull(i, 'UInt8')  AS int_uint8,
       accurateCastOrNull(e, 'UInt16') AS enum_uint16, accurateCastOrNull(i, 'UInt16') AS int_uint16,
       accurateCastOrNull(e, 'UInt32') AS enum_uint32, accurateCastOrNull(i, 'UInt32') AS int_uint32,
       accurateCastOrNull(e, 'UInt64') AS enum_uint64, accurateCastOrNull(i, 'UInt64') AS int_uint64
FORMAT Vertical;

SELECT '-- ... nor in the temporal targets';
WITH CAST('v', $$Enum8('v' = -1)$$) AS e, toInt8(-1) AS i
SELECT accurateCastOrNull(e, 'Date')     AS enum_date,     accurateCastOrNull(i, 'Date')     AS int_date,
       accurateCastOrNull(e, 'DateTime') AS enum_datetime, accurateCastOrNull(i, 'DateTime') AS int_datetime
FORMAT Vertical;

SELECT '-- A wide Enum16 value is not truncated into a narrow target';
WITH CAST('v', $$Enum16('v' = 32767)$$) AS e, toInt16(32767) AS i
SELECT accurateCastOrNull(e, 'Int8')  AS enum_int8,  accurateCastOrNull(i, 'Int8')  AS int_int8,
       accurateCastOrNull(e, 'UInt8') AS enum_uint8, accurateCastOrNull(i, 'UInt8') AS int_uint8
FORMAT Vertical;

SELECT '-- A representable value is still converted, by both functions';
WITH CAST('v', $$Enum8('v' = 127)$$) AS e, toInt8(127) AS i
SELECT accurateCastOrNull(e, 'UInt8') AS enum_or_null, accurateCastOrNull(i, 'UInt8') AS int_or_null,
       accurateCast(e, 'UInt8')       AS enum_cast,    accurateCast(i, 'UInt8')       AS int_cast
FORMAT Vertical;

SELECT '-- accurateCast throws where accurateCastOrNull returns NULL';
SELECT accurateCast(CAST('v', $$Enum8('v' = -1)$$), 'UInt8'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCast(CAST('v', $$Enum8('v' = -1)$$), 'DateTime'); -- { serverError CANNOT_CONVERT_TYPE }

SELECT '-- accurateCastOrDefault returns the target default, as it does for the integer';
WITH CAST('v', $$Enum8('v' = -1)$$) AS e, toInt8(-1) AS i
SELECT accurateCastOrDefault(e, 'UInt8') AS enum_default, accurateCastOrDefault(i, 'UInt8') AS int_default;

SELECT '-- plain CAST and toUInt8 keep wrapping the value';
WITH CAST('v', $$Enum8('v' = -1)$$) AS e
SELECT CAST(e AS UInt8) AS plain_cast, toUInt8(e) AS to_uint8;

SELECT '-- a stored column, and a Nullable Enum, behave the same as the literal';
DROP TABLE IF EXISTS enum_source;
CREATE TABLE enum_source (e Enum8('neg' = -1, 'zero' = 0, 'high' = 127)) ENGINE = MergeTree ORDER BY e;
INSERT INTO enum_source VALUES ('neg'), ('zero'), ('high');
SELECT e,
       accurateCastOrNull(e, 'UInt8')                        AS from_column,
       accurateCastOrNull(CAST(e, 'Nullable(Enum8(\'neg\' = -1, \'zero\' = 0, \'high\' = 127))'), 'UInt8') AS from_nullable,
       accurateCastOrNull(CAST(e, 'Int8'), 'UInt8')           AS from_int8
FROM enum_source
ORDER BY e;
DROP TABLE enum_source;
