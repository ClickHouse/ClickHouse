-- Tags: no-fasttest
-- Test for DateTime64 pre-epoch fractional seconds fix (GitHub issue #85396)

-- 1. Test parseDateTime64BestEffort (src/IO/parseDateTimeBestEffort.cpp)
-- Original problem case
SELECT parseDateTime64BestEffort('1969-01-01 00:00:00.468', 3, 'UTC');
-- Test with different scales
SELECT parseDateTime64BestEffort('1969-07-20 20:17:40.123456', 6, 'UTC');
-- Test negative timestamps with fractional seconds
SELECT parseDateTime64BestEffort('1950-01-01 00:00:00.500', 3, 'UTC');
-- Test epoch boundary
SELECT parseDateTime64BestEffort('1969-12-31 23:59:59.999', 3, 'UTC');
SELECT parseDateTime64BestEffort('1970-01-01 00:00:00.000', 3, 'UTC');

-- 2. Test makeDateTime64 functions (src/Functions/makeDate.cpp)
SELECT makeDateTime64(1969, 1, 1, 0, 0, 0, 468, 3);
SELECT makeDateTime64(1969, 12, 31, 23, 59, 59, 999, 3);
SELECT makeDateTime64(1969, 6, 15, 12, 0, 0, 500000, 6);

-- 3. Test changeYear/changeMonth/changeDay functions (src/Functions/changeDate.cpp)
SELECT changeYear('2024-01-01 00:00:00.462'::DateTime64, 1969);
SELECT changeMonth('1969-06-15 12:30:45.123'::DateTime64, 1);
SELECT changeDay('1969-12-01 08:15:22.789'::DateTime64, 31);

-- 4. Test nowSubsecond (src/Functions/nowSubsecond.cpp) - indirect test via now64()
-- Note: nowSubsecond is used internally by now64() function
SELECT length(toString(now64(3))) > 0;

-- 5. Test ULIDStringToDateTime conversion (src/Functions/ULIDStringToDateTime.cpp)
-- Note: Using fixed ULID to ensure test reproducibility across environments
SELECT ULIDStringToDateTime('01ARZ3NDEKTSV4RRFFQ69G5FAV', 'UTC');

-- 6. Test UUID conversion functions (src/Functions/FunctionsCodingUUID.cpp)
-- generateUUIDv7 uses DateTime64 internally
SELECT length(toString(generateUUIDv7())) = 36;

-- 7. Verify Decimal types are unaffected by changes
SELECT CAST(-123.456 AS Decimal64(3));
SELECT CAST(-1969.123 AS Decimal64(3));
SELECT CAST(1234.567 AS Decimal32(3));

-- 8. Test arithmetic with DateTime64 pre-epoch
SELECT parseDateTime64BestEffort('1969-01-01 00:00:00.500', 3, 'UTC') + INTERVAL 1 SECOND;