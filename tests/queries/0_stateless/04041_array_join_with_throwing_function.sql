-- Verify functions that can throw are correctly handled during ARRAY JOIN lift up to 
-- avoid exceptions on rows that are filtered out by empty arrays.

SET enable_lazy_columns_replication = 0;

-- intDiv on division by zero
SELECT '-- intDiv on division by zero';
SELECT intDiv(a, b), arr_val FROM (
    SELECT a, b, arr FROM VALUES('a UInt64, b UInt64, arr Array(UInt8)', (10, 2, [1]), (10, 0, []))
) ARRAY JOIN arr AS arr_val;

-- toDate on invalid string
SELECT '-- toDate on invalid string';
SELECT toDate(s), arr_val FROM (
    SELECT s, arr FROM VALUES('s String, arr Array(UInt8)', ('2024-01-01', [1]), ('not-a-date', []))
) ARRAY JOIN arr AS arr_val;

-- CAST on invalid conversion
SELECT '-- CAST on invalid conversion';
SELECT CAST(s AS UInt64), arr_val FROM (
    SELECT s, arr FROM VALUES('s String, arr Array(UInt8)', ('123', [1]), ('not-a-number', []))
) ARRAY JOIN arr AS arr_val;

-- repeat on too large result
SELECT '-- repeat on too large result';
SELECT length(repeat(s, n)), arr_val FROM (
    SELECT s, n, arr FROM VALUES('s String, n UInt64, arr Array(UInt8)', ('x', 1, [1]), ('x', 1000000000, []))
) ARRAY JOIN arr AS arr_val;

-- parseDateTime on invalid format
SELECT '-- parseDateTime on invalid format';
SELECT parseDateTime(s, '%Y-%m-%d'), arr_val FROM (
    SELECT s, arr FROM VALUES('s String, arr Array(UInt8)', ('2024-01-01', [1]), ('not-a-date', []))
) ARRAY JOIN arr AS arr_val;

-- arrayZip on mismatched lengths
SELECT '-- arrayZip on mismatched lengths';
SELECT arrayZip(a1, a2), arr_val FROM (
    SELECT a1, a2, arr FROM VALUES('a1 Array(UInt8), a2 Array(UInt8), arr Array(UInt8)', ([1], [1], [1]), ([1,2], [1], []))
) ARRAY JOIN arr AS arr_val;

-- geoToH3 on invalid coordinates
SELECT '-- geoToH3 on invalid coordinates';
SELECT geoToH3(lon, lat, 5), arr_val FROM (
    SELECT lon, lat, arr FROM VALUES('lon Float64, lat Float64, arr Array(UInt8)', (37.79, 55.8, [1]), (nan, nan, []))
) ARRAY JOIN arr AS arr_val;

-- JSONMergePatch on invalid JSON
SELECT '-- JSONMergePatch on invalid JSON';
SELECT JSONMergePatch(j), arr_val FROM (
    SELECT j, arr FROM VALUES('j String, arr Array(UInt8)', ('{"a":1}', [1]), ('not-json', []))
) ARRAY JOIN arr AS arr_val;

-- round with scale column that exceeds range
SELECT '-- round with non-constant scale column that exceeds range';
SELECT round(x, s), arr_val FROM (
    SELECT x, s, arr FROM VALUES('x Float64, s Int64, arr Array(UInt8)', (3.14159, 2, [1]), (3.14159, 100000, []))
) ARRAY JOIN arr AS arr_val;

-- sleep on excessive duration
SELECT '-- sleep on excessive duration';
SELECT sleep(99999), arr_val FROM (
    SELECT arr FROM VALUES('arr Array(UInt8)', ([]))
) ARRAY JOIN arr AS arr_val
SETTINGS function_sleep_max_microseconds_per_block = 10000;

-- file() on nonexistent file
SELECT '-- file() on nonexistent file';
SELECT length(file(path)), arr_val FROM (
    SELECT path, arr FROM VALUES('path String, arr Array(UInt8)', ('nonexistent.txt', []))
) ARRAY JOIN arr AS arr_val;

-- Aligned ARRAY JOIN with multiple columns
SELECT '-- Aligned ARRAY JOIN with multiple columns';
SELECT intDiv(a, b), arr1_val, arr2_val FROM (
    SELECT a, b, arr1, arr2 FROM VALUES('a UInt64, b UInt64, arr1 Array(UInt8), arr2 Array(UInt8)',
        (10, 2, [1], [10]), (10, 0, [], []))
) ARRAY JOIN arr1 AS arr1_val, arr2 AS arr2_val;

-- Aligned ARRAY JOIN with multiple columns and mismatched array lengths
SELECT '-- Aligned ARRAY JOIN with multiple columns and mismatched array lengths';
SELECT intDiv(a, b), arr1_val, arr2_val FROM (
    SELECT a, b, arr1, arr2 FROM VALUES('a UInt64, b UInt64, arr1 Array(UInt8), arr2 Array(UInt8)',
        (10, 2, [1], [10, 11]), (10, 0, [], [1]))
) ARRAY JOIN arr1 AS arr1_val, arr2 AS arr2_val; -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }

-- Aligned LEFT ARRAY JOIN with multiple columns and mismatched array lengths
SELECT '-- Aligned LEFT ARRAY JOIN with multiple columns and mismatched array lengths';
SELECT intDiv(a, b), arr1_val, arr2_val FROM (
    SELECT a, b, arr1, arr2 FROM VALUES('a UInt64, b UInt64, arr1 Array(UInt8), arr2 Array(UInt8)',
        (10, 2, [1], [10, 11]), (10, 0, [], [1]))
) LEFT ARRAY JOIN arr1 AS arr1_val, arr2 AS arr2_val; -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }

-- Unaligned ARRAY JOIN with multiple columns
SELECT '-- Unaligned ARRAY JOIN with multiple columns';
SELECT intDiv(a, b), arr1_val, arr2_val FROM (
    SELECT a, b, arr1, arr2 FROM VALUES('a UInt64, b UInt64, arr1 Array(UInt8), arr2 Array(UInt8)',
        (10, 2, [1], [10, 20]), (10, 5, [1], []), (10, 0, [], []))
) ARRAY JOIN arr1 AS arr1_val, arr2 AS arr2_val SETTINGS enable_unaligned_array_join = 1;
