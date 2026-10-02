-- `hasAll` and `hasAny` on integer arrays, compared with a reference that checks each element of the second array with `has`.
-- Lengths cross the 16-element groups and 128-byte blocks of the vectorized search, small value ranges make both found
-- and missing values common, and nulls are placed in neither, either or both arrays, including a first array of only nulls.

DROP TABLE IF EXISTS arrays;
CREATE TABLE arrays (a Array(Nullable(Int64)), b Array(Nullable(Int64))) ENGINE = Memory;

-- Each of the 12 * 6 * 3 * 5 * 3 = 3240 rows is a different combination of the parameters below.
INSERT INTO arrays
SELECT
    arrayMap(i -> if((null_mode IN (1, 3) AND cityHash64(number, i, 'first null') % 8 = 0) OR null_mode = 4, NULL,
        toInt64(cityHash64(number, i, 'first') % value_range)), range(first_length)) AS a,
    arrayMap(j -> if(null_mode IN (2, 3, 4) AND cityHash64(number, j, 'second null') % 8 = 0, NULL,
        multiIf(
            second_mode = 2 AND j = second_length - 1, 120,
            second_mode = 0, toInt64(cityHash64(number, j, 'second') % value_range),
            a[cityHash64(number, j, 'position') % first_length + 1])), range(second_length)) AS b
FROM
(
    SELECT
        number,
        [1, 15, 16, 17, 31, 32, 33, 64, 127, 128, 129, 300][number % 12 + 1] AS first_length,
        [1, 15, 16, 17, 33, 100][intDiv(number, 12) % 6 + 1] AS second_length,
        [2, 16, 100][intDiv(number, 72) % 3 + 1] AS value_range,
        -- 0: no nulls, 1: nulls in the first array, 2: nulls in the second array, 3: in both, 4: the first array is all nulls.
        intDiv(number, 216) % 5 AS null_mode,
        -- 0: independent values, 1: values taken from the first array, 2: the same with a missing value at the end.
        intDiv(number, 1080) AS second_mode
    FROM numbers(3240)
);

-- The mismatch columns must be 0; the other counts show that both results occur.
SELECT 'Int8', 'Nullable', count(),
    countIf(hasAll(x, y)) AS all_true, countIf(hasAll(x, y) != arrayAll(v -> has(x, v), y)) AS all_mismatches,
    countIf(hasAny(x, y)) AS any_true, countIf(hasAny(x, y) != arrayExists(v -> has(x, v), y)) AS any_mismatches
FROM (SELECT CAST(a, 'Array(Nullable(Int8))') AS x, CAST(b, 'Array(Nullable(Int8))') AS y FROM arrays);

SELECT 'Int8', 'not Nullable', count(),
    countIf(hasAll(x, y)) AS all_true, countIf(hasAll(x, y) != arrayAll(v -> has(x, v), y)) AS all_mismatches,
    countIf(hasAny(x, y)) AS any_true, countIf(hasAny(x, y) != arrayExists(v -> has(x, v), y)) AS any_mismatches
FROM (SELECT CAST(arrayMap(v -> assumeNotNull(v), a), 'Array(Int8)') AS x, CAST(arrayMap(v -> assumeNotNull(v), b), 'Array(Int8)') AS y FROM arrays);

SELECT 'Int8', 'mixed', count(),
    countIf(hasAll(x, y)) AS all_true, countIf(hasAll(x, y) != arrayAll(v -> has(x, v), y)) AS all_mismatches,
    countIf(hasAny(x, y)) AS any_true, countIf(hasAny(x, y) != arrayExists(v -> has(x, v), y)) AS any_mismatches
FROM (SELECT CAST(a, 'Array(Nullable(Int8))') AS x, CAST(arrayMap(v -> assumeNotNull(v), b), 'Array(Int8)') AS y FROM arrays);

SELECT 'UInt8', 'Nullable', count(),
    countIf(hasAll(x, y)) AS all_true, countIf(hasAll(x, y) != arrayAll(v -> has(x, v), y)) AS all_mismatches,
    countIf(hasAny(x, y)) AS any_true, countIf(hasAny(x, y) != arrayExists(v -> has(x, v), y)) AS any_mismatches
FROM (SELECT CAST(a, 'Array(Nullable(UInt8))') AS x, CAST(b, 'Array(Nullable(UInt8))') AS y FROM arrays);

SELECT 'UInt8', 'not Nullable', count(),
    countIf(hasAll(x, y)) AS all_true, countIf(hasAll(x, y) != arrayAll(v -> has(x, v), y)) AS all_mismatches,
    countIf(hasAny(x, y)) AS any_true, countIf(hasAny(x, y) != arrayExists(v -> has(x, v), y)) AS any_mismatches
FROM (SELECT CAST(arrayMap(v -> assumeNotNull(v), a), 'Array(UInt8)') AS x, CAST(arrayMap(v -> assumeNotNull(v), b), 'Array(UInt8)') AS y FROM arrays);

SELECT 'UInt8', 'mixed', count(),
    countIf(hasAll(x, y)) AS all_true, countIf(hasAll(x, y) != arrayAll(v -> has(x, v), y)) AS all_mismatches,
    countIf(hasAny(x, y)) AS any_true, countIf(hasAny(x, y) != arrayExists(v -> has(x, v), y)) AS any_mismatches
FROM (SELECT CAST(a, 'Array(Nullable(UInt8))') AS x, CAST(arrayMap(v -> assumeNotNull(v), b), 'Array(UInt8)') AS y FROM arrays);

SELECT 'Int16', 'Nullable', count(),
    countIf(hasAll(x, y)) AS all_true, countIf(hasAll(x, y) != arrayAll(v -> has(x, v), y)) AS all_mismatches,
    countIf(hasAny(x, y)) AS any_true, countIf(hasAny(x, y) != arrayExists(v -> has(x, v), y)) AS any_mismatches
FROM (SELECT CAST(a, 'Array(Nullable(Int16))') AS x, CAST(b, 'Array(Nullable(Int16))') AS y FROM arrays);

SELECT 'Int16', 'not Nullable', count(),
    countIf(hasAll(x, y)) AS all_true, countIf(hasAll(x, y) != arrayAll(v -> has(x, v), y)) AS all_mismatches,
    countIf(hasAny(x, y)) AS any_true, countIf(hasAny(x, y) != arrayExists(v -> has(x, v), y)) AS any_mismatches
FROM (SELECT CAST(arrayMap(v -> assumeNotNull(v), a), 'Array(Int16)') AS x, CAST(arrayMap(v -> assumeNotNull(v), b), 'Array(Int16)') AS y FROM arrays);

SELECT 'Int16', 'mixed', count(),
    countIf(hasAll(x, y)) AS all_true, countIf(hasAll(x, y) != arrayAll(v -> has(x, v), y)) AS all_mismatches,
    countIf(hasAny(x, y)) AS any_true, countIf(hasAny(x, y) != arrayExists(v -> has(x, v), y)) AS any_mismatches
FROM (SELECT CAST(a, 'Array(Nullable(Int16))') AS x, CAST(arrayMap(v -> assumeNotNull(v), b), 'Array(Int16)') AS y FROM arrays);

SELECT 'Int32', 'Nullable', count(),
    countIf(hasAll(x, y)) AS all_true, countIf(hasAll(x, y) != arrayAll(v -> has(x, v), y)) AS all_mismatches,
    countIf(hasAny(x, y)) AS any_true, countIf(hasAny(x, y) != arrayExists(v -> has(x, v), y)) AS any_mismatches
FROM (SELECT CAST(a, 'Array(Nullable(Int32))') AS x, CAST(b, 'Array(Nullable(Int32))') AS y FROM arrays);

SELECT 'Int32', 'not Nullable', count(),
    countIf(hasAll(x, y)) AS all_true, countIf(hasAll(x, y) != arrayAll(v -> has(x, v), y)) AS all_mismatches,
    countIf(hasAny(x, y)) AS any_true, countIf(hasAny(x, y) != arrayExists(v -> has(x, v), y)) AS any_mismatches
FROM (SELECT CAST(arrayMap(v -> assumeNotNull(v), a), 'Array(Int32)') AS x, CAST(arrayMap(v -> assumeNotNull(v), b), 'Array(Int32)') AS y FROM arrays);

SELECT 'Int32', 'mixed', count(),
    countIf(hasAll(x, y)) AS all_true, countIf(hasAll(x, y) != arrayAll(v -> has(x, v), y)) AS all_mismatches,
    countIf(hasAny(x, y)) AS any_true, countIf(hasAny(x, y) != arrayExists(v -> has(x, v), y)) AS any_mismatches
FROM (SELECT CAST(a, 'Array(Nullable(Int32))') AS x, CAST(arrayMap(v -> assumeNotNull(v), b), 'Array(Int32)') AS y FROM arrays);

SELECT 'Int64', 'Nullable', count(),
    countIf(hasAll(x, y)) AS all_true, countIf(hasAll(x, y) != arrayAll(v -> has(x, v), y)) AS all_mismatches,
    countIf(hasAny(x, y)) AS any_true, countIf(hasAny(x, y) != arrayExists(v -> has(x, v), y)) AS any_mismatches
FROM (SELECT CAST(a, 'Array(Nullable(Int64))') AS x, CAST(b, 'Array(Nullable(Int64))') AS y FROM arrays);

SELECT 'Int64', 'not Nullable', count(),
    countIf(hasAll(x, y)) AS all_true, countIf(hasAll(x, y) != arrayAll(v -> has(x, v), y)) AS all_mismatches,
    countIf(hasAny(x, y)) AS any_true, countIf(hasAny(x, y) != arrayExists(v -> has(x, v), y)) AS any_mismatches
FROM (SELECT CAST(arrayMap(v -> assumeNotNull(v), a), 'Array(Int64)') AS x, CAST(arrayMap(v -> assumeNotNull(v), b), 'Array(Int64)') AS y FROM arrays);

SELECT 'Int64', 'mixed', count(),
    countIf(hasAll(x, y)) AS all_true, countIf(hasAll(x, y) != arrayAll(v -> has(x, v), y)) AS all_mismatches,
    countIf(hasAny(x, y)) AS any_true, countIf(hasAny(x, y) != arrayExists(v -> has(x, v), y)) AS any_mismatches
FROM (SELECT CAST(a, 'Array(Nullable(Int64))') AS x, CAST(arrayMap(v -> assumeNotNull(v), b), 'Array(Int64)') AS y FROM arrays);

SELECT 'UInt64', 'Nullable', count(),
    countIf(hasAll(x, y)) AS all_true, countIf(hasAll(x, y) != arrayAll(v -> has(x, v), y)) AS all_mismatches,
    countIf(hasAny(x, y)) AS any_true, countIf(hasAny(x, y) != arrayExists(v -> has(x, v), y)) AS any_mismatches
FROM (SELECT CAST(a, 'Array(Nullable(UInt64))') AS x, CAST(b, 'Array(Nullable(UInt64))') AS y FROM arrays);

SELECT 'UInt64', 'not Nullable', count(),
    countIf(hasAll(x, y)) AS all_true, countIf(hasAll(x, y) != arrayAll(v -> has(x, v), y)) AS all_mismatches,
    countIf(hasAny(x, y)) AS any_true, countIf(hasAny(x, y) != arrayExists(v -> has(x, v), y)) AS any_mismatches
FROM (SELECT CAST(arrayMap(v -> assumeNotNull(v), a), 'Array(UInt64)') AS x, CAST(arrayMap(v -> assumeNotNull(v), b), 'Array(UInt64)') AS y FROM arrays);

SELECT 'UInt64', 'mixed', count(),
    countIf(hasAll(x, y)) AS all_true, countIf(hasAll(x, y) != arrayAll(v -> has(x, v), y)) AS all_mismatches,
    countIf(hasAny(x, y)) AS any_true, countIf(hasAny(x, y) != arrayExists(v -> has(x, v), y)) AS any_mismatches
FROM (SELECT CAST(a, 'Array(Nullable(UInt64))') AS x, CAST(arrayMap(v -> assumeNotNull(v), b), 'Array(UInt64)') AS y FROM arrays);

DROP TABLE arrays;
