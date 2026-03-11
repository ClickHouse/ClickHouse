-- SMOKE TEST
SELECT '=== Test: SMOKE TEST ===';

SELECT naturalSortKey(repeat('2', number + 1))
FROM numbers(80);

-- Test: Leading Zeros Preservation
SELECT '=== Test: Leading Zeros ===';

SELECT
    value,
    naturalSortKey(value) AS sort_key
FROM (
    SELECT arrayJoin([
        'file02.txt',
        'file003.txt',
        'file00005.txt',
        'file1.txt',
        'file10.txt',
        'file0004.txt'
    ]) AS value
)
ORDER BY naturalSortKey(value);

-- Test: Multiple Number Segments
SELECT '=== Test: Multiple Number Segments ===';

SELECT
    value,
    naturalSortKey(value) AS sort_key
FROM (
    SELECT arrayJoin([
        'v1.002.3',
        'v01.2.03',
        'v001.0002.0003',
        'v1.10.3',
        'v1.2.3',
        'v10.2.3',
        'v1.2.10'
    ]) AS value
)
ORDER BY naturalSortKey(value), value;

-- Test: Unicode Characters
SELECT '=== Test: Unicode Characters ===';

SELECT
    value,
    naturalSortKey(value) AS sort_key
FROM (
    SELECT arrayJoin([
        '文件1.txt',
        'файл1.txt',
        'файл2.txt',
        '文件10.txt',
        'αρχείο10.txt',
        'файл10.txt',
        'αρχείο1.txt',
        'ファイル1.txt'
    ]) AS value
)
ORDER BY naturalSortKey(value);

-- Test: Empty Strings and NULLs
SELECT '=== Test: Empty and NULL ===';

SELECT
    value,
    naturalSortKey(value) AS sort_key,
    isNull(value) AS is_null
FROM (
    SELECT arrayJoin([
        NULL,
        '',
        '1',
        '2',
        '3',
        NULL,
        '',
        '',
        'a',
        'b',
        'c',
        NULL,
        '',
        NULL,
        '4',
        '5',
        NULL,
        '',
        ''
    ]) AS value
)
ORDER BY naturalSortKey(value);

-- Test: Only Empty Strings and NULLs
SELECT '=== Test: Only Empty and NULLs ===';

SELECT
    value
FROM (
    SELECT arrayJoin([
        '',
        '',
        NULL,
        NULL,
        '',
        ''
    ]) AS value
)
ORDER BY naturalSortKey(value);

-- Test: Numbers at Different Positions
SELECT '=== Test: Number Positions ===';

SELECT
    value,
    naturalSortKey(value) AS sort_key
FROM (
    SELECT arrayJoin([
        '1file.txt',
        'file.1',
        'file10.txt',
        'file.2',
        '2file.txt',
        'file.10',
        '10file.txt',
        'file1.txt',
        'file2.txt'
    ]) AS value
)
ORDER BY naturalSortKey(value);

-- Test: Consecutive Numbers
SELECT '=== Test: Consecutive Numbers ===';

SELECT
    value,
    naturalSortKey(value) AS sort_key
FROM (
    SELECT arrayJoin([
        'file23.txt',
        'a1b2c10',
        'file123456.txt',
        'file1234.txt',
        'a10b2c3',
        'a1b10c3',
        'a1b2c3',
        'file234.txt',
        'file12.txt'
    ]) AS value
)
ORDER BY naturalSortKey(value);

-- Test: Zero-Only Numbers
SELECT '=== Test: Zero Numbers ===';

SELECT
    value,
    naturalSortKey(value) AS sort_key
FROM (
    SELECT arrayJoin([
        'file00.txt',
        'file1.txt',
        'file00001.txt',
        'file0000.txt',
        'file0.txt',
        'file000.txt'
    ]) AS value
)
ORDER BY naturalSortKey(value), value;

-- Test: Long Strings
SELECT '=== Test: Long Strings ===';

SELECT
    value,
    naturalSortKey(value) AS sort_key,
    length(value) AS len
FROM (
    SELECT arrayJoin([
        repeat('a', 1000) || '10' || repeat('b', 1000),
        repeat('a', 1000) || '2' || repeat('b', 1000),
        repeat('a', 1000) || '1' || repeat('b', 1000)
    ]) AS value
)
ORDER BY naturalSortKey(value);

-- Test: Real-world Patterns
SELECT '=== Test: Real-world Patterns ===';

SELECT
    value,
    naturalSortKey(value) AS sort_key
FROM (
    SELECT arrayJoin([
        '1.0.1',
        '192.168.1.10',
        '192.168.10.1',
        '2024-01-02',
        '1.10.0',
        '2024-10-01',
        '2024-01-10',
        '1.1.0',
        '10.0.0',
        '192.168.1.1',
        '2024-01-01',
        '1.0.0',
        '192.168.1.2',
        '2.0.0',
        '1.0.10'
    ]) AS value
)
ORDER BY naturalSortKey(value);

-- Test: Real-wrld Patterns with few columns
SELECT '=== Test: Real-world Patterns Few Columns ===';
SELECT
    file,
    file_size
FROM (
    SELECT 'file1.txt' AS file, 10 as file_size UNION ALL
    SELECT 'file10.txt' AS file, 5 as file_size UNION ALL
    SELECT 'file2.txt' AS file, 15 as file_size UNION ALL
    SELECT 'file20.txt' AS file, 8 as file_size
)
ORDER BY naturalSortKey(file);