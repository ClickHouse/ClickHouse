-- Tests for auto-conversion of non-String haystack in LIKE expressions
-- Issue #18196

-- Integer types
SELECT like(1, '1%');
SELECT like(100, '1%');
SELECT like(255, '2%');
SELECT like(toInt8(-5), '-5');
SELECT like(toUInt64(12345), '123%');

-- Float types
SELECT like(toFloat32(3.14), '3.%');
SELECT like(toFloat64(2.718), '2.7%');

-- Decimal types
SELECT like(toDecimal32(1.5, 2), '1.5%');

-- Date/DateTime types
SELECT like(toDate('2024-01-15'), '2024%');
SELECT like(toDateTime('2024-06-01 12:00:00'), '2024-06%');

-- IPv4 / IPv6
SELECT like(toIPv4('192.168.1.1'), '192.168%');
SELECT like(toIPv6('::1'), '%1');

-- UUID
SELECT like(toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'), '61f0c404%');

-- Variants: notLike, ilike, notILike
SELECT notLike(42, '5%');
SELECT ilike(100, '1%');
SELECT notILike(99, '1%');

-- LIKE in SQL syntax
SELECT 1 LIKE '1%';
SELECT 255 LIKE '25%';

-- CTE usage (reproducer from issue #18196)
WITH t AS (SELECT 1 AS a UNION ALL SELECT 10)
SELECT a FROM t WHERE a LIKE '1%'
ORDER BY a;

-- Nullable (framework strips Nullable before calling the function)
SELECT like(toNullable(42), '4%');
SELECT like(CAST(NULL AS Nullable(UInt32)), '4%');

-- Negative matches (should return 0)
SELECT like(42, '9%');
SELECT like(100, '%99');
SELECT notLike(1, '1%');
SELECT like(255, '1%');
SELECT ilike(42, '9%');

-- LowCardinality (the common, non-suspicious case)
SELECT like(toLowCardinality('hello'), 'hel%');

-- Non-const column: exercises the vectorConstant code path (not constant folding)
SELECT count() FROM numbers(100) WHERE number LIKE '5%';

-- Empty pattern (matches nothing for a non-empty string)
SELECT like(42, '');

-- Setting disabled: should error
SET allow_implicit_string_conversion_in_like = 0;
SELECT like(1, '1%');  -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}

-- Re-enable and confirm it works again
SET allow_implicit_string_conversion_in_like = 1;
SELECT like(1, '1%');

-- Array should always error regardless of setting
SELECT like([1, 2, 3], '1%');  -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}

-- String/FixedString still work (no regression)
SELECT like('hello', 'hell%');
SELECT like(toFixedString('world', 5), 'wor%');
