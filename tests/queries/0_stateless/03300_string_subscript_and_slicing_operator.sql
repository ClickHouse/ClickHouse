SELECT 'Test string subscript operator';
-- Basic single character access
SELECT 'ClickHouse'[1];
SELECT 'ClickHouse'[2];
SELECT 'ClickHouse'[10];

-- Regression tests for short literals
SELECT 'abc'[3];
SELECT 'abc'[-1];

-- Negative indexing (from end)
SELECT 'ClickHouse'[-1];
SELECT 'ClickHouse'[-5];

-- Single character strings
SELECT 'a'[1];
SELECT 'a'[-1];

-- Range slicing
SELECT 'abc'[1:1];
SELECT 'abc'[2:3];
SELECT 'ClickHouse'[3:7];
SELECT 'ClickHouse'[-5:-1];
SELECT 'ClickHouse'[4:-2];
SELECT 'ClickHouse'[-4:9];

-- Using arrayElement function explicitly
SELECT arrayElement('ClickHouse', 1);
SELECT arrayElement('ClickHouse', -1);

-- Testing with columns
DROP TABLE IF EXISTS test_string_subscript;
CREATE TABLE test_string_subscript (s String, idx Int32) ENGINE = Memory;
INSERT INTO test_string_subscript VALUES ('hello', 1), ('world', 2), ('test', -1), ('abc', 3);

SELECT s, idx, s[idx] FROM test_string_subscript ORDER BY s;

DROP TABLE test_string_subscript;

-- Testing with tuple-based slicing from columns
CREATE TABLE test_string_slice (s String, start Int32, stop Int32) ENGINE = Memory;
INSERT INTO test_string_slice VALUES ('clickhouse', 2, 5), ('column', 1, 3), ('tuple', 2, 4);

SELECT s, start, stop, s[start:stop] FROM test_string_slice ORDER BY s;
SELECT start, stop, 'CONSTANT'[start:stop] FROM test_string_slice ORDER BY start, stop;

DROP TABLE test_string_slice;

-- Constant slice applied to varying rows
SELECT arrayJoin(['alpha', 'beta', 'gamma']) AS s, s[2:4];

-- Testing with different string operations
SELECT concat('Hello', ' ', 'World')[7];
SELECT upper('clickhouse')[1];
SELECT lower('CLICKHOUSE')[5];

-- Testing with NULL
SELECT NULL[1];

-- Out of bounds error tests (should throw ILLEGAL_INDEX error code 127)
SELECT 'abc'[4]; -- { serverError ILLEGAL_INDEX }
SELECT 'abc'[0]; -- { serverError ILLEGAL_INDEX }
SELECT 'abc'[-10]; -- { serverError ILLEGAL_INDEX }
SELECT ''[1]; -- { serverError ILLEGAL_INDEX }
SELECT 'ClickHouse'[-20]; -- { serverError ILLEGAL_INDEX }

-- Out of bounds slice tests
SELECT 'abc'[1:10]; -- { serverError ILLEGAL_INDEX }
SELECT 'ab'[-5:-1]; -- { serverError ILLEGAL_INDEX }
SELECT 'abc'[10:20]; -- { serverError ILLEGAL_INDEX }
SELECT 'ClickHouse'[3:1]; -- { serverError ILLEGAL_INDEX }

