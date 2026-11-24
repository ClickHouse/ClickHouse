SELECT 'Test string subscript operator';
-- Basic single byte access
SELECT 'ClickHouse'[1];
SELECT 'ClickHouse'[2];
SELECT 'ClickHouse'[10];

-- Regression tests for short literals
SELECT 'abc'[3];
SELECT 'abc'[-1];

-- Negative indexing (from end)
SELECT 'ClickHouse'[-1];
SELECT 'ClickHouse'[-5];

-- Single byte strings
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
DROP TABLE IF EXISTS test_string_slice;
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
SELECT 'abc'[NULL];

-- The colon of a ternary operator is not a slice
SELECT [1, 2, 3][1 > 0 ? 2 : 3];
SELECT 'abc'[1 > 0 ? 2 : 3];

-- Out of bounds error tests
SELECT 'abc'[4]; -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT 'abc'[0]; -- { serverError ZERO_ARRAY_OR_TUPLE_INDEX }
SELECT 'abc'[-10]; -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT ''[1]; -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT 'ClickHouse'[-20]; -- { serverError ARGUMENT_OUT_OF_BOUND }

-- Out of bounds slice tests
SELECT 'abc'[1:10]; -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT 'ab'[-5:-1]; -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT 'abc'[10:20]; -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT 'ClickHouse'[3:1]; -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT 'abc'[0:2]; -- { serverError ZERO_ARRAY_OR_TUPLE_INDEX }
SELECT 'abc'[1:0]; -- { serverError ZERO_ARRAY_OR_TUPLE_INDEX }

-- Invalid position types
SELECT 'abc'['a']; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT 'abc'[1.5]; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT 'abc'[(1, 2, 3)]; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT 'abc'[1:'a']; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT [1, 2, 3][1:2]; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT 'LowCardinality source with a constant position';
SELECT s, s[1], s[-1], s[1:2], toTypeName(s[1]) FROM (SELECT toLowCardinality(arrayJoin(['abc', 'def'])) AS s) ORDER BY s;
SELECT s, s[1], s[2:3], toTypeName(s[1]) FROM (SELECT CAST(arrayJoin(['abc', NULL]) AS LowCardinality(Nullable(String))) AS s) ORDER BY s;
SELECT s[1] FROM (SELECT toLowCardinality(arrayJoin(['abc', ''])) AS s); -- { serverError ARGUMENT_OUT_OF_BOUND }

SELECT 'Unsigned positions above the maximum of Int64';
SELECT 'abc'[18446744073709551615]; -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT 'abc'[9223372036854775808]; -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT 'abc'[-9223372036854775808]; -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT materialize('abc')[materialize(18446744073709551615)]; -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT 'abc'[materialize(18446744073709551615)]; -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT 'abc'[2:18446744073709551615]; -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT 'abc'[18446744073709551615:3]; -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT materialize('abc')[materialize(2):materialize(18446744073709551615)]; -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT arrayElementOrNull('abc', 18446744073709551615), arrayElementOrNull('abc', (2, 18446744073709551615)), arrayElementOrNull('abc', -9223372036854775808);
SELECT arrayElementOrNull(materialize('abc'), materialize(18446744073709551615)), arrayElementOrNull(materialize('abc'), (materialize(18446744073709551615), 3));

SELECT 'arrayElementOrNull';
SELECT arrayElementOrNull('abc', 2), toTypeName(arrayElementOrNull('abc', 2));
SELECT arrayElementOrNull('abc', 4), arrayElementOrNull('abc', 0), arrayElementOrNull('abc', -4), arrayElementOrNull('', 1);
SELECT arrayElementOrNull('abc', (2, 3)), arrayElementOrNull('abc', (3, 2)), arrayElementOrNull('abc', (1, 4)), arrayElementOrNull('abc', (0, 1));
SELECT s, i, arrayElementOrNull(s, i), arrayElementOrNull(s, (i, -1)) FROM VALUES('s String, i Int8', ('abc', 1), ('abc', 4), ('', 1), ('xyz', -1), ('xyz', 0)) ORDER BY s, i;
SELECT arrayElementOrNull(s, 1), toTypeName(arrayElementOrNull(s, 1)) FROM (SELECT toLowCardinality(arrayJoin(['abc', ''])) AS s) ORDER BY s;

SELECT 'Nullable strings and positions';
SELECT toNullable('abc')[1], toTypeName(toNullable('abc')[1]);
SELECT 'abc'[toNullable(1)], toTypeName('abc'[toNullable(1)]);
SELECT 'abc'[CAST(NULL AS Nullable(Int32)):2], toTypeName('abc'[CAST(NULL AS Nullable(Int32)):2]);
SELECT 'abc'[NULL:2], 'abc'[1:NULL], toTypeName('abc'[NULL:2]);
SELECT 'abc'[toNullable(1):toNullable(2)], toTypeName('abc'[toNullable(1):toNullable(2)]);
SELECT s, i, s[i], s[i:-1] FROM VALUES('s Nullable(String), i Nullable(Int32)', ('abc', 1), (NULL, 1), ('abc', NULL), ('', NULL), (NULL, 0)) ORDER BY s, i;
SELECT toNullable('')[1]; -- { serverError ARGUMENT_OUT_OF_BOUND }

SELECT 'Guards under short-circuit evaluation';
SELECT if(s != '', s[1], '') FROM VALUES('s String', ('abc'), ('')) SETTINGS short_circuit_function_evaluation = 'enable';
SELECT if(length(s) >= 2, s[1:2], '') FROM VALUES('s String', ('abc'), ('a')) SETTINGS short_circuit_function_evaluation = 'enable';
SELECT multiIf(s = '', '-', s[-1]) FROM VALUES('s String', ('abc'), ('')) SETTINGS short_circuit_function_evaluation = 'enable';
SELECT s FROM VALUES('s String', ('abc'), ('')) WHERE s != '' AND s[1] = 'a' SETTINGS short_circuit_function_evaluation = 'enable';

SELECT 'Formatting keeps an aliased slice tuple';
SELECT formatQuerySingleLine('SELECT ''abc''[(number + 1, 3) AS bounds] AS piece, bounds FROM numbers(1)');
SELECT formatQuerySingleLine('SELECT s[1:2], s[-3:-1], s[a:b], s[(a ? 1 : 2):3], s[1:a ? 2 : 3]');
SELECT 'abc'[(number + 1, 3) AS bounds] AS piece, bounds FROM numbers(1);

DROP VIEW IF EXISTS test_string_slice_view;
CREATE VIEW test_string_slice_view AS SELECT 'abc'[(number + 1, 3) AS bounds] AS piece, bounds, 'ClickHouse'[number + 1:-6] AS head FROM numbers(1);
DETACH TABLE test_string_slice_view;
ATTACH TABLE test_string_slice_view;
SELECT * FROM test_string_slice_view;
DROP VIEW test_string_slice_view;
