SET explain_query_plan_default = 'legacy';
-- The rewrite is what most of this test asserts, and CI randomizes it off; pin it on. The two
-- queries that need it off carry their own `SETTINGS optimize_rewrite_has_to_in = 0`.
SET optimize_rewrite_has_to_in = 1;

-- { echo }

-- `notHas` is the negation of `has` with identical argument and NULL semantics.
SELECT notHas([1, 2, 3], 2), notHas([1, 2, 3], 4), notHas([], 1);

-- NULL semantics match `has`: a NULL needle matches only NULL array elements, and the result is
-- always plain 0/1, never NULL.
SELECT notHas([1, 2], NULL), notHas([NULL, 1], NULL), notHas([NULL, 1], 2);
SELECT notHas([1, 2], CAST(2, 'Nullable(UInt8)')), notHas([NULL, 2], CAST(2, 'Nullable(UInt8)')), notHas([NULL], CAST(NULL, 'Nullable(UInt8)'));

-- Strings, FixedString, and cross-type numeric comparison by value.
SELECT notHas(['a', 'b'], 'b'), notHas(['a', 'b'], 'c'), notHas(['a', 'b'], toFixedString('a', 1));
SELECT notHas([toFixedString('aa', 2), toFixedString('bb', 2)], 'bb');
SELECT notHas([1, 2, 3], 2.0), notHas([1.5, 2.5], 2), notHas([1.0, 2.0], 2), notHas([-1, 0, 1], 0.0);
SELECT notHas([toDecimal32(1.1, 2), toDecimal32(2.2, 2)], toDecimal32(2.2, 2));

-- Dates, UUIDs, and Enums. Like `has`, an Enum element matches a needle of the Enum type, not the
-- name as a plain string.
SELECT notHas([toDate('2026-01-01'), toDate('2026-01-02')], toDate('2026-01-02'));
SELECT notHas([toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0')], toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'));
SELECT notHas(CAST(['a', 'b'], 'Array(Enum8(\'a\' = 1, \'b\' = 2))'), CAST('b', 'Enum8(\'a\' = 1, \'b\' = 2)'));
SELECT notHas(CAST(['a', 'b'], 'Array(Enum8(\'a\' = 1, \'b\' = 2))'), 'b');

-- LowCardinality on either side.
SELECT notHas(CAST(['x', 'y'], 'Array(LowCardinality(String))'), 'y'), notHas(['x', 'y'], toLowCardinality('y'));

-- Composite element types compared by value: tuples and nested arrays.
SELECT notHas([(1, 'a'), (2, 'b')], (2, 'b')), notHas([(1, 'a')], (1, 'b'));
SELECT notHas([[1, 2], [3]], [3]), notHas([[1, 2], [3]], [4]);

-- Maps check keys, with both string and non-string key types.
SELECT notHas(map('a', 1, 'b', 2), 'b'), notHas(map('a', 1, 'b', 2), 'c');
SELECT notHas(map(1, 'a', 2, 'b'), 2), notHas(map(1, 'a', 2, 'b'), 3);

-- JSON checks paths, including nested ones.
SELECT notHas(CAST('{"a" : 1, "b" : {"c" : 2}}', 'JSON'), 'a'), notHas(CAST('{"a" : 1}', 'JSON'), 'b'), notHas(CAST('{"b" : {"c" : 2}}', 'JSON'), 'b.c');

-- All const/column combinations of the two arguments dispatch the same way.
SELECT notHas([1, 2], 2), notHas(materialize([1, 2]), 2), notHas([1, 2], materialize(2)), notHas(materialize([1, 2]), materialize(3));

-- Row-by-row agreement with `NOT has` over numeric and string data with NULLs.
SELECT count() FROM (SELECT [number, number + 1, NULL] AS arr, if(number % 3 = 0, NULL, number) AS x FROM numbers(100))
WHERE notHas(arr, x) IS DISTINCT FROM NOT has(arr, x);
SELECT count() FROM (SELECT [toString(number % 7), 'x', NULL] AS arr, if(number % 3 = 0, NULL, toString(number % 5)) AS x FROM numbers(100))
WHERE notHas(arr, x) IS DISTINCT FROM NOT has(arr, x);

-- `notHas` on a constant array is rewritten to `notIn` by the analyzer (`optimize_rewrite_has_to_in`),
-- so it executes through the set machinery and prunes by the primary key like `NOT IN`.
-- The rewrite is a query tree pass, which exists only in the analyzer.
SET enable_analyzer = 1;
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT materialize(4) AS x WHERE notHas([1, 2], x);

-- A NULL array element blocks the rewrite (`has` and `in` treat NULLs differently), so `notHas` stays.
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT materialize(4) AS x WHERE notHas([1, NULL], x);

DROP TABLE IF EXISTS test_not_has;
CREATE TABLE test_not_has (x UInt64) ENGINE = MergeTree
ORDER BY x
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_not_has SELECT intDiv(number, 4) FROM numbers(24);

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_not_has WHERE notHas([1], x)) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules:%/%';
SELECT count() FROM test_not_has WHERE notHas([1], x);
SELECT count() FROM test_not_has WHERE notHas([1], x) SETTINGS use_primary_key = 0;
SELECT count() FROM test_not_has WHERE NOT has([1], x);
SELECT count() FROM test_not_has WHERE x NOT IN (1);

-- Without the rewrite, `notHas` executes as-is; the results stay the same.
SELECT count() FROM test_not_has WHERE notHas([1], x) SETTINGS optimize_rewrite_has_to_in = 0;

-- When the rewrite is blocked (NULL array element), `notHas` executes as-is; the results stay the same.
SELECT count() FROM test_not_has WHERE notHas([1, NULL], x);
SELECT count() FROM test_not_has WHERE notHas([1, NULL], x) SETTINGS use_primary_key = 0;

-- Float arrays must not become primary-key set atoms: `has` treats NaN as unequal to itself,
-- while the set-index comparator treats NaNs as equal. Both primary-key modes must return all rows.
DROP TABLE IF EXISTS test_not_has_float;
CREATE TABLE test_not_has_float (x Float64) ENGINE = MergeTree
ORDER BY x
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
INSERT INTO test_not_has_float VALUES (nan), (1), (2);
SELECT count() FROM test_not_has_float WHERE notHas([CAST('nan', 'Float64')], x);
SELECT count() FROM test_not_has_float WHERE notHas([CAST('nan', 'Float64')], x) SETTINGS use_primary_key = 0;
SELECT count() FROM test_not_has_float WHERE NOT has([CAST('nan', 'Float64')], x);
SELECT count() FROM test_not_has_float WHERE NOT has([CAST('nan', 'Float64')], x) SETTINGS use_primary_key = 0;
DROP TABLE test_not_has_float;

-- The same rule applies when a tuple array element contains a float.
DROP TABLE IF EXISTS test_not_has_float_tuple;
CREATE TABLE test_not_has_float_tuple (x Float64, y UInt8) ENGINE = MergeTree
ORDER BY (x, y)
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
INSERT INTO test_not_has_float_tuple VALUES (nan, 1), (1, 1), (2, 1);
SELECT count() FROM test_not_has_float_tuple WHERE notHas([tuple(CAST('nan', 'Float64'), toUInt8(1))], (x, y));
SELECT count() FROM test_not_has_float_tuple WHERE notHas([tuple(CAST('nan', 'Float64'), toUInt8(1))], (x, y)) SETTINGS use_primary_key = 0;
SELECT count() FROM test_not_has_float_tuple WHERE NOT has([tuple(CAST('nan', 'Float64'), toUInt8(1))], (x, y));
SELECT count() FROM test_not_has_float_tuple WHERE NOT has([tuple(CAST('nan', 'Float64'), toUInt8(1))], (x, y)) SETTINGS use_primary_key = 0;
DROP TABLE test_not_has_float_tuple;

-- Empty array: `has` matches nothing, `notHas` matches everything.
SELECT count() FROM test_not_has WHERE has([], x);
SELECT count() FROM test_not_has WHERE notHas([], x);
SELECT count() FROM test_not_has WHERE NOT has([], x);

DROP TABLE test_not_has;
