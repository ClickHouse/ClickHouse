-- The array subscript operator with an array of indexes: `arr[indexes]` is equivalent to
-- `arrayMap(i -> arr[i], indexes)`, and `arrayElementOrNull(arr, indexes)` is equivalent to
-- `arrayMap(i -> arrayElementOrNull(arr, i), indexes)`.

SELECT '-- Basic functionality';
SELECT [10, 20, 30, 40][[2, 4, 1]];
SELECT [10, 20, 30, 40][[1, 1, 1]];
SELECT [10, 20, 30, 40][[]::Array(Int32)];
SELECT arrayElement([10, 20, 30, 40], [3, 2]);

SELECT '-- Negative indexes';
SELECT ['a', 'b', 'c'][[-1, -2]];
SELECT [10, 20, 30][[-1, 1]];

SELECT '-- Out of bounds gives the default value, like a scalar index does';
SELECT [10, 20, 30][[1, 5, 2]];
SELECT ['a', 'b'][[1, 3]];
SELECT [10, 20, 30][[0, 1, 2]];
SELECT [10, 20, 30][[-4, -3]];
SELECT []::Array(Int32)[[1, 2]];

SELECT '-- The minimal Int64 must not overflow while being negated';
SELECT [10, 20, 30][[toInt64(-9223372036854775808), toInt64(1)]];
SELECT arrayElementOrNull([10, 20, 30], [toInt64(-9223372036854775808)]);

SELECT '-- arrayElementOrNull gives NULL out of bounds';
SELECT arrayElementOrNull([10, 20, 30], [1, 5, 2]);
SELECT arrayElementOrNull([10, 20, 30], [0, 1, 2]);
SELECT arrayElementOrNull(['a', 'b'], [1, 3]);
SELECT arrayElementOrNull([]::Array(Int32), [1, 2]);

SELECT '-- A nullable element type keeps NULL out of bounds in both modes';
SELECT [1, NULL, 3][[1, 2, 3]];
SELECT [1, NULL, 3][[2]];
SELECT [1, NULL, 3][[4]];
SELECT [1, NULL, 3][[1, 4, 2]];
SELECT arrayElementOrNull([1, NULL, 3], [1, 2, 4]);

SELECT '-- Result types';
SELECT toTypeName([10, 20, 30][[1]]);
SELECT toTypeName([1, NULL, 3][[1]]);
SELECT toTypeName(arrayElementOrNull([10, 20, 30], [1]));
SELECT toTypeName(arrayElementOrNull([1, NULL, 3], [1]));
SELECT toTypeName(['a', 'b']::Array(LowCardinality(String))[[1]]);
SELECT toTypeName(materialize(['a', 'b']::Array(LowCardinality(String)))[[1]]);

SELECT '-- LowCardinality elements are materialized, like for a scalar index';
SELECT ['a', 'b']::Array(LowCardinality(String))[[2, 1, 5]];
SELECT materialize(['a', 'b']::Array(LowCardinality(String)))[[2, 1, 5]];
SELECT arrayElementOrNull(materialize(['a', 'b']::Array(LowCardinality(String))), [2, 5]);

SELECT '-- Every native integer type is accepted as the index';
SELECT [10, 20, 30][[toUInt8(1), toUInt8(2)]];
SELECT [10, 20, 30][[toUInt16(3), toUInt16(1)]];
SELECT [10, 20, 30][[toUInt32(2), toUInt32(4)]];
SELECT [10, 20, 30][[toUInt64(2), toUInt64(3)]];
SELECT [10, 20, 30][[toInt8(-1), toInt8(2)]];
SELECT [10, 20, 30][[toInt16(-2), toInt16(3)]];
SELECT [10, 20, 30][[toInt32(1), toInt32(-1)]];
SELECT [10, 20, 30][[toInt64(-1), toInt64(-2)]];

SELECT '-- Element types without a numeric fast path';
SELECT [[1, 2], [3, 4], [5, 6]][[2, 1, 9]];
SELECT [(1, 'a'), (2, 'b'), (3, 'c')][[2, 1, 9]];
SELECT [map('a', 1), map('c', 3)][[2, 1, 9]];
SELECT [toFixedString('ab', 2), toFixedString('cd', 2)][[2, 1, 9]];

SELECT '-- Element types with a numeric fast path';
SELECT [1.5, 2.5, 3.5]::Array(Float32)[[2, 3, 1]];
SELECT [1.5, 2.5, 3.5]::Array(Float64)[[3, 1]];
SELECT [toInt128(100), toInt128(200), toInt128(300)][[1, 3]];
SELECT [toUInt128(100), toUInt128(200)][[2, 1]];
SELECT [toInt256(1000), toInt256(2000)][[2]];
SELECT [toUInt256(1000), toUInt256(2000)][[1, 2]];
SELECT [toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'), toUUID('79f0c404-5cb3-11e7-907b-a6006ad3dba0')][[2, 1]];
SELECT [toIPv4('192.168.1.1'), toIPv4('10.0.0.1'), toIPv4('127.0.0.1')][[3, 1]];
SELECT [toIPv6('::1'), toIPv6('fe80::1')][[2, 1]];
SELECT [toDate('2024-01-01'), toDate('2024-06-15'), toDate('2024-12-31')][[3, 1]];
SELECT [toDate32('2024-01-01'), toDate32('2024-06-15')][[2, 1]];
SELECT [toDateTime('2024-01-01 00:00:00', 'UTC'), toDateTime('2024-06-15 12:00:00', 'UTC')][[2]];
SELECT [toDateTime64('2024-01-01 00:00:00.000', 3, 'UTC'), toDateTime64('2024-06-15 12:00:00.123', 3, 'UTC')][[2, 1]];
SELECT [toDecimal32(1.23, 2), toDecimal32(4.56, 2), toDecimal32(7.89, 2)][[2, 3]];
SELECT [toDecimal64(1.23, 2), toDecimal64(4.56, 2)][[1, 2, 1]];
SELECT [toDecimal128(1.23, 2), toDecimal128(4.56, 2)][[2]];

SELECT '-- A non-constant source and a non-constant index';
DROP TABLE IF EXISTS test_arr_idx;
CREATE TABLE test_arr_idx (arr Array(Int32), idx Array(Int32)) ENGINE = Memory;
INSERT INTO test_arr_idx VALUES ([10, 20, 30], [1, 3]), ([40, 50], [2, 1, 2]), ([100], [1, 1]), ([1, 2], []), ([], [1]);
SELECT arr[idx] FROM test_arr_idx ORDER BY ALL;
SELECT arrayElementOrNull(arr, idx) FROM test_arr_idx ORDER BY ALL;
SELECT arr[[1]] FROM test_arr_idx ORDER BY ALL;
DROP TABLE test_arr_idx;

SELECT '-- A constant source and a non-constant index (the lookup table use case)';
DROP TABLE IF EXISTS test_const_src;
CREATE TABLE test_const_src (idx Array(UInt32)) ENGINE = Memory;
INSERT INTO test_const_src VALUES ([1, 3]), ([2, 2, 1]), ([3]), ([]), ([9]);
SELECT [100, 200, 300][idx] FROM test_const_src ORDER BY ALL;
SELECT arrayElementOrNull([100, 200, 300], idx) FROM test_const_src ORDER BY ALL;
DROP TABLE test_const_src;

SELECT '-- A nullable element type with a non-constant source';
DROP TABLE IF EXISTS test_nullable_elements;
CREATE TABLE test_nullable_elements (arr Array(Nullable(UInt8)), idx Array(Int8)) ENGINE = Memory;
INSERT INTO test_nullable_elements VALUES ([10, NULL, 30], [1, 2, 4]), ([NULL], [-1, 1]);
SELECT arr[idx] FROM test_nullable_elements ORDER BY ALL;
SELECT arrayElementOrNull(arr, idx) FROM test_nullable_elements ORDER BY ALL;
DROP TABLE test_nullable_elements;

SELECT '-- A nullable index element type propagates NULL, like a scalar nullable index';
SELECT toTypeName([10, 20, 30][[toNullable(1)]]);
SELECT [10, 20, 30][[toNullable(1)]];
SELECT [10, 20, 30][[1, NULL, 2]];
SELECT materialize([10, 20, 30])[[1, NULL, 2]];
SELECT ['a', 'b', 'c'][[1, NULL, -1]];
SELECT [1, NULL, 3][[1, NULL, 2]];
SELECT [10, 20, 30][[1, 2]::Array(LowCardinality(Int8))] SETTINGS allow_suspicious_low_cardinality_types = 1;
SELECT [10, 20, 30][[1, NULL, 2]::Array(LowCardinality(Nullable(Int8)))] SETTINGS allow_suspicious_low_cardinality_types = 1;

SELECT '-- An out-of-range index still yields the default value, only a NULL index yields NULL';
SELECT [10, 20, 30][[5, NULL, -9]];
SELECT arrayElementOrNull([10, 20, 30], [5, NULL, -9]);

SELECT '-- A nullable index with a non-constant source';
DROP TABLE IF EXISTS test_nullable_index;
CREATE TABLE test_nullable_index (arr Array(UInt8), idx Array(Nullable(Int8))) ENGINE = Memory;
INSERT INTO test_nullable_index VALUES ([10, 20, 30], [1, NULL, 4]), ([], [NULL, 1]);
SELECT arr[idx] FROM test_nullable_index ORDER BY ALL;
SELECT arrayElementOrNull(arr, idx) FROM test_nullable_index ORDER BY ALL;
DROP TABLE test_nullable_index;

SELECT '-- Equivalence with arrayMap; every row must be 1';
SELECT toTypeName(x) = toTypeName(y) AND toString(x) = toString(y) FROM (SELECT [10, 20, 30, 40][[2, 4, 1]] AS x, arrayMap(i -> [10, 20, 30, 40][i], [2, 4, 1]) AS y);
SELECT toTypeName(x) = toTypeName(y) AND toString(x) = toString(y) FROM (SELECT [10, 20, 30][[1, 5, 0, -1, -9]] AS x, arrayMap(i -> [10, 20, 30][i], [1, 5, 0, -1, -9]) AS y);
SELECT toTypeName(x) = toTypeName(y) AND toString(x) = toString(y) FROM (SELECT ['a', 'b'][[1, 3]] AS x, arrayMap(i -> ['a', 'b'][i], [1, 3]) AS y);
SELECT toTypeName(x) = toTypeName(y) AND toString(x) = toString(y) FROM (SELECT [1, NULL, 3][[1, 2, 4]] AS x, arrayMap(i -> [1, NULL, 3][i], [1, 2, 4]) AS y);
SELECT toTypeName(x) = toTypeName(y) AND toString(x) = toString(y) FROM (SELECT [[1, 2], [3]][[2, 5]] AS x, arrayMap(i -> [[1, 2], [3]][i], [2, 5]) AS y);
SELECT toTypeName(x) = toTypeName(y) AND toString(x) = toString(y) FROM (SELECT [('a', 1)][[1, 5]] AS x, arrayMap(i -> [('a', 1)][i], [1, 5]) AS y);
SELECT toTypeName(x) = toTypeName(y) AND toString(x) = toString(y) FROM (SELECT [map('a', 1)][[1, 5]] AS x, arrayMap(i -> [map('a', 1)][i], [1, 5]) AS y);
SELECT toTypeName(x) = toTypeName(y) AND toString(x) = toString(y) FROM (SELECT ['a', 'b']::Array(LowCardinality(String))[[2, 5]] AS x, arrayMap(i -> ['a', 'b']::Array(LowCardinality(String))[i], [2, 5]) AS y);
SELECT toTypeName(x) = toTypeName(y) AND toString(x) = toString(y) FROM (SELECT arrayElementOrNull([10, 20, 30], [1, 5]) AS x, arrayMap(i -> arrayElementOrNull([10, 20, 30], i), [1, 5]) AS y);
SELECT toTypeName(x) = toTypeName(y) AND toString(x) = toString(y) FROM (SELECT arrayElementOrNull([1, NULL, 3], [2, 5]) AS x, arrayMap(i -> arrayElementOrNull([1, NULL, 3], i), [2, 5]) AS y);
SELECT countIf(NOT (toTypeName(x) = toTypeName(y) AND toString(x) = toString(y))) = 0 FROM (SELECT arr[idx] AS x, arrayMap(i -> arr[i], idx) AS y FROM (SELECT range(number % 5) AS arr, arrayMap(j -> toInt32(j) - 3, range(number % 7)) AS idx FROM numbers(100)));
SELECT countIf(NOT (toTypeName(x) = toTypeName(y) AND toString(x) = toString(y))) = 0 FROM (SELECT arrayElementOrNull(arr, idx) AS x, arrayMap(i -> arrayElementOrNull(arr, i), idx) AS y FROM (SELECT range(number % 5) AS arr, arrayMap(j -> toInt32(j) - 3, range(number % 7)) AS idx FROM numbers(100)));
SELECT countIf(NOT (toTypeName(x) = toTypeName(y) AND toString(x) = toString(y))) = 0 FROM (SELECT [100, 200, 300][idx] AS x, arrayMap(i -> [100, 200, 300][i], idx) AS y FROM (SELECT arrayMap(j -> toInt32(j) - 3, range(number % 8)) AS idx FROM numbers(100)));
SELECT countIf(NOT (toTypeName(x) = toTypeName(y) AND toString(x) = toString(y))) = 0 FROM (SELECT arr[idx] AS x, arrayMap(i -> arr[i], idx) AS y FROM (SELECT arrayMap(j -> if(j % 2 = 0, NULL, toUInt8(j)), range(number % 5)) AS arr, arrayMap(j -> toInt32(j) - 2, range(number % 6)) AS idx FROM numbers(100)));
SELECT countIf(NOT (toTypeName(x) = toTypeName(y) AND toString(x) = toString(y))) = 0 FROM (SELECT arr[idx] AS x, arrayMap(i -> arr[i], idx) AS y FROM (SELECT arrayMap(j -> toString(j), range(number % 5)) AS arr, arrayMap(j -> toInt32(j) - 2, range(number % 6)) AS idx FROM numbers(100)));
SELECT countIf(NOT (toTypeName(x) = toTypeName(y) AND toString(x) = toString(y))) = 0 FROM (SELECT arr[idx] AS x, arrayMap(i -> arr[i], idx) AS y FROM (SELECT arrayMap(j -> toLowCardinality(toString(j)), range(number % 5)) AS arr, arrayMap(j -> toInt32(j) - 2, range(number % 6)) AS idx FROM numbers(100)));
SELECT toTypeName(x) = toTypeName(y) AND toString(x) = toString(y) FROM (SELECT [10, 20, 30][[1, NULL, 2]] AS x, arrayMap(i -> [10, 20, 30][i], [1, NULL, 2]) AS y);
SELECT toTypeName(x) = toTypeName(y) AND toString(x) = toString(y) FROM (SELECT [10, 20, 30][[5, NULL, -9]] AS x, arrayMap(i -> [10, 20, 30][i], [5, NULL, -9]) AS y);
SELECT toTypeName(x) = toTypeName(y) AND toString(x) = toString(y) FROM (SELECT ['a', 'b'][[1, NULL, 3]] AS x, arrayMap(i -> ['a', 'b'][i], [1, NULL, 3]) AS y);
SELECT toTypeName(x) = toTypeName(y) AND toString(x) = toString(y) FROM (SELECT [1, NULL, 3][[1, NULL, 4]] AS x, arrayMap(i -> [1, NULL, 3][i], [1, NULL, 4]) AS y);
SELECT toTypeName(x) = toTypeName(y) AND toString(x) = toString(y) FROM (SELECT [[1, 2], [3]][[2, NULL]] AS x, arrayMap(i -> [[1, 2], [3]][i], [2, NULL]) AS y);
SELECT toTypeName(x) = toTypeName(y) AND toString(x) = toString(y) FROM (SELECT [('a', 1)][[1, NULL]] AS x, arrayMap(i -> [('a', 1)][i], [1, NULL]) AS y);
SELECT toTypeName(x) = toTypeName(y) AND toString(x) = toString(y) FROM (SELECT arrayElementOrNull([10, 20, 30], [1, NULL, 5]) AS x, arrayMap(i -> arrayElementOrNull([10, 20, 30], i), [1, NULL, 5]) AS y);
SELECT countIf(NOT (toTypeName(x) = toTypeName(y) AND toString(x) = toString(y))) = 0 FROM (SELECT arr[idx] AS x, arrayMap(i -> arr[i], idx) AS y FROM (SELECT range(number % 5) AS arr, arrayMap(j -> if(j % 3 = 0, NULL, toInt32(j) - 3), range(number % 7)) AS idx FROM numbers(100)));
SELECT countIf(NOT (toTypeName(x) = toTypeName(y) AND toString(x) = toString(y))) = 0 FROM (SELECT arrayElementOrNull(arr, idx) AS x, arrayMap(i -> arrayElementOrNull(arr, i), idx) AS y FROM (SELECT arrayMap(j -> toString(j), range(number % 5)) AS arr, arrayMap(j -> if(j % 3 = 0, NULL, toInt32(j) - 3), range(number % 7)) AS idx FROM numbers(100)));

SELECT '-- The index must be an array of integers';
SELECT [10, 20, 30][[1.5]]; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT [10, 20, 30][['a']]; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT [10, 20, 30][[[1]]]; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT [10, 20, 30][[toNullable(1.5)]]; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayElementOrNull([10, 20, 30], [1.5]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT map('a', 1)[[1]]; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
