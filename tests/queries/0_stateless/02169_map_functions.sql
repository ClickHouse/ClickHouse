DROP TABLE IF EXISTS table_map;
CREATE TABLE table_map (id UInt32, col Map(String, UInt64)) engine = MergeTree() ORDER BY tuple();
INSERT INTO table_map SELECT number, map('key1', number, 'key2', number * 2) FROM numbers(1111, 3);
INSERT INTO table_map SELECT number, map('key3', number, 'key2', number + 1, 'key4', number + 2) FROM numbers(100, 4);

SELECT mapFilter((k, v) -> k like '%3' and v > 102, col) FROM table_map ORDER BY id;
SELECT col, mapFilter((k, v) -> ((v % 10) > 1), col) FROM table_map ORDER BY id ASC;
SELECT mapApply((k, v) -> (k, v + 1), col) FROM table_map ORDER BY id;
SELECT mapFilter((k, v) -> 0, col) from table_map;
SELECT mapApply((k, v) -> tuple(v + 9223372036854775806), col) FROM table_map; -- { serverError BAD_ARGUMENTS }

SELECT mapFilter((k, v) -> k = 0.1::Float32, map(0.1::Float32, 4, 0.2::Float32, 5));
SELECT mapFilter((k, v) -> k = 0.1::Float64, map(0.1::Float64, 4, 0.2::Float64, 5));
SELECT mapFilter((k, v) -> k = array(1,2), map(array(1,2), 4, array(3,4), 5));
SELECT mapFilter((k, v) -> k = map(1,2), map(map(1,2), 4, map(3,4), 5));
SELECT mapFilter((k, v) -> k = tuple(1,2), map(tuple(1,2), 4, tuple(3,4), 5));

SELECT mapExists((k, v) -> k = 0.1::Float32, map(0.1::Float32, 4, 0.2::Float32, 5));
SELECT mapExists((k, v) -> k = 0.1::Float64, map(0.1::Float64, 4, 0.2::Float64, 5));
SELECT mapExists((k, v) -> k = array(1,2), map(array(1,2), 4, array(3,4), 5));
SELECT mapExists((k, v) -> k = map(1,2), map(map(1,2), 4, map(3,4), 5));
SELECT mapExists((k, v) -> k = tuple(1,2), map(tuple(1,2), 4, tuple(3,4), 5));

SELECT mapAll((k, v) -> k = 0.1::Float32, map(0.1::Float32, 4, 0.2::Float32, 5));
SELECT mapAll((k, v) -> k = 0.1::Float64, map(0.1::Float64, 4, 0.2::Float64, 5));
SELECT mapAll((k, v) -> k = array(1,2), map(array(1,2), 4, array(3,4), 5));
SELECT mapAll((k, v) -> k = map(1,2), map(map(1,2), 4, map(3,4), 5));
SELECT mapAll((k, v) -> k = tuple(1,2), map(tuple(1,2), 4, tuple(3,4), 5));

SELECT mapSort((k, v) -> k, map(0.1::Float32, 4, 0.2::Float32, 5));
SELECT mapSort((k, v) -> k, map(0.1::Float64, 4, 0.2::Float64, 5));
SELECT mapSort((k, v) -> k, map(array(1,2), 4, array(3,4), 5));
SELECT mapSort((k, v) -> k, map(map(1,2), 4, map(3,4), 5));
SELECT mapSort((k, v) -> k, map(tuple(1,2), 4, tuple(3,4), 5));

SELECT mapConcat(col, map('key5', 500), map('key6', 600)) FROM table_map ORDER BY id;
SELECT mapConcat(col, materialize(map('key5', 500)), map('key6', 600)) FROM table_map ORDER BY id;
SELECT concat(map('key5', 500), map('key6', 600));
SELECT map('key5', 500) || map('key6', 600);

SELECT mapConcat(map(0.1::Float32, 4), map(0.2::Float32, 5));
SELECT mapConcat(map(0.1::Float64, 4), map(0.2::Float64, 5));
SELECT mapConcat(map(array(1,2), 4), map(array(3,4), 5));
SELECT mapConcat(map(map(1,2), 4), map(map(3,4), 5));
SELECT mapConcat(map(tuple(1,2), 4), map(tuple(3,4), 5));

SELECT mapExists((k, v) -> k LIKE '%3', col) FROM table_map ORDER BY id;
SELECT mapExists((k, v) -> k LIKE '%2' AND v < 1000, col) FROM table_map ORDER BY id;

SELECT mapAll((k, v) -> k LIKE '%3', col) FROM table_map ORDER BY id;
SELECT mapAll((k, v) -> k LIKE '%2' AND v < 1000, col) FROM table_map ORDER BY id;

SELECT mapSort(col) FROM table_map ORDER BY id;
SELECT mapSort((k, v) -> v, col) FROM table_map ORDER BY id;
SELECT mapPartialSort((k, v) -> k, 2, col) FROM table_map ORDER BY id;

SELECT mapUpdate(map(1, 3, 3, 2), map(1, 0, 2, 0));
SELECT mapApply((x, y) -> (x, x + 1), map(1, 0, 2, 0));
SELECT mapApply((x, y) -> (x, x + 1), materialize(map(1, 0, 2, 0)));
SELECT mapApply((x, y) -> ('x', 'y'), map(1, 0, 2, 0));
SELECT mapApply((x, y) -> ('x', 'y'), materialize(map(1, 0, 2, 0)));
SELECT mapApply((x, y) -> (x, x + 1), map(1.0, 0, 2.0, 0));
SELECT mapApply((x, y) -> (x, x + 1), materialize(map(1.0, 0, 2.0, 0)));

SELECT mapUpdate(map('k1', 1, 'k2', 2), map('k1', 11, 'k2', 22));
SELECT mapUpdate(materialize(map('k1', 1, 'k2', 2)), map('k1', 11, 'k2', 22));
SELECT mapUpdate(map('k1', 1, 'k2', 2), materialize(map('k1', 11, 'k2', 22)));
SELECT mapUpdate(materialize(map('k1', 1, 'k2', 2)), materialize(map('k1', 11, 'k2', 22)));

SELECT mapUpdate(map('k1', 1, 'k2', 2, 'k3', 3), map('k2', 22, 'k3', 33, 'k4', 44));
SELECT mapUpdate(materialize(map('k1', 1, 'k2', 2, 'k3', 3)), map('k2', 22, 'k3', 33, 'k4', 44));
SELECT mapUpdate(map('k1', 1, 'k2', 2, 'k3', 3), materialize(map('k2', 22, 'k3', 33, 'k4', 44)));
SELECT mapUpdate(materialize(map('k1', 1, 'k2', 2, 'k3', 3)), materialize(map('k2', 22, 'k3', 33, 'k4', 44)));

SELECT mapUpdate(map('k1', 1, 'k2', 2), map('k3', 33, 'k4', 44));
SELECT mapUpdate(materialize(map('k1', 1, 'k2', 2)), map('k3', 33, 'k4', 44));
SELECT mapUpdate(map('k1', 1, 'k2', 2), materialize(map('k3', 33, 'k4', 44)));
SELECT mapUpdate(materialize(map('k1', 1, 'k2', 2)), materialize(map('k3', 33, 'k4', 44)));

WITH (range(0, number % 10), range(0, number % 10))::Map(UInt64, UInt64) AS m1,
     (range(0, number % 10, 2), arrayMap(x -> x * x, range(0, number % 10, 2)))::Map(UInt64, UInt64) AS m2
SELECT DISTINCT mapUpdate(m1, m2) FROM numbers (100000);

SELECT mapApply(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT mapApply((x, y) -> (x), map(1, 0, 2, 0)); -- { serverError BAD_ARGUMENTS }
SELECT mapApply((x, y) -> ('x'), map(1, 0, 2, 0)); -- { serverError BAD_ARGUMENTS }
SELECT mapApply((x) -> (x, x), map(1, 0, 2, 0));
SELECT mapApply((x, y) -> (x, 1, 2), map(1, 0, 2, 0)); -- { serverError BAD_ARGUMENTS }
SELECT mapApply((x, y) -> (x, x + 1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT mapApply(map(1, 0, 2, 0), (x, y) -> (x, x + 1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT mapApply((x, y) -> (x, x+1), map(1, 0, 2, 0), map(1, 0, 2, 0)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT mapFilter(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT mapFilter((x, y) -> (toInt32(x)), map(1, 0, 2, 0)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT mapFilter((x, y) -> ('x'), map(1, 0, 2, 0)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT mapFilter((x) -> (x, x), map(1, 0, 2, 0)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT mapFilter((x, y) -> (x, 1, 2), map(1, 0, 2, 0)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT mapFilter((x, y) -> (x, x + 1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT mapFilter(map(1, 0, 2, 0), (x, y) -> (x > 0)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT mapFilter((x, y) -> (x, x + 1), map(1, 0, 2, 0), map(1, 0, 2, 0)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT mapConcat([1, 2], map(1, 2)); -- { serverError NO_COMMON_TYPE }
SELECT mapSort(map(1, 2), map(3, 4)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT mapUpdate(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT mapUpdate(map(1, 3, 3, 2), map(1, 0, 2, 0),  map(1, 0, 2, 0)); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

DROP TABLE table_map;
