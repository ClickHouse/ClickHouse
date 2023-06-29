DROP TABLE IF EXISTS table_map;
CREATE TABLE table_map (id UInt32, col Map(String, UInt64)) engine = MergeTree() ORDER BY tuple();
INSERT INTO table_map SELECT number, map('key1', number, 'key2', number * 2) FROM numbers(1111, 3);
INSERT INTO table_map SELECT number, map('key3', number, 'key2', number + 1, 'key4', number + 2) FROM numbers(100, 4);

SELECT mapFilter((k, v) -> k like '%3' and v > 102, col) FROM table_map ORDER BY id;
SELECT col, mapFilter((k, v) -> ((v % 10) > 1), col) FROM table_map ORDER BY id ASC;
SELECT mapApply((k, v) -> (k, v + 1), col) FROM table_map ORDER BY id;
SELECT mapFilter((k, v) -> 0, col) from table_map;
SELECT mapApply((k, v) -> tuple(v + 9223372036854775806), col) FROM table_map; -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT mapUpdate(map(1, 3, 3, 2), map(1, 0, 2, 0));
SELECT mapApply((x, y) -> (x, x + 1), map(1, 0, 2, 0));
SELECT mapApply((x, y) -> (x, x + 1), materialize(map(1, 0, 2, 0)));
SELECT mapApply((x, y) -> ('x', 'y'), map(1, 0, 2, 0));
SELECT mapApply((x, y) -> ('x', 'y'), materialize(map(1, 0, 2, 0)));

SELECT mapApply(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT mapApply((x, y) -> (x), map(1, 0, 2, 0)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT mapApply((x, y) -> ('x'), map(1, 0, 2, 0)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT mapApply((x) -> (x, x), map(1, 0, 2, 0)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT mapApply((x, y) -> (x, 1, 2), map(1, 0, 2, 0)); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT mapApply((x, y) -> (x, x + 1)); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT mapApply(map(1, 0, 2, 0), (x, y) -> (x, x + 1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT mapApply((x, y) -> (x, x+1), map(1, 0, 2, 0), map(1, 0, 2, 0)); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT mapFilter(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT mapFilter((x, y) -> (toInt32(x)), map(1, 0, 2, 0)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT mapFilter((x, y) -> ('x'), map(1, 0, 2, 0)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT mapFilter((x) -> (x, x), map(1, 0, 2, 0)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT mapFilter((x, y) -> (x, 1, 2), map(1, 0, 2, 0)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT mapFilter((x, y) -> (x, x + 1)); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT mapFilter(map(1, 0, 2, 0), (x, y) -> (x > 0)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT mapFilter((x, y) -> (x, x + 1), map(1, 0, 2, 0), map(1, 0, 2, 0)); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT mapUpdate(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT mapUpdate(map(1, 3, 3, 2), map(1, 0, 2, 0),  map(1, 0, 2, 0)); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

DROP TABLE table_map;
