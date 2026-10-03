SET enable_analyzer = 1;
SET enable_named_columns_in_function_tuple = 1;

-- Basic rewrite
SELECT tuple(1 AS a, 2 AS b, 3 AS c) FORMAT JSONEachRow;

-- Rewrite with strings
SELECT tuple('hello' AS greeting, 'world' AS place) FORMAT JSONEachRow;

-- Mixed types with aliases
SELECT tuple(1 AS id, 'test' AS name, 3.14 AS value) FORMAT JSONEachRow;

-- Nested tuples with aliases
SELECT tuple(tuple(1 AS inner1, 2 AS inner2) AS nested, 3 AS outer) FORMAT JSONEachRow;

-- Disable setting - should NOT rewrite
SELECT tuple(1 AS x, 2 AS y) SETTINGS enable_named_columns_in_function_tuple = 0 FORMAT JSONEachRow;

-- Duplicate names - should NOT rewrite FORMAT JSONEachRow;
SELECT tuple(number, number) FROM numbers(1) FORMAT JSONEachRow;

-- Quoted name - should NOT rewrite
SELECT tuple(1 AS `@123invalid`, 2 AS y) FORMAT JSONEachRow;

-- Constant without alias - should NOT rewrite
SELECT tuple('x', 'y', 'z') FORMAT JSONEachRow;

-- Explicit names - should NOT rewrite
SELECT tuple('x', 'y', 'z')(1 AS a, 2 AS b, 3 AS c) FORMAT JSONEachRow;

-- Verify the result has named columns
SELECT tuple(1 AS a, 2 AS b, 3 AS c) AS t, toTypeName(t), t.a, t.b, t.c FORMAT JSONEachRow;
