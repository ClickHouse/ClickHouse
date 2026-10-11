SELECT * RENAME a AS x, * FROM (SELECT 1 AS a) FORMAT TSVWithNames;
SELECT COLUMNS('a') RENAME a AS x, COLUMNS('a') FROM (SELECT 1 AS a) FORMAT TSVWithNames;
SELECT * RENAME (col -> concat(col, '_renamed')), * FROM (SELECT 1 AS a) FORMAT TSVWithNames;
SELECT a, * RENAME a AS x, a FROM (SELECT 1 AS a) FORMAT TSVWithNames;
SELECT * APPLY (x -> x) RENAME a AS x, * FROM (SELECT 1 AS a) FORMAT TSVWithNames;
SELECT * REPLACE (a + 1 AS a) RENAME a AS x, * FROM (SELECT 1 AS a) FORMAT TSVWithNames;
SELECT x FROM (SELECT * RENAME a AS x FROM (SELECT 1 AS a));
SELECT tupleNames(tuple(* RENAME a AS x)) FROM (SELECT 1 AS a) SETTINGS enable_named_columns_in_function_tuple = 1;
