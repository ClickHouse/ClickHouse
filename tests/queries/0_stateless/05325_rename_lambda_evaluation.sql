SELECT * RENAME (col -> if(col = 'a', 'first', concat(col, '_other'))) FROM (SELECT 1 AS a, 2 AS b, 3 AS c) FORMAT TSVWithNames;
WITH '_suffix' AS suffix SELECT * RENAME (col -> concat(col, suffix)) FROM (SELECT 1 AS a, 2 AS b) FORMAT TSVWithNames;
SELECT * RENAME (col -> col) FROM (SELECT 1 AS a, 2 AS b) FORMAT TSVWithNames;
SELECT * RENAME (col -> 'same') FROM (SELECT 1 AS a, 1 AS b) FORMAT TSVWithNames;
SELECT * EXCEPT b RENAME (col -> toString(toUInt64(col) + 1)) FROM (SELECT 7 AS `10`, 8 AS b) FORMAT TSVWithNames;
SELECT COLUMNS('a') RENAME (col -> concat(col, '_one')), COLUMNS('b') RENAME (col -> concat(col, '_two')) FROM (SELECT 1 AS a, 2 AS b) FORMAT TSVWithNames;
SELECT * RENAME (col -> arrayStringConcat(arrayMap(suffix -> concat(col, suffix), ['_x', '_y']), ',')) FROM (SELECT 1 AS a, 2 AS b) FORMAT TSVWithNames;
SELECT length(tupleNames(tuple(* RENAME (col -> repeat(concat(col, col), 524288))))[1]) FROM (SELECT 1 AS a) SETTINGS enable_named_columns_in_function_tuple = 1;
SELECT * RENAME (col -> 1) FROM (SELECT 1 AS a); -- { serverError BAD_ARGUMENTS }
SELECT * RENAME (col -> concat(col, toString(rand()))) FROM (SELECT 1 AS a); -- { serverError BAD_ARGUMENTS }
SELECT * RENAME (col -> arrayStringConcat(arrayMap(x -> toString(rand()), [1]), '')) FROM (SELECT 1 AS a); -- { serverError BAD_ARGUMENTS }
SELECT * RENAME (col -> concat(col, toString(number))) FROM numbers(2); -- { serverError BAD_ARGUMENTS }
