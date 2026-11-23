DROP TABLE IF EXISTS t_length_1;
DROP TABLE IF EXISTS t_length_2;

SET optimize_functions_to_subcolumns = 1;
SET enable_analyzer = 1;
SET optimize_on_insert = 0;

CREATE TABLE t_length_1 (id UInt64, arr Array(UInt64)) ENGINE = ReplacingMergeTree ORDER BY id;
CREATE TABLE t_length_2 (id UInt64, arr_length UInt64) ENGINE = ReplacingMergeTree ORDER BY id;

INSERT INTO t_length_1 VALUES (1, [1, 2, 3]), (2, [4, 5]);
INSERT INTO t_length_2 VALUES (1, 3), (1, 2), (2, 2);

SELECT length(arr) FROM t_length_1 WHERE length(arr) in (SELECT arr_length FROM t_length_2);
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT length(arr) FROM t_length_1 WHERE length(arr) in (SELECT arr_length FROM t_length_2);

SELECT length(arr) FROM t_length_1 WHERE length(arr) in (SELECT arr_length FROM t_length_2 FINAL);
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT length(arr) FROM t_length_1 WHERE length(arr) in (SELECT arr_length FROM t_length_2 FINAL);

SELECT length(arr) FROM t_length_1 FINAL WHERE length(arr) in (SELECT arr_length FROM t_length_2 FINAL);
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT length(arr) FROM t_length_1 FINAL WHERE length(arr) in (SELECT arr_length FROM t_length_2 FINAL);

DROP TABLE t_length_1;
DROP TABLE t_length_2;
