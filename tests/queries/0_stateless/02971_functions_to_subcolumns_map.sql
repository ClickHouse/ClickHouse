DROP TABLE IF EXISTS t_func_to_subcolumns_map;

CREATE TABLE t_func_to_subcolumns_map (id UInt64, m Map(String, UInt64)) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_func_to_subcolumns_map VALUES (1, map('aaa', 1, 'bbb', 2)) (2, map('ccc', 3));

SET optimize_functions_to_subcolumns = 1;
SET allow_experimental_analyzer = 0;

EXPLAIN SYNTAX SELECT length(m) FROM t_func_to_subcolumns_map;
SELECT length(m) FROM t_func_to_subcolumns_map;

SET optimize_functions_to_subcolumns = 1;
SET allow_experimental_analyzer = 1;

EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT length(m) FROM t_func_to_subcolumns_map;
SELECT length(m) FROM t_func_to_subcolumns_map;

DROP TABLE t_func_to_subcolumns_map;
