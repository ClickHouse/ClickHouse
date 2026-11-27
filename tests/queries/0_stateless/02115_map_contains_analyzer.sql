DROP TABLE IF EXISTS t_map_contains;

CREATE TABLE t_map_contains (m Map(String, UInt32)) ENGINE = Memory;

INSERT INTO t_map_contains VALUES (map('a', 1, 'b', 2)), (map('c', 3, 'd', 4));

SET optimize_functions_to_subcolumns = 1;
SET enable_analyzer = 1;

EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT mapContains(m, 'a') FROM t_map_contains;
SELECT mapContains(m, 'a') FROM t_map_contains;

DROP TABLE t_map_contains;
