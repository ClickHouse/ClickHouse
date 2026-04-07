DROP TABLE IF EXISTS t_column_names;

CREATE TABLE t_column_names (arr Array(UInt64), n Nullable(String)) ENGINE = Memory;

INSERT INTO t_column_names VALUES ([1, 2, 3], 'foo');

SET optimize_functions_to_subcolumns = 1;
SET enable_analyzer = 1;

EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT length(arr), isNull(n) FROM t_column_names;
SELECT length(arr), isNull(n) FROM t_column_names FORMAT JSONEachRow;

DROP TABLE t_column_names;
