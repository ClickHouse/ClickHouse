DROP TABLE IF EXISTS t_get_subcolumn;

SET enable_analyzer = 1;
SET enable_json_type = 1;
SET optimize_functions_to_subcolumns = 1;

CREATE TABLE t_get_subcolumn
(
    t Tuple(a UInt64, b Tuple(c UInt64, d String)),
    a Array(UInt64),
    j JSON
) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_get_subcolumn VALUES ((1, (1, 'aaa')), [1], '{"a": "b", "c": {"d": 10, "e": [1, 2, 3]}}');

SELECT getSubcolumn(t, 'a') FROM t_get_subcolumn;
EXPLAIN QUERY TREE dump_ast = 1, dump_tree = 0 SELECT getSubcolumn(t, 'a') FROM t_get_subcolumn;

SELECT tupleElement(t, 'a') FROM t_get_subcolumn;
EXPLAIN QUERY TREE dump_ast = 1, dump_tree = 0 SELECT tupleElement(t, 'a') FROM t_get_subcolumn;

SELECT tupleElement(t, 1) FROM t_get_subcolumn;
EXPLAIN QUERY TREE dump_ast = 1, dump_tree = 0 SELECT tupleElement(t, 1) FROM t_get_subcolumn;

SELECT getSubcolumn(t, 1) FROM t_get_subcolumn; -- {serverError ILLEGAL_COLUMN}

SELECT getSubcolumn(t, 'b.d') FROM t_get_subcolumn;
EXPLAIN QUERY TREE dump_ast = 1, dump_tree = 0 SELECT getSubcolumn(t, 'b.d') FROM t_get_subcolumn;

SELECT getSubcolumn(a, 'size0') FROM t_get_subcolumn;
EXPLAIN QUERY TREE dump_ast = 1, dump_tree = 0 SELECT getSubcolumn(a, 'size0') FROM t_get_subcolumn;

SELECT getSubcolumn(j, 'a') FROM t_get_subcolumn;
EXPLAIN QUERY TREE dump_ast = 1, dump_tree = 0 SELECT getSubcolumn(j, 'a') FROM t_get_subcolumn;

SELECT getSubcolumn(j, 'c.d') FROM t_get_subcolumn;
EXPLAIN QUERY TREE dump_ast = 1, dump_tree = 0 SELECT getSubcolumn(j, 'c.d') FROM t_get_subcolumn;

SELECT getSubcolumn(j, 'c.e') FROM t_get_subcolumn;
EXPLAIN QUERY TREE dump_ast = 1, dump_tree = 0 SELECT getSubcolumn(j, 'c.e') FROM t_get_subcolumn;

DROP TABLE t_get_subcolumn;
