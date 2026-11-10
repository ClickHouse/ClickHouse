-- `tupleElement` over a `Nullable(Tuple(...))` column is rewritten to a read of the element subcolumn.
-- The subcolumn carries the enclosing null map exactly as `tupleElement` folds it into its result,
-- so a row whose whole tuple is NULL gives NULL either way, for a plain, an already-`Nullable` and a
-- `LowCardinality` element alike.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_nullable_tuple_element_rewrite;

CREATE TABLE t_nullable_tuple_element_rewrite
(
    key UInt64,
    t Nullable(Tuple(a UInt64, b Nullable(String), c LowCardinality(String)))
)
ENGINE = MergeTree ORDER BY key;

INSERT INTO t_nullable_tuple_element_rewrite VALUES (1, (10, 'x', 'lc1')), (2, NULL), (3, (30, NULL, 'lc3')), (4, (40, 'y', 'lc4'));

SELECT 'same answer with and without the rewrite';
SELECT key, tupleElement(t, 'a'), tupleElement(t, 'b'), tupleElement(t, 3) FROM t_nullable_tuple_element_rewrite ORDER BY key SETTINGS optimize_functions_to_subcolumns = 0;
SELECT key, tupleElement(t, 'a'), tupleElement(t, 'b'), tupleElement(t, 3) FROM t_nullable_tuple_element_rewrite ORDER BY key SETTINGS optimize_functions_to_subcolumns = 1;

SELECT 'element types';
SELECT toTypeName(tupleElement(t, 'a')), toTypeName(tupleElement(t, 'b')), toTypeName(tupleElement(t, 3)) FROM t_nullable_tuple_element_rewrite LIMIT 1 SETTINGS optimize_functions_to_subcolumns = 0;
SELECT toTypeName(tupleElement(t, 'a')), toTypeName(tupleElement(t, 'b')), toTypeName(tupleElement(t, 3)) FROM t_nullable_tuple_element_rewrite LIMIT 1 SETTINGS optimize_functions_to_subcolumns = 1;

SELECT 'filter on an element';
SELECT key FROM t_nullable_tuple_element_rewrite WHERE tupleElement(t, 'a') > 20 ORDER BY key SETTINGS optimize_functions_to_subcolumns = 0;
SELECT key FROM t_nullable_tuple_element_rewrite WHERE tupleElement(t, 'a') > 20 ORDER BY key SETTINGS optimize_functions_to_subcolumns = 1;
SELECT count() FROM t_nullable_tuple_element_rewrite WHERE tupleElement(t, 'a') IS NULL SETTINGS optimize_functions_to_subcolumns = 0;
SELECT count() FROM t_nullable_tuple_element_rewrite WHERE tupleElement(t, 'a') IS NULL SETTINGS optimize_functions_to_subcolumns = 1;

SELECT 'aggregated';
SELECT sum(tupleElement(t, 'a')), count(tupleElement(t, 'b')), uniqExact(tupleElement(t, 3)) FROM t_nullable_tuple_element_rewrite SETTINGS optimize_functions_to_subcolumns = 0;
SELECT sum(tupleElement(t, 'a')), count(tupleElement(t, 'b')), uniqExact(tupleElement(t, 3)) FROM t_nullable_tuple_element_rewrite SETTINGS optimize_functions_to_subcolumns = 1;

SELECT 'rewrite fired';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT tupleElement(t, 'a') FROM t_nullable_tuple_element_rewrite SETTINGS optimize_functions_to_subcolumns = 1) WHERE explain ILIKE '%`t.a`%';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT tupleElement(t, 'b') FROM t_nullable_tuple_element_rewrite SETTINGS optimize_functions_to_subcolumns = 1) WHERE explain ILIKE '%`t.b`%';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT tupleElement(t, 3) FROM t_nullable_tuple_element_rewrite SETTINGS optimize_functions_to_subcolumns = 1) WHERE explain ILIKE '%`t.c`%';

-- A column of the outer-joined side gets a default in the rows without a match, so functions over
-- `Nullable` columns of such a table are never rewritten; the answer is still the same.
SELECT 'outer-joined side';
SELECT l.key, tupleElement(r.t, 'a') FROM (SELECT number + 1 AS key FROM numbers(5)) AS l LEFT JOIN t_nullable_tuple_element_rewrite AS r ON l.key = r.key ORDER BY l.key SETTINGS optimize_functions_to_subcolumns = 0;
SELECT l.key, tupleElement(r.t, 'a') FROM (SELECT number + 1 AS key FROM numbers(5)) AS l LEFT JOIN t_nullable_tuple_element_rewrite AS r ON l.key = r.key ORDER BY l.key SETTINGS optimize_functions_to_subcolumns = 1;
SELECT count() > 0 FROM (EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT tupleElement(r.t, 'a') FROM (SELECT number + 1 AS key FROM numbers(5)) AS l LEFT JOIN t_nullable_tuple_element_rewrite AS r ON l.key = r.key SETTINGS optimize_functions_to_subcolumns = 1) WHERE explain ILIKE '%`t.a`%';

DROP TABLE t_nullable_tuple_element_rewrite;
