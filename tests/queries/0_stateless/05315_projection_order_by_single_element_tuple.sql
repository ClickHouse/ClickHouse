-- A `tuple` of one argument in the ORDER BY of a projection has to survive formatting:
-- unwrapped to `ORDER BY c0`, it parses back as the bare `c0`, a different AST.

SELECT formatQuery('CREATE TABLE t (c0 Int32, PROJECTION p (SELECT c0 ORDER BY tuple(c0))) ENGINE = MergeTree ORDER BY tuple()');
SELECT formatQuery('CREATE TABLE t (c0 Int32, c1 Int32, PROJECTION p (SELECT c0 ORDER BY (c0, c1))) ENGINE = MergeTree ORDER BY tuple()');
SELECT formatQuery('CREATE TABLE t (c0 Int32, c1 Int32, PROJECTION p (SELECT c0 ORDER BY tuple(c0, c1))) ENGINE = MergeTree ORDER BY tuple()');
SELECT formatQuery('CREATE TABLE t (c0 Int32, PROJECTION p (SELECT c0 ORDER BY c0)) ENGINE = MergeTree ORDER BY tuple()');

DROP TABLE IF EXISTS t_projection_single_element_tuple;
CREATE TABLE t_projection_single_element_tuple (c0 Int32, PROJECTION p (SELECT c0 ORDER BY tuple(c0))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_projection_single_element_tuple VALUES (2), (1), (3);
SELECT c0 FROM t_projection_single_element_tuple ORDER BY c0;
DROP TABLE t_projection_single_element_tuple;
