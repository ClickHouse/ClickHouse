-- A single-argument `or` used to trip `chassert(or_argument_nodes.size() > 1)` in
-- `tryExtractCommonExpressions` (an exception with a logical error in debug builds). Such a node can
-- reach the pass because an argument of type `Nothing` (here: the alias of an empty array from
-- `ARRAY JOIN`) short-circuits function resolution before the "at least 2 arguments" check runs.
-- Found by AST fuzzer.

DROP TABLE IF EXISTS t_04759;
CREATE TABLE t_04759 (c0 Int32) ENGINE = MergeTree ORDER BY c0;
INSERT INTO t_04759 VALUES (1), (2), (3);

SELECT 9223372036854775807 AS x
FROM t_04759 AS tx
LEFT ARRAY JOIN [] AS a0
GLOBAL ANTI RIGHT JOIN t_04759 ON or(t_04759.c0 = a0); -- { serverError NOT_IMPLEMENTED }

DROP TABLE t_04759;
