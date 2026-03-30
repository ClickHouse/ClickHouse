-- Regression test: cloneSubdagWithInputs must re-resolve function nodes
-- when mergeInplace replaces inputs with nodes of a different type.
-- Without the fix, this would throw "Unexpected return type from ..."

SET enable_join_runtime_filters = 1;
SET allow_experimental_join_condition = 1;
SET join_algorithm = 'hash';

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (a UInt16, b String) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t2 (a Nullable(UInt64), b String) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t1 VALUES (1, 'x'), (2, 'y'), (3, 'z');
INSERT INTO t2 VALUES (1, 'x'), (2, 'y'), (NULL, 'w');

-- The join key expression `a + 0` forces a function node in the key DAG.
-- t1.a is UInt16 but t2.a is Nullable(UInt64), so after cloneSubdagWithInputs
-- merges the key subDAG with the stream header, the function's argument types
-- change and must be re-resolved.
SELECT t1.a, t2.b
FROM t1
INNER JOIN t2 ON t1.a + 0 = t2.a + 0
ORDER BY t1.a;

DROP TABLE t1;
DROP TABLE t2;
