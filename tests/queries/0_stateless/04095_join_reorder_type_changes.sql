-- Regression test: join reorder with type-changing joins (e.g. LEFT JOIN + join_use_nulls)
-- could cause "Cannot fold actions for projection" when the optimizer separates a relation
-- from the join that causes its type change.
--
-- The minimal 4-table reproducer uses two chained LEFT JOINs followed by an INNER JOIN whose
-- ON condition references a column from the nested LEFT JOIN together with an IS NOT NULL
-- predicate. With `join_use_nulls = 1`, the optimizer flattens the child LEFT JOIN into the
-- join graph and reorders relations so that a type-changing step is applied at the wrong
-- point, triggering the exception on unfixed servers.

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;
DROP TABLE IF EXISTS t4;

CREATE TABLE t1 (id Int32, a Int32, b Nullable(Int32)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t2 (id Int32, c Nullable(Int32)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t3 (id Int32, a Int32, c Nullable(Int32)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t4 (id Int32, a Int32) ENGINE = MergeTree ORDER BY id;

INSERT INTO t1 VALUES (1, 1, 1);
INSERT INTO t2 VALUES (1, 1);
INSERT INTO t3 VALUES (1, 1, 1);
INSERT INTO t4 VALUES (1, 1);

SELECT t2.id
FROM t2
    LEFT JOIN t3 ON t2.c = t3.c
    LEFT JOIN t1 ON t3.a = t1.a
    INNER JOIN t4 ON t1.id IS NOT NULL AND t1.a = t4.a
ORDER BY ALL
SETTINGS join_use_nulls = 1, query_plan_optimize_join_order_limit = 10;

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
DROP TABLE t4;
