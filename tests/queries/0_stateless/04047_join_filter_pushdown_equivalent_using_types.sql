-- Regression test for legacy `JoinStep` filter pushdown through equivalent `USING` columns.
-- Equivalent-column replacement must preserve `Cast JOIN USING columns` actions instead of
-- replacing the derived node by name and dropping the type-conversion chain.

SET enable_analyzer = 1;
SET query_plan_use_new_logical_join_step = 0;
SET enable_join_runtime_filters = 0;
SET join_use_nulls = 1;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;

CREATE TABLE t1 (id UInt8, value String) ENGINE = Memory;
CREATE TABLE t2 (id UInt16, value String) ENGINE = Memory;
CREATE TABLE t3 (id Nullable(UInt64), value String) ENGINE = Memory;

INSERT INTO t1 VALUES (0, 'a'), (1, 'b');
INSERT INTO t2 VALUES (0, 'x'), (1, 'y');
INSERT INTO t3 VALUES (1, 'q'), (NULL, 'r');

SELECT id
FROM t1 INNER JOIN t2 USING (id) LEFT JOIN t3 USING (id)
WHERE id = 1;

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
