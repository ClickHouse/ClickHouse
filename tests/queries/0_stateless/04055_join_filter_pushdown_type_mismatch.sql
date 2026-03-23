SET enable_analyzer = 1;
SET query_plan_use_new_logical_join_step = 0;
SET enable_join_runtime_filters = 0;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;

CREATE TABLE t1 (id UInt8, value String) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t2 (id UInt16, value String) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t3 (id Nullable(UInt64), value String) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t1 VALUES (0, 'a'), (1, 'b'), (2, 'c');
INSERT INTO t2 VALUES (0, 'x'), (1, 'y'), (3, 'z');
INSERT INTO t3 VALUES (0, 'p'), (1, 'q'), (NULL, 'r');

-- equals
SELECT 1 FROM t1 RIGHT JOIN t2 USING (id) LEFT JOIN t3 USING (id) WHERE id = 10;

-- notEquals
SELECT count() FROM t1 RIGHT JOIN t2 USING (id) LEFT JOIN t3 USING (id) WHERE id != 10;

-- greaterOrEquals
SELECT count() FROM t1 RIGHT JOIN t2 USING (id) LEFT JOIN t3 USING (id) WHERE id >= 1;

-- in
SELECT count() FROM t1 RIGHT JOIN t2 USING (id) LEFT JOIN t3 USING (id) WHERE id IN (1, 2);

-- Verify correct result
SELECT id FROM t1 RIGHT JOIN t2 USING (id) LEFT JOIN t3 USING (id) WHERE id = 1;

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
