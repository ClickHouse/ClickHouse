-- https://github.com/ClickHouse/ClickHouse/issues/89407
-- coalesce with FULL OUTER JOIN over tables with different integer widths
-- should not cause a logical error due to type mismatch.

DROP TABLE IF EXISTS t1_04077;
DROP TABLE IF EXISTS t2_04077;
DROP TABLE IF EXISTS t3_04077;

CREATE TABLE t1_04077 (x UInt8) ENGINE = Memory;
INSERT INTO t1_04077 VALUES (1);
CREATE TABLE t2_04077 (x UInt16) ENGINE = Memory;
INSERT INTO t2_04077 VALUES (1);
CREATE TABLE t3_04077 (x UInt32) ENGINE = Memory;
INSERT INTO t3_04077 VALUES (1);

SET query_plan_use_new_logical_join_step = 0;
SELECT coalesce(t2_04077.x, t1_04077.x) FROM t1_04077 FULL OUTER JOIN t2_04077 USING (x) FULL OUTER JOIN t3_04077 USING (x);

DROP TABLE t1_04077;
DROP TABLE t2_04077;
DROP TABLE t3_04077;
