-- Tags: no-parallel-replicas

-- FIXME: with parallel replicas:
-- Cannot convert column `__table1.b` because it is non constant in source stream but must be constant in result. (ILLEGAL_COLUMN)

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t1lc;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t2lc;

SET allow_suspicious_low_cardinality_types = 1;
SET enable_analyzer = 1;

CREATE TABLE t1 (`a`  UInt64, `b` Int32 ALIAS 1) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t1lc (`a`  UInt64, `b` LowCardinality(Int32) ALIAS 1) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t1 VALUES (1), (2), (3);
INSERT INTO t1lc VALUES (1), (2), (3);

CREATE TABLE t2 (`a` UInt64, `b` Nullable(Int64) ) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t2lc (`a` UInt64, `b` LowCardinality(Nullable(Int64)) ) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t2 VALUES (1, 1), (2, 1), (3, 3);
INSERT INTO t2lc VALUES (1, 1), (2, 1), (3, 3);

SELECT b FROM t1 JOIN t2 USING (b) ORDER BY ALL;
SELECT b FROM t1lc JOIN t2lc USING (b) ORDER BY ALL;
SELECT b FROM t1lc JOIN t2 USING (b) ORDER BY ALL;

SELECT * FROM t1 JOIN t2 USING (b) ORDER BY ALL;
SELECT * FROM t1lc JOIN t2lc USING (b) ORDER BY ALL;
SELECT * FROM t1lc JOIN t2 USING (b) ORDER BY ALL;

SELECT t1.* FROM t1 JOIN t2 USING (b) ORDER BY ALL;
SELECT t1.b FROM t1 JOIN t2 USING (b) ORDER BY ALL;
SELECT t1.*, t2.* FROM t1 JOIN t2 USING (b) ORDER BY ALL;
SELECT t1lc.*, t2lc.* FROM t1lc JOIN t2lc USING (b) ORDER BY ALL;
SELECT t1lc.*, t2.* FROM t1lc JOIN t2 USING (b) ORDER BY ALL;
