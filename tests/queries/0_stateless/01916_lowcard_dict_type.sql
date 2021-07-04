DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (`x` UInt32, `lc` LowCardinality(String) ) ENGINE = Memory;
INSERT INTO t1 VALUES (1, '1'), (2, '2');

SELECT toIntervalMinute(lc) as e, toTypeName(e) FROM t1;
SELECT toIntervalDay(lc) as e, toTypeName(e) FROM t1;

CREATE TABLE t2 (`x` UInt32, `lc` LowCardinality(String) ) ENGINE = Memory;
INSERT INTO t2 VALUES (1, '61f0c404-5cb3-11e7-907b-a6006ad3dba2');

SELECT toUUID(lc) as e, toTypeName(e) FROM t2;

INSERT INTO t2 VALUES (2, '2');

SELECT toIntervalMinute(lc), toTypeName(materialize(r.lc)) FROM t1 AS l INNER JOIN t2 as r USING (lc);

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
