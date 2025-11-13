select CAST((), 'SimpleAggregateFunction(min, Tuple())');

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (c0 Tuple()) ENGINE = Memory;
INSERT INTO tab VALUES (()), (()), (());

SELECT CAST(c0, 'SimpleAggregateFunction(min, Tuple())') FROM tab;

DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (c0 SimpleAggregateFunction(min, Tuple())) ENGINE = MergeTree() ORDER BY tuple();
INSERT INTO t0 (c0) VALUES (tuple());
SELECT * FROM t0;
