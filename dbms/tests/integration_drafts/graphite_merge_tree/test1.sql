DROP TABLE IF EXISTS test.graphite;
CREATE TABLE test.graphite (metric String, value Float64, timestamp UInt32, date Date, updated UInt32) ENGINE = GraphiteMergeTree(date, (metric, timestamp), 8192, 'graphite_rollup');

INSERT into test.graphite (metric, value, timestamp, date, updated) VALUES ('one_min.x1', 100, toUInt32(toDateTime('2017-02-02 18:19:00')), toDate('2017-02-02'), 1);
INSERT into test.graphite (metric, value, timestamp, date, updated) VALUES ('one_min.x1', 200, toUInt32(toDateTime('2017-02-02 18:19:00')), toDate('2017-02-02'), 2);

SELECT * FROM test.graphite ORDER BY updated;

OPTIMIZE TABLE test.graphite;

SELECT * FROM test.graphite ORDER BY updated;

DROP TABLE test.graphite;
