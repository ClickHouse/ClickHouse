DROP TABLE IF EXISTS test.graphite;
CREATE TABLE test.graphite (metric String, value Float64, timestamp UInt32, date Date, updated UInt32) ENGINE = GraphiteMergeTree(date, (metric, timestamp), 8192, 'graphite_rollup');

INSERT INTO test.graphite SELECT 'one_min.x' AS metric, toFloat64(number) AS value, toUInt32(1111111111 + intDiv(number, 3)) AS timestamp, toDate('2017-02-02') AS date, toUInt32(intDiv(number, 2)) AS updated FROM (SELECT * FROM system.numbers LIMIT 1000000) WHERE intDiv(timestamp, 600) * 600 = 1111444200;
OPTIMIZE TABLE test.graphite PARTITION 201702 FINAL;
SELECT * FROM test.graphite;

SELECT avg(v), max(upd) FROM (SELECT timestamp, argMax(value, (updated, number)) AS v, max(updated) AS upd FROM (SELECT 'one_min.x5' AS metric, toFloat64(number) AS value, toUInt32(1111111111 + intDiv(number, 3)) AS timestamp, toDate('2017-02-02') AS date, toUInt32(intDiv(number, 2)) AS updated, number FROM system.numbers LIMIT 1000000) WHERE intDiv(timestamp, 600) * 600 = 1111444200 GROUP BY timestamp);

DROP TABLE test.graphite;
