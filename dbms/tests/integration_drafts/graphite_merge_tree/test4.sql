DROP TABLE IF EXISTS test.graphite;
CREATE TABLE test.graphite (metric String, value Float64, timestamp UInt32, date Date, updated UInt32) ENGINE = GraphiteMergeTree(date, (metric, timestamp), 8192, 'graphite_rollup');

INSERT INTO test.graphite SELECT 'one_min.x' AS metric, toFloat64(number) AS value, toUInt32(1111111111 + intDiv(number, 3) * 600) AS timestamp, toDate('2017-02-02') AS date, toUInt32(100 - number) AS updated FROM (SELECT * FROM system.numbers LIMIT 50);
OPTIMIZE TABLE test.graphite PARTITION 201702 FINAL;
SELECT * FROM test.graphite;

INSERT INTO test.graphite SELECT 'one_min.y' AS metric, toFloat64(number) AS value, toUInt32(1111111111 + number * 600) AS timestamp, toDate('2017-02-02') AS date, toUInt32(100 - number) AS updated FROM (SELECT * FROM system.numbers LIMIT 50);
OPTIMIZE TABLE test.graphite PARTITION 201702 FINAL;
SELECT * FROM test.graphite;

DROP TABLE test.graphite;
