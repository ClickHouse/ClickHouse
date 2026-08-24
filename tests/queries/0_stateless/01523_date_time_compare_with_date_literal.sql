DROP TABLE IF EXISTS test;

CREATE TABLE test(timestamp DateTime) ENGINE = MergeTree ORDER BY timestamp;

INSERT INTO test VALUES ('2020-10-15 00:00:00');
INSERT INTO test VALUES ('2020-10-15 12:00:00');
INSERT INTO test VALUES ('2020-10-16 00:00:00');

SELECT 'DateTime';
SELECT * FROM test WHERE timestamp != '2020-10-15' ORDER BY timestamp;
SELECT '';
SELECT * FROM test WHERE timestamp == '2020-10-15' ORDER BY timestamp;
SELECT '';
SELECT * FROM test WHERE timestamp > '2020-10-15' ORDER BY timestamp;
SELECT '';
SELECT * FROM test WHERE timestamp >= '2020-10-15' ORDER by timestamp;
SELECT '';
SELECT * FROM test WHERE timestamp < '2020-10-16' ORDER BY timestamp;
SELECT '';
SELECT * FROM test WHERE timestamp <= '2020-10-16' ORDER BY timestamp;
SELECT '';
SELECT '';
SELECT * FROM test WHERE '2020-10-15' != timestamp ORDER BY timestamp;
SELECT '';
SELECT * FROM test WHERE '2020-10-15' == timestamp ORDER BY timestamp;
SELECT '';
SELECT * FROM test WHERE '2020-10-15' < timestamp ORDER BY timestamp;
SELECT '';
SELECT * FROM test WHERE '2020-10-15' <= timestamp ORDER BY timestamp;
SELECT '';
SELECT * FROM test WHERE '2020-10-16' > timestamp ORDER BY timestamp;
SELECT '';
SELECT * FROM test WHERE '2020-10-16' >= timestamp ORDER BY timestamp;
SELECT '';

DROP TABLE test;
CREATE TABLE test(timestamp DateTime64) ENGINE = MergeTree ORDER BY timestamp;

INSERT INTO test VALUES ('2020-10-15 00:00:00');
INSERT INTO test VALUES ('2020-10-15 12:00:00');
INSERT INTO test VALUES ('2020-10-16 00:00:00');

SELECT 'DateTime64';
SELECT * FROM test WHERE timestamp != '2020-10-15' ORDER BY timestamp;
SELECT '';
SELECT * FROM test WHERE timestamp == '2020-10-15' ORDER BY timestamp;
SELECT '';
SELECT * FROM test WHERE timestamp > '2020-10-15' ORDER BY timestamp;
SELECT '';
SELECT * FROM test WHERE timestamp >= '2020-10-15' ORDER BY timestamp;
SELECT '';
SELECT * FROM test WHERE timestamp < '2020-10-16' ORDER BY timestamp;
SELECT '';
SELECT * FROM test WHERE timestamp <= '2020-10-16' ORDER BY timestamp;
SELECT '';
SELECT '';
SELECT * FROM test WHERE '2020-10-15' != timestamp ORDER BY timestamp;
SELECT '';
SELECT * FROM test WHERE '2020-10-15' == timestamp ORDER BY timestamp;
SELECT '';
SELECT * FROM test WHERE '2020-10-15' < timestamp ORDER BY timestamp;
SELECT '';
SELECT * FROM test WHERE '2020-10-15' <= timestamp ORDER BY timestamp;
SELECT '';
SELECT * FROM test WHERE '2020-10-16' > timestamp ORDER BY timestamp;
SELECT '';
SELECT * FROM test WHERE '2020-10-16' >= timestamp ORDER BY timestamp;
SELECT '';

DROP TABLE test;
