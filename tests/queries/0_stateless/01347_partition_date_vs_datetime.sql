DROP TABLE IF EXISTS test_datetime;
CREATE TABLE test_datetime (time DateTime) ENGINE=MergeTree PARTITION BY time ORDER BY time;
INSERT INTO test_datetime (time) VALUES (toDate(18012));
SELECT * FROM test_datetime WHERE time=toDate(18012);
DROP TABLE test_datetime;
