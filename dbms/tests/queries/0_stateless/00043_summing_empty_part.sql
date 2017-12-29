CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS test.empty_summing;
CREATE TABLE test.empty_summing (d Date, k UInt64, v Int8) ENGINE=SummingMergeTree(d, k, 8192);

INSERT INTO test.empty_summing VALUES ('2015-01-01', 1, 10);
INSERT INTO test.empty_summing VALUES ('2015-01-01', 1, -10);

OPTIMIZE TABLE test.empty_summing;
SELECT * FROM test.empty_summing;

INSERT INTO test.empty_summing VALUES ('2015-01-01', 1, 4),('2015-01-01', 2, -9),('2015-01-01', 3, -14);
INSERT INTO test.empty_summing VALUES ('2015-01-01', 1, -2),('2015-01-01', 1, -2),('2015-01-01', 3, 14);
INSERT INTO test.empty_summing VALUES ('2015-01-01', 1, 0),('2015-01-01', 3, 0);

OPTIMIZE TABLE test.empty_summing;
SELECT * FROM test.empty_summing;

DROP TABLE test.empty_summing;
