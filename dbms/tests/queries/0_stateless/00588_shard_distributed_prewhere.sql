DROP TABLE IF EXISTS test.mergetree;
DROP TABLE IF EXISTS test.distributed;

CREATE TABLE test.mergetree (x UInt64, s String) ENGINE = MergeTree ORDER BY x;
INSERT INTO test.mergetree VALUES (1, 'hello'), (2, 'world');

SELECT * FROM test.mergetree PREWHERE x = 1 WHERE s LIKE '%l%' ORDER BY x, s;
SELECT * FROM remote('127.0.0.{1,2,3}', test.mergetree) PREWHERE x = 1 WHERE s LIKE '%l%' ORDER BY x, s;

CREATE TABLE test.distributed AS test.mergetree ENGINE = Distributed(test_shard_localhost, test, mergetree);

SELECT * FROM test.distributed PREWHERE x = 1 WHERE s LIKE '%l%' ORDER BY x, s;

DROP TABLE test.mergetree;
DROP TABLE test.distributed;
