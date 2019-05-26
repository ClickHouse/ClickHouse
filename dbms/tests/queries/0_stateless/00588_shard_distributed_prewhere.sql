DROP TABLE IF EXISTS test.mergetree_00588;
DROP TABLE IF EXISTS test.distributed_00588;

CREATE TABLE test.mergetree_00588 (x UInt64, s String) ENGINE = MergeTree ORDER BY x;
INSERT INTO test.mergetree_00588 VALUES (1, 'hello'), (2, 'world');

SELECT * FROM test.mergetree_00588 PREWHERE x = 1 WHERE s LIKE '%l%' ORDER BY x, s;
SELECT * FROM remote('127.0.0.{1,2,3}', test.mergetree_00588) PREWHERE x = 1 WHERE s LIKE '%l%' ORDER BY x, s;

CREATE TABLE test.distributed_00588 AS test.mergetree_00588 ENGINE = Distributed(test_shard_localhost, test, mergetree_00588);

SELECT * FROM test.distributed_00588 PREWHERE x = 1 WHERE s LIKE '%l%' ORDER BY x, s;

DROP TABLE test.mergetree_00588;
DROP TABLE test.distributed_00588;
