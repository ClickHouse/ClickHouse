DROP TABLE IF EXISTS mergetree_00588;
DROP TABLE IF EXISTS distributed_00588;

CREATE TABLE mergetree_00588 (x UInt64, s String) ENGINE = MergeTree ORDER BY x;
INSERT INTO mergetree_00588 VALUES (1, 'hello'), (2, 'world');

SELECT * FROM mergetree_00588 PREWHERE x = 1 WHERE s LIKE '%l%' ORDER BY x, s;
SELECT * FROM remote('127.0.0.{1,2,3}', currentDatabase(), mergetree_00588) PREWHERE x = 1 WHERE s LIKE '%l%' ORDER BY x, s;

CREATE TABLE distributed_00588 AS mergetree_00588 ENGINE = Distributed(test_shard_localhost, currentDatabase(), mergetree_00588);

SELECT * FROM distributed_00588 PREWHERE x = 1 WHERE s LIKE '%l%' ORDER BY x, s;

DROP TABLE mergetree_00588;
DROP TABLE distributed_00588;
