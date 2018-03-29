DROP TABLE IF EXISTS test.mergetree;
DROP TABLE IF EXISTS test.distributed;

CREATE TABLE test.mergetree (x UInt64, s String) ENGINE = MergeTree ORDER BY x;
INSERT INTO test.mergetree VALUES (1, 'hello'), (2, 'world');

SELECT CASE x WHEN 1 THEN 'hello' WHEN 2 THEN 'world' ELSE  'unknow' END FROM test.mergetree;
SELECT count() AS cnt FROM (SELECT CASE x WHEN 1 THEN 'hello' WHEN 2 THEN 'world' ELSE  'unknow' END FROM test.mergetree);

CREATE TABLE test.distributed AS test.mergetree ENGINE = Distributed(test_shard_localhost, test, mergetree);

SELECT CASE x WHEN 1 THEN 'hello' WHEN 2 THEN 'world' ELSE  'unknow' END FROM test.distributed;
SELECT count() AS cnt FROM (SELECT CASE x WHEN 1 THEN 'hello' WHEN 2 THEN 'world' ELSE  'unknow' END FROM test.distributed);

DROP TABLE test.mergetree;
DROP TABLE test.distributed;
