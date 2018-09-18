DROP TABLE IF EXISTS test.test_local_1;
DROP TABLE IF EXISTS test.test_local_2;
DROP TABLE IF EXISTS test.test_distributed_1;
DROP TABLE IF EXISTS test.test_distributed_2;

CREATE TABLE test.test_local_1 (date Date, value UInt32) ENGINE = MergeTree(date, date, 8192);
CREATE TABLE test.test_local_2 (date Date, value UInt32) ENGINE = MergeTree(date, date, 8192);
CREATE TABLE test.test_distributed_1 AS test.test_local_1 ENGINE = Distributed('test_shard_localhost', 'test', test_local_1, rand());
CREATE TABLE test.test_distributed_2 AS test.test_local_2 ENGINE = Distributed('test_shard_localhost', 'test', test_local_2, rand());

INSERT INTO test.test_local_1 VALUES ('2018-08-01',100);
INSERT INTO test.test_local_2 VALUES ('2018-08-01',200);

SELECT sum(value) FROM merge('test', 'test_local_1|test_distributed_2');
SELECT sum(value) FROM merge('test', 'test_distributed_1|test_distributed_2');

DROP TABLE IF EXISTS test.test_local_1;
DROP TABLE IF EXISTS test.test_local_2;
DROP TABLE IF EXISTS test.test_distributed_1;
DROP TABLE IF EXISTS test.test_distributed_2;
