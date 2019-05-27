DROP TABLE IF EXISTS test.test_local;
DROP TABLE IF EXISTS test.test_distributed;

CREATE TABLE test.test_local (date Date, value Date MATERIALIZED toDate('2017-08-01')) ENGINE = MergeTree(date, date, 8192);
CREATE TABLE test.test_distributed AS test.test_local ENGINE = Distributed('test_shard_localhost', 'test', test_local, rand());

SET insert_distributed_sync=1;

INSERT INTO test.test_distributed VALUES ('2018-08-01');
SELECT * FROM test.test_distributed;
SELECT * FROM test.test_local;
SELECT date, value FROM test.test_local;
