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

SELECT '--------------Single Local------------';
SELECT * FROM merge('test', 'test_local_1');
SELECT *, _table FROM merge('test', 'test_local_1') ORDER BY _table;
SELECT sum(value), _table FROM merge('test', 'test_local_1') GROUP BY _table ORDER BY _table;
SELECT * FROM merge('test', 'test_local_1') WHERE _table = 'test_local_1';
SELECT * FROM merge('test', 'test_local_1') PREWHERE _table = 'test_local_1';
SELECT * FROM merge('test', 'test_local_1') WHERE _table in ('test_local_1', 'test_local_2');
SELECT * FROM merge('test', 'test_local_1') PREWHERE _table in ('test_local_1', 'test_local_2');

SELECT '--------------Single Distributed------------';
SELECT * FROM merge('test', 'test_distributed_1');
SELECT *, _table FROM merge('test', 'test_distributed_1') ORDER BY _table;
SELECT sum(value), _table FROM merge('test', 'test_distributed_1') GROUP BY _table ORDER BY _table;
SELECT * FROM merge('test', 'test_distributed_1') WHERE _table = 'test_distributed_1';
SELECT * FROM merge('test', 'test_distributed_1') PREWHERE _table = 'test_distributed_1';
SELECT * FROM merge('test', 'test_distributed_1') WHERE _table in ('test_distributed_1', 'test_distributed_2');
SELECT * FROM merge('test', 'test_distributed_1') PREWHERE _table in ('test_distributed_1', 'test_distributed_2');

SELECT '--------------Local Merge Local------------';
SELECT * FROM merge('test', 'test_local_1|test_local_2');
SELECT *, _table FROM merge('test', 'test_local_1|test_local_2') ORDER BY _table;
SELECT sum(value), _table FROM merge('test', 'test_local_1|test_local_2') GROUP BY _table ORDER BY _table;
SELECT * FROM merge('test', 'test_local_1|test_local_2') WHERE _table = 'test_local_1';
SELECT * FROM merge('test', 'test_local_1|test_local_2') PREWHERE _table = 'test_local_1';
SELECT * FROM merge('test', 'test_local_1|test_local_2') WHERE _table in ('test_local_1', 'test_local_2') ORDER BY value;
SELECT * FROM merge('test', 'test_local_1|test_local_2') PREWHERE _table in ('test_local_1', 'test_local_2') ORDER BY value;

SELECT '--------------Local Merge Distributed------------';
SELECT * FROM merge('test', 'test_local_1|test_distributed_2');
SELECT *, _table FROM merge('test', 'test_local_1|test_distributed_2') ORDER BY _table;
SELECT sum(value), _table FROM merge('test', 'test_local_1|test_distributed_2') GROUP BY _table ORDER BY _table;
SELECT * FROM merge('test', 'test_local_1|test_distributed_2') WHERE _table = 'test_local_1';
SELECT * FROM merge('test', 'test_local_1|test_distributed_2') PREWHERE _table = 'test_local_1';
SELECT * FROM merge('test', 'test_local_1|test_distributed_2') WHERE _table in ('test_local_1', 'test_distributed_2') ORDER BY value;
SELECT * FROM merge('test', 'test_local_1|test_distributed_2') PREWHERE _table in ('test_local_1', 'test_distributed_2') ORDER BY value;

SELECT '--------------Distributed Merge Distributed------------';
SELECT * FROM merge('test', 'test_distributed_1|test_distributed_2');
SELECT *, _table FROM merge('test', 'test_distributed_1|test_distributed_2') ORDER BY _table;
SELECT sum(value), _table FROM merge('test', 'test_distributed_1|test_distributed_2') GROUP BY _table ORDER BY _table;
SELECT * FROM merge('test', 'test_distributed_1|test_distributed_2') WHERE _table = 'test_distributed_1';
SELECT * FROM merge('test', 'test_distributed_1|test_distributed_2') PREWHERE _table = 'test_distributed_1';
SELECT * FROM merge('test', 'test_distributed_1|test_distributed_2') WHERE _table in ('test_distributed_1', 'test_distributed_2') ORDER BY value;
SELECT * FROM merge('test', 'test_distributed_1|test_distributed_2') PREWHERE _table in ('test_distributed_1', 'test_distributed_2') ORDER BY value;

DROP TABLE IF EXISTS test.test_local_1;
DROP TABLE IF EXISTS test.test_local_2;
DROP TABLE IF EXISTS test.test_distributed_1;
DROP TABLE IF EXISTS test.test_distributed_2;
