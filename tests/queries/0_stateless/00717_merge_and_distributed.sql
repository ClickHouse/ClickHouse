-- Tags: distributed

SET send_logs_level = 'fatal';

DROP TABLE IF EXISTS test_local_1;
DROP TABLE IF EXISTS test_local_2;
DROP TABLE IF EXISTS test_distributed_1;
DROP TABLE IF EXISTS test_distributed_2;

CREATE TABLE test_local_1 (date Date, value UInt32) ENGINE = MergeTree(date, date, 8192);
CREATE TABLE test_local_2 (date Date, value UInt32) ENGINE = MergeTree(date, date, 8192);
CREATE TABLE test_distributed_1 AS test_local_1 ENGINE = Distributed('test_shard_localhost', currentDatabase(), test_local_1, rand());
CREATE TABLE test_distributed_2 AS test_local_2 ENGINE = Distributed('test_shard_localhost', currentDatabase(), test_local_2, rand());

INSERT INTO test_local_1 VALUES ('2018-08-01',100);
INSERT INTO test_local_2 VALUES ('2018-08-01',200);

SELECT '--------------Single Local------------';
SELECT * FROM merge(currentDatabase(), 'test_local_1');
SELECT *, _table FROM merge(currentDatabase(), 'test_local_1') ORDER BY _table;
SELECT sum(value), _table FROM merge(currentDatabase(), 'test_local_1') GROUP BY _table ORDER BY _table;
SELECT * FROM merge(currentDatabase(), 'test_local_1') WHERE _table = 'test_local_1';
SELECT * FROM merge(currentDatabase(), 'test_local_1') PREWHERE _table = 'test_local_1'; -- { serverError 10 }
SELECT * FROM merge(currentDatabase(), 'test_local_1') WHERE _table in ('test_local_1', 'test_local_2');
SELECT * FROM merge(currentDatabase(), 'test_local_1') PREWHERE _table in ('test_local_1', 'test_local_2'); -- { serverError 10 }

SELECT '--------------Single Distributed------------';
SELECT * FROM merge(currentDatabase(), 'test_distributed_1');
SELECT *, _table FROM merge(currentDatabase(), 'test_distributed_1') ORDER BY _table;
SELECT sum(value), _table FROM merge(currentDatabase(), 'test_distributed_1') GROUP BY _table ORDER BY _table;
SELECT * FROM merge(currentDatabase(), 'test_distributed_1') WHERE _table = 'test_distributed_1';
SELECT * FROM merge(currentDatabase(), 'test_distributed_1') PREWHERE _table = 'test_distributed_1';
SELECT * FROM merge(currentDatabase(), 'test_distributed_1') WHERE _table in ('test_distributed_1', 'test_distributed_2');
SELECT * FROM merge(currentDatabase(), 'test_distributed_1') PREWHERE _table in ('test_distributed_1', 'test_distributed_2');

SELECT '--------------Local Merge Local------------';
SELECT * FROM merge(currentDatabase(), 'test_local_1|test_local_2') ORDER BY _table;
SELECT *, _table FROM merge(currentDatabase(), 'test_local_1|test_local_2') ORDER BY _table;
SELECT sum(value), _table FROM merge(currentDatabase(), 'test_local_1|test_local_2') GROUP BY _table ORDER BY _table;
SELECT * FROM merge(currentDatabase(), 'test_local_1|test_local_2') WHERE _table = 'test_local_1';
SELECT * FROM merge(currentDatabase(), 'test_local_1|test_local_2') PREWHERE _table = 'test_local_1'; -- { serverError 10 }
SELECT * FROM merge(currentDatabase(), 'test_local_1|test_local_2') WHERE _table in ('test_local_1', 'test_local_2') ORDER BY value;
SELECT * FROM merge(currentDatabase(), 'test_local_1|test_local_2') PREWHERE _table in ('test_local_1', 'test_local_2') ORDER BY value; -- { serverError 10 }

SELECT '--------------Local Merge Distributed------------';
SELECT * FROM merge(currentDatabase(), 'test_local_1|test_distributed_2') ORDER BY _table;
SELECT *, _table FROM merge(currentDatabase(), 'test_local_1|test_distributed_2') ORDER BY _table;
SELECT sum(value), _table FROM merge(currentDatabase(), 'test_local_1|test_distributed_2') GROUP BY _table ORDER BY _table;
SELECT * FROM merge(currentDatabase(), 'test_local_1|test_distributed_2') WHERE _table = 'test_local_1';
SELECT * FROM merge(currentDatabase(), 'test_local_1|test_distributed_2') PREWHERE _table = 'test_local_1';
SELECT * FROM merge(currentDatabase(), 'test_local_1|test_distributed_2') WHERE _table in ('test_local_1', 'test_distributed_2') ORDER BY value;
SELECT * FROM merge(currentDatabase(), 'test_local_1|test_distributed_2') PREWHERE _table in ('test_local_1', 'test_distributed_2') ORDER BY value;

SELECT '--------------Distributed Merge Distributed------------';
SELECT * FROM merge(currentDatabase(), 'test_distributed_1|test_distributed_2') ORDER BY _table;
SELECT *, _table FROM merge(currentDatabase(), 'test_distributed_1|test_distributed_2') ORDER BY _table;
SELECT sum(value), _table FROM merge(currentDatabase(), 'test_distributed_1|test_distributed_2') GROUP BY _table ORDER BY _table;
SELECT * FROM merge(currentDatabase(), 'test_distributed_1|test_distributed_2') WHERE _table = 'test_distributed_1';
SELECT * FROM merge(currentDatabase(), 'test_distributed_1|test_distributed_2') PREWHERE _table = 'test_distributed_1';
SELECT * FROM merge(currentDatabase(), 'test_distributed_1|test_distributed_2') WHERE _table in ('test_distributed_1', 'test_distributed_2') ORDER BY value;
SELECT * FROM merge(currentDatabase(), 'test_distributed_1|test_distributed_2') PREWHERE _table in ('test_distributed_1', 'test_distributed_2') ORDER BY value;

DROP TABLE IF EXISTS test_local_1;
DROP TABLE IF EXISTS test_local_2;
DROP TABLE IF EXISTS test_distributed_1;
DROP TABLE IF EXISTS test_distributed_2;

DROP TABLE IF EXISTS test_u64_local;
DROP TABLE IF EXISTS test_s64_local;
DROP TABLE IF EXISTS test_u64_distributed;
DROP TABLE IF EXISTS test_s64_distributed;

CREATE TABLE test_s64_local (date Date, value Int64) ENGINE = MergeTree(date, date, 8192);
CREATE TABLE test_u64_local (date Date, value UInt64) ENGINE = MergeTree(date, date, 8192);
CREATE TABLE test_s64_distributed AS test_s64_local ENGINE = Distributed('test_shard_localhost', currentDatabase(), test_s64_local, rand());
CREATE TABLE test_u64_distributed AS test_u64_local ENGINE = Distributed('test_shard_localhost', currentDatabase(), test_u64_local, rand());

INSERT INTO test_s64_local VALUES ('2018-08-01', -1);
INSERT INTO test_u64_local VALUES ('2018-08-01', 1);

SELECT '--------------Implicit type conversion------------';
SELECT * FROM merge(currentDatabase(), 'test_s64_distributed|test_u64_distributed') ORDER BY value;
SELECT * FROM merge(currentDatabase(), 'test_s64_distributed|test_u64_distributed') WHERE date = '2018-08-01' ORDER BY value;
SELECT * FROM merge(currentDatabase(), 'test_s64_distributed|test_u64_distributed') WHERE _table = 'test_u64_distributed' ORDER BY value;
SELECT * FROM merge(currentDatabase(), 'test_s64_distributed|test_u64_distributed') WHERE value = 1;

DROP TABLE IF EXISTS test_u64_local;
DROP TABLE IF EXISTS test_s64_local;
DROP TABLE IF EXISTS test_u64_distributed;
DROP TABLE IF EXISTS test_s64_distributed;
