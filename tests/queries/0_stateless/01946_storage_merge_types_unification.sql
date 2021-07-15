DROP TABLE IF EXISTS test_a64_local;
DROP TABLE IF EXISTS test_u64_local;
DROP TABLE IF EXISTS test_s64_local;

DROP TABLE IF EXISTS test_a64_distributed;
DROP TABLE IF EXISTS test_u64_distributed;
DROP TABLE IF EXISTS test_s64_distributed;

CREATE TABLE test_a64_local (date Date, value UInt64) ENGINE = MergeTree(date, date, 8192);
CREATE TABLE test_s64_local (date Date, value Int64) ENGINE = MergeTree(date, date, 8192);
CREATE TABLE test_u64_local (date Date, value UInt64) ENGINE = MergeTree(date, date, 8192);

CREATE TABLE test_a64_distributed AS test_a64_local ENGINE = Distributed('test_shard_localhost', currentDatabase(), test_a64_local, rand());
CREATE TABLE test_s64_distributed AS test_s64_local ENGINE = Distributed('test_shard_localhost', currentDatabase(), test_s64_local, rand());
CREATE TABLE test_u64_distributed AS test_u64_local ENGINE = Distributed('test_shard_localhost', currentDatabase(), test_u64_local, rand());

INSERT INTO test_a64_local VALUES ('2018-08-01', 2);
INSERT INTO test_s64_local VALUES ('2018-08-01', -1);
INSERT INTO test_u64_local VALUES ('2018-08-01', 1);

DESCRIBE TABLE merge(currentDatabase(), 'test_.64_distributed');

SELECT '--- ORDER BY value ---';
SELECT * FROM merge(currentDatabase(), 'test_.64_distributed') ORDER BY value;

SELECT '--- WHERE toTypeName(value) = Int128 ---';
SELECT * FROM merge(currentDatabase(), 'test_.64_distributed') WHERE toTypeName(value) = 'Int128' ORDER BY value;

SELECT '--- WHERE toTypeName(value) = UInt64 ---';
SELECT * FROM merge(currentDatabase(), 'test_.64_distributed') WHERE toTypeName(value) = 'UInt64';

SELECT '--- WHERE date = 2018-08-01 ---';
SELECT * FROM merge(currentDatabase(), 'test_.64_distributed') WHERE date = '2018-08-01' ORDER BY value;

SELECT '--- WHERE _table = test_s64_distributed ---';
SELECT * FROM merge(currentDatabase(), 'test_.64_distributed') WHERE _table = 'test_s64_distributed' ORDER BY value;

SELECT '--- WHERE value = -1 ---';
SELECT * FROM merge(currentDatabase(), 'test_.64_distributed') WHERE value = -1;

DROP TABLE IF EXISTS test_a64_local;
DROP TABLE IF EXISTS test_u64_local;
DROP TABLE IF EXISTS test_s64_local;

DROP TABLE IF EXISTS test_a64_distributed;
DROP TABLE IF EXISTS test_u64_distributed;
DROP TABLE IF EXISTS test_s64_distributed;
