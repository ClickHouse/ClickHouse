DROP TABLE IF EXISTS test_merge_u32_local;
DROP TABLE IF EXISTS test_merge_s64_local;
DROP TABLE IF EXISTS test_merge_u64_local;

DROP TABLE IF EXISTS test_merge_u32_distributed;
DROP TABLE IF EXISTS test_merge_s64_distributed;

CREATE TABLE test_merge_s64_local (date Date, value Int64) ENGINE = MergeTree(date, date, 8192);
CREATE TABLE test_merge_u32_local (date Date, value UInt32) ENGINE = MergeTree(date, date, 8192);

CREATE TABLE test_merge_s64_distributed AS test_merge_s64_local ENGINE = Distributed('test_shard_localhost', currentDatabase(), test_merge_s64_local, rand());
CREATE TABLE test_merge_u32_distributed AS test_merge_u32_local ENGINE = Distributed('test_shard_localhost', currentDatabase(), test_merge_u32_local, rand());

INSERT INTO test_merge_s64_local VALUES ('2018-08-01', -1);
INSERT INTO test_merge_u32_local VALUES ('2018-08-01', 1);

SELECT '--- Result structure ---';
DESCRIBE TABLE merge(currentDatabase(), 'test_merge_.*');

SELECT '--- ORDER BY value ---';
SELECT * FROM merge(currentDatabase(), 'test_merge_.*') ORDER BY value;

SELECT '--- WHERE toTypeName(value) = Int64 ---';
SELECT * FROM merge(currentDatabase(), 'test_merge_.*') WHERE toTypeName(value) = 'Int64' ORDER BY value;

SELECT '--- WHERE date = 2018-08-01 ---';
SELECT * FROM merge(currentDatabase(), 'test_merge_.*') WHERE date = '2018-08-01' ORDER BY value;

SELECT '--- WHERE _table = test_merge_s64_distributed ---';
SELECT * FROM merge(currentDatabase(), 'test_merge_.*') WHERE _table = 'test_merge_s64_distributed' ORDER BY value;

SELECT '--- WHERE value = -1 ---';
SELECT * FROM merge(currentDatabase(), 'test_merge_.*') WHERE value = -1;

SELECT '--- WHERE value = 1048575 ---';
SELECT * FROM merge(currentDatabase(), 'test_merge_.*') WHERE value = 1048575;

DROP TABLE IF EXISTS test_merge_u64_local;
CREATE TABLE test_merge_u64_local (date Date, value UInt64) ENGINE = MergeTree(date, date, 8192);

SELECT '--- Expected error: no common type for Int64 and UInt64 ---';
SELECT * FROM merge(currentDatabase(), 'test_merge_.*') ORDER BY value; -- { serverError 386 }

DROP TABLE IF EXISTS test_merge_u32_local;
DROP TABLE IF EXISTS test_merge_s64_local;
DROP TABLE IF EXISTS test_merge_u64_local;

DROP TABLE IF EXISTS test_merge_u32_distributed;
DROP TABLE IF EXISTS test_merge_s64_distributed;
