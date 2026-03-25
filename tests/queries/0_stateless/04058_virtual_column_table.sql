-- Test that the `_table` common virtual column works correctly across all main storage engines.

SET describe_include_virtual_columns = 1;
SET allow_experimental_alias_table_engine = 1;

DROP TABLE IF EXISTS t_mt;
DROP TABLE IF EXISTS t_rmt;
DROP TABLE IF EXISTS t_memory;
DROP TABLE IF EXISTS t_log;
DROP TABLE IF EXISTS t_stripelog;
DROP TABLE IF EXISTS t_merge;
DROP TABLE IF EXISTS t_distributed;
DROP TABLE IF EXISTS t_alias;
DROP TABLE IF EXISTS t_join;
DROP TABLE IF EXISTS t_url;
DROP TABLE IF EXISTS t_gen;
DROP TABLE IF EXISTS t_mview;
DROP TABLE IF EXISTS t_mview_inner;

-- 1. MergeTree — virtual columns filled via shared_virtual_fields in ReadFromMergeTree
CREATE TABLE t_mt (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_mt VALUES (1);
SELECT 'MergeTree', x, _table FROM t_mt;
SELECT 'MergeTree+_part', x, _table, _part FROM t_mt;

-- 2. ReplicatedMergeTree
CREATE TABLE t_rmt (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_rmt', '1') ORDER BY x;
INSERT INTO t_rmt VALUES (2);
SELECT 'ReplicatedMergeTree', x, _table FROM t_rmt;

-- 3. Memory
CREATE TABLE t_memory (x UInt64) ENGINE = Memory;
INSERT INTO t_memory VALUES (3);
SELECT 'Memory', x, _table FROM t_memory;

-- 4. Log
CREATE TABLE t_log (x UInt64) ENGINE = Log;
INSERT INTO t_log VALUES (4);
SELECT 'Log', x, _table FROM t_log;

-- 5. StripeLog — Pipe-based, explicit filter+extend
CREATE TABLE t_stripelog (x UInt64) ENGINE = StripeLog;
INSERT INTO t_stripelog VALUES (5);
SELECT 'StripeLog', x, _table FROM t_stripelog;

-- 6. Merge — proxy storage, fills _table with contributing sub-table names
CREATE TABLE t_merge AS t_mt ENGINE = Merge(currentDatabase(), '^t_mt$');
SELECT 'Merge', x, _table FROM t_merge;

-- 7. Distributed — proxy storage, fills _table itself
CREATE TABLE t_distributed AS t_mt ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_mt);
SELECT 'Distributed', x, _table FROM t_distributed;

-- 8. Alias — proxy storage, delegates to target
CREATE TABLE t_alias ENGINE = Alias(t_mt);
SELECT 'Alias', x, _table FROM t_alias;

-- 9. Join — Pipe-based storage
CREATE TABLE t_join (x UInt64) ENGINE = Join(ANY, LEFT, x);
INSERT INTO t_join VALUES (9);
SELECT 'Join', x, _table FROM t_join;

-- 10. URL — file-like storage, just check DESCRIBE since URL is not reachable
CREATE TABLE t_url (x UInt64) ENGINE = URL('http://localhost:1/nonexistent', 'CSV');

-- 11. GenerateRandom — Pipe-based with filter+extend
CREATE TABLE t_gen (x UInt64) ENGINE = GenerateRandom;
SELECT 'GenerateRandom', _table FROM t_gen LIMIT 1;

-- 12. System tables — IStorageSystemOneBlock-based
SELECT 'system.one', _table FROM system.one;

-- 13. system.numbers — own read override
SELECT 'system.numbers', _table FROM system.numbers LIMIT 1;

-- 14. MaterializedView — proxy to inner storage
CREATE TABLE t_mview_inner (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE MATERIALIZED VIEW t_mview TO t_mview_inner AS SELECT x FROM t_mt;
INSERT INTO t_mt VALUES (14);
SELECT 'MaterializedView', x, _table FROM t_mview;

-- 15. Verify _table works in WHERE clause
SELECT 'WHERE_filter', x, _table FROM t_mt WHERE _table = 't_mt' ORDER BY x;

-- 16. Verify _table in Merge correctly filters by sub-table name
SELECT 'Merge_WHERE', _table FROM t_merge WHERE _table = 't_mt' LIMIT 1;

-- 17. Verify _table with only virtual column selected (no physical columns)
SELECT 'virtual_only', _table FROM t_memory;

DROP TABLE IF EXISTS t_mview;
DROP TABLE IF EXISTS t_mview_inner;
DROP TABLE IF EXISTS t_gen;
DROP TABLE IF EXISTS t_url;
DROP TABLE IF EXISTS t_join;
DROP TABLE IF EXISTS t_alias;
DROP TABLE IF EXISTS t_distributed;
DROP TABLE IF EXISTS t_merge;
DROP TABLE IF EXISTS t_stripelog;
DROP TABLE IF EXISTS t_log;
DROP TABLE IF EXISTS t_memory;
DROP TABLE IF EXISTS t_rmt;
DROP TABLE IF EXISTS t_mt;
