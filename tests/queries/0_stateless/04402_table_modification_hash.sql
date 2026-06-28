-- Tests for the modification_hash column of system.tables (issue #108713).
-- modification_hash is a value that changes whenever the data behind the table changes.

DROP TABLE IF EXISTS t_mt;
DROP TABLE IF EXISTS t_mem;
DROP TABLE IF EXISTS t_log;
DROP TABLE IF EXISTS t_url_glob;
DROP TABLE IF EXISTS t_url_failover;
DROP TABLE IF EXISTS hashes;

CREATE TABLE hashes (k String, v Nullable(UInt128)) ENGINE = Memory;

-- MergeTree: the hash must change on insert and on merge, and stay stable when nothing changes.
CREATE TABLE t_mt (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_mt VALUES (1);
INSERT INTO hashes SELECT 'mt_a', modification_hash FROM system.tables WHERE database = currentDatabase() AND name = 't_mt';
INSERT INTO hashes SELECT 'mt_a2', modification_hash FROM system.tables WHERE database = currentDatabase() AND name = 't_mt';
INSERT INTO t_mt VALUES (2);
INSERT INTO hashes SELECT 'mt_b', modification_hash FROM system.tables WHERE database = currentDatabase() AND name = 't_mt';
OPTIMIZE TABLE t_mt FINAL;
INSERT INTO hashes SELECT 'mt_c', modification_hash FROM system.tables WHERE database = currentDatabase() AND name = 't_mt';

-- Memory: changes on insert.
CREATE TABLE t_mem (x UInt64) ENGINE = Memory;
INSERT INTO t_mem VALUES (1);
INSERT INTO hashes SELECT 'mem_a', modification_hash FROM system.tables WHERE database = currentDatabase() AND name = 't_mem';
INSERT INTO t_mem VALUES (2);
INSERT INTO hashes SELECT 'mem_b', modification_hash FROM system.tables WHERE database = currentDatabase() AND name = 't_mem';

-- Log: changes on insert.
CREATE TABLE t_log (x UInt64) ENGINE = Log;
INSERT INTO t_log VALUES (1);
INSERT INTO hashes SELECT 'log_a', modification_hash FROM system.tables WHERE database = currentDatabase() AND name = 't_log';
INSERT INTO t_log VALUES (2);
INSERT INTO hashes SELECT 'log_b', modification_hash FROM system.tables WHERE database = currentDatabase() AND name = 't_log';

SELECT 'mt not null', (SELECT v FROM hashes WHERE k = 'mt_a') IS NOT NULL;
SELECT 'mt stable', (SELECT v FROM hashes WHERE k = 'mt_a') = (SELECT v FROM hashes WHERE k = 'mt_a2');
SELECT 'mt changed on insert', (SELECT v FROM hashes WHERE k = 'mt_a') != (SELECT v FROM hashes WHERE k = 'mt_b');
SELECT 'mt changed on merge', (SELECT v FROM hashes WHERE k = 'mt_b') != (SELECT v FROM hashes WHERE k = 'mt_c');
SELECT 'mem changed on insert', (SELECT v FROM hashes WHERE k = 'mem_a') != (SELECT v FROM hashes WHERE k = 'mem_b');
SELECT 'log changed on insert', (SELECT v FROM hashes WHERE k = 'log_a') != (SELECT v FROM hashes WHERE k = 'log_b');

-- URL tables expand glob (`{a,b}`) and `|`-failover patterns into several
-- concrete URLs at read time, so a single probe of the literal pattern string
-- cannot guarantee change detection: modification_hash is NULL (fail closed).
-- No server is contacted - the guard returns before any HTTP request.
CREATE TABLE t_url_glob (x UInt64) ENGINE = URL('http://localhost:1/{a,b}.csv', CSV);
CREATE TABLE t_url_failover (x UInt64) ENGINE = URL('http://localhost:1/a.csv|http://localhost:1/b.csv', CSV);
SELECT 'url glob null', modification_hash IS NULL FROM system.tables WHERE database = currentDatabase() AND name = 't_url_glob';
SELECT 'url failover null', modification_hash IS NULL FROM system.tables WHERE database = currentDatabase() AND name = 't_url_failover';

-- Version-only engines (Memory/Log/StripeLog) rely on the table UUID to distinguish incarnations
-- of a same-named table together with a monotonic data_version. In an Ordinary database the UUID is
-- Nil, so DROP + CREATE restarts data_version from the same value and a recreated table holding
-- different data could collide. They fail closed there: modification_hash is NULL.
SET allow_deprecated_database_ordinary = 1;
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Ordinary;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_mem_ord (x UInt64) ENGINE = Memory;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t_mem_ord VALUES (1);
SELECT 'ordinary memory null', modification_hash IS NULL FROM system.tables WHERE database = {CLICKHOUSE_DATABASE_1:String} AND name = 't_mem_ord';
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

-- System tables cannot tell whether their data changed: modification_hash is NULL.
SELECT 'system table null', modification_hash IS NULL FROM system.tables WHERE database = 'system' AND name = 'one';

DROP TABLE hashes;
DROP TABLE t_mt;
DROP TABLE t_mem;
DROP TABLE t_log;
DROP TABLE t_url_glob;
DROP TABLE t_url_failover;
