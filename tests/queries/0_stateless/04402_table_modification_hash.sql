-- Tests for the modification_hash column of system.tables (issue #108713).
-- modification_hash is a value that changes whenever the data behind the table changes.

DROP TABLE IF EXISTS t_mt;
DROP TABLE IF EXISTS t_mem;
DROP TABLE IF EXISTS t_log;
DROP TABLE IF EXISTS t_sample;
DROP TABLE IF EXISTS t_order;
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

-- MergeTree: the sampling key affects which rows a SAMPLE query returns, so a metadata-only
-- ALTER of the sampling key (which does not rewrite parts) must still change the hash.
CREATE TABLE t_sample (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY (a, b) SAMPLE BY a;
INSERT INTO t_sample SELECT number, number FROM numbers(8);
INSERT INTO hashes SELECT 'samp_a', modification_hash FROM system.tables WHERE database = currentDatabase() AND name = 't_sample';
ALTER TABLE t_sample REMOVE SAMPLE BY;
INSERT INTO hashes SELECT 'samp_b', modification_hash FROM system.tables WHERE database = currentDatabase() AND name = 't_sample';

-- The sorting key is the deduplication key of the ReplacingMergeTree family, so a metadata-only
-- ALTER MODIFY ORDER BY changes FINAL results without rewriting any parts, and the hash must change.
-- Two separate inserts (two parts) so FINAL deduplicates across parts: with ORDER BY (k, v) the two
-- rows are distinct (count 2), after MODIFY ORDER BY k they collapse on k (count 1).
CREATE TABLE t_order (k UInt64, v UInt64) ENGINE = ReplacingMergeTree PRIMARY KEY k ORDER BY (k, v);
INSERT INTO t_order VALUES (1, 10);
INSERT INTO t_order VALUES (1, 20);
INSERT INTO hashes SELECT 'ord_a', modification_hash FROM system.tables WHERE database = currentDatabase() AND name = 't_order';
INSERT INTO hashes SELECT 'ord_final_a', count() FROM t_order FINAL;
ALTER TABLE t_order MODIFY ORDER BY k;
INSERT INTO hashes SELECT 'ord_b', modification_hash FROM system.tables WHERE database = currentDatabase() AND name = 't_order';
INSERT INTO hashes SELECT 'ord_final_b', count() FROM t_order FINAL;

SELECT 'mt not null', (SELECT v FROM hashes WHERE k = 'mt_a') IS NOT NULL;
SELECT 'mt stable', (SELECT v FROM hashes WHERE k = 'mt_a') = (SELECT v FROM hashes WHERE k = 'mt_a2');
SELECT 'mt changed on insert', (SELECT v FROM hashes WHERE k = 'mt_a') != (SELECT v FROM hashes WHERE k = 'mt_b');
SELECT 'mt changed on merge', (SELECT v FROM hashes WHERE k = 'mt_b') != (SELECT v FROM hashes WHERE k = 'mt_c');
SELECT 'mem changed on insert', (SELECT v FROM hashes WHERE k = 'mem_a') != (SELECT v FROM hashes WHERE k = 'mem_b');
SELECT 'log changed on insert', (SELECT v FROM hashes WHERE k = 'log_a') != (SELECT v FROM hashes WHERE k = 'log_b');
SELECT 'sample key change', (SELECT v FROM hashes WHERE k = 'samp_a') != (SELECT v FROM hashes WHERE k = 'samp_b');
SELECT 'order key change', (SELECT v FROM hashes WHERE k = 'ord_a') != (SELECT v FROM hashes WHERE k = 'ord_b');
SELECT 'order final result changed', (SELECT v FROM hashes WHERE k = 'ord_final_a') != (SELECT v FROM hashes WHERE k = 'ord_final_b');

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
-- Creating an Ordinary database emits a one-time server warning that the client forwards to stderr;
-- silence it so the flaky check (which fails on any stderr) stays green, as 01053 does.
SET send_logs_level = 'fatal';
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Ordinary;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_mem_ord (x UInt64) ENGINE = Memory;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t_mem_ord VALUES (1);
SELECT 'ordinary memory null', modification_hash IS NULL FROM system.tables WHERE database = {CLICKHOUSE_DATABASE_1:String} AND name = 't_mem_ord';

-- A self-referential Distributed table (its local shard resolves back to itself) must fail closed
-- (NULL) instead of recursing into its own getModificationHash. CREATE rejects self-reference, but
-- ATTACH (loading metadata) does not, so such a table can exist. ATTACH with a definition requires an
-- Ordinary database, which is why this case lives in this section.
ATTACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_dist_self (x UInt64)
    ENGINE = Distributed(test_shard_localhost, {CLICKHOUSE_DATABASE_1:String}, t_dist_self);
SELECT 'distributed self-ref null', modification_hash IS NULL FROM system.tables WHERE database = {CLICKHOUSE_DATABASE_1:String} AND name = 't_dist_self';

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

-- System tables cannot tell whether their data changed: modification_hash is NULL.
SELECT 'system table null', modification_hash IS NULL FROM system.tables WHERE database = 'system' AND name = 'one';

DROP TABLE hashes;
DROP TABLE t_mt;
DROP TABLE t_mem;
DROP TABLE t_log;
DROP TABLE t_sample;
DROP TABLE t_order;
DROP TABLE t_url_glob;
DROP TABLE t_url_failover;
