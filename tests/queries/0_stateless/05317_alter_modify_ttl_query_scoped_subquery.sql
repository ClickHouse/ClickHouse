-- Tags: zookeeper, no-replicated-database
--       no-replicated-database: the replicated table has its own Keeper path, and the test creates a Replicated database.

-- ALTER TABLE ... MODIFY TTL accepts only a TTL that the table can be loaded with. A table is loaded with its TTL
-- analyzed in the global context, so a subquery of the TTL cannot read anything that exists only for the query or
-- the session of the ALTER: a table function, a view over one, a parameterized view or a temporary table.
-- CREATE TABLE rejects the same TTL.

SET ast_fuzzer_any_query = 0;
SET materialize_ttl_after_modify = 0;

DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS t_rmt;
DROP TABLE IF EXISTS t_build;
DROP TABLE IF EXISTS t_probe;
DROP TABLE IF EXISTS v_numbers;
DROP TABLE IF EXISTS pv;

CREATE TABLE t_build (k UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_build SELECT number * 1000 FROM numbers(10);
CREATE TABLE t_probe (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_probe SELECT number FROM numbers(100);
CREATE VIEW v_numbers AS SELECT number FROM numbers(10);
CREATE VIEW pv AS SELECT k FROM t_build WHERE k > {p:UInt64};
CREATE TEMPORARY TABLE tmp_05317_ttl (k UInt64);
INSERT INTO tmp_05317_ttl VALUES (1);

CREATE TABLE t (d DateTime, x UInt64, y UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t VALUES ('2100-01-01 00:00:00', 1, 1);

CREATE TABLE t_create (d DateTime, x UInt64) ENGINE = MergeTree ORDER BY tuple()
TTL d + INTERVAL 1 YEAR WHERE x < (SELECT count() FROM numbers(10)); -- { serverError THERE_IS_NO_QUERY }

ALTER TABLE t MODIFY TTL d + INTERVAL 1 YEAR WHERE x < (SELECT count() FROM numbers(10)); -- { serverError THERE_IS_NO_QUERY }
ALTER TABLE t MODIFY TTL d + INTERVAL 1 YEAR WHERE x < (SELECT count() FROM (SELECT * FROM numbers(10))); -- { serverError THERE_IS_NO_QUERY }
ALTER TABLE t MODIFY TTL d + INTERVAL 1 YEAR WHERE x IN (SELECT number FROM numbers(10)); -- { serverError THERE_IS_NO_QUERY }
ALTER TABLE t MODIFY TTL d + INTERVAL 1 YEAR GROUP BY x SET y = (SELECT count() FROM numbers(10)); -- { serverError THERE_IS_NO_QUERY }
ALTER TABLE t MODIFY TTL d + INTERVAL 1 YEAR WHERE x < (SELECT count() FROM merge({CLICKHOUSE_DATABASE:String}, '^t_build$')); -- { serverError THERE_IS_NO_QUERY }
ALTER TABLE t MODIFY TTL d + INTERVAL 1 YEAR
WHERE x < (SELECT count() FROM t_probe AS p, remote('127.0.0.1', {CLICKHOUSE_DATABASE:String}, 't_build') AS b WHERE p.k != b.k); -- { serverError THERE_IS_NO_QUERY }
ALTER TABLE t MODIFY TTL d + INTERVAL 1 YEAR WHERE x < (SELECT count() FROM v_numbers); -- { serverError THERE_IS_NO_QUERY }
ALTER TABLE t MODIFY TTL d + INTERVAL 1 YEAR WHERE x < (SELECT count() FROM pv(p = 1)); -- { serverError THERE_IS_NO_QUERY }
ALTER TABLE t MODIFY TTL d + INTERVAL 1 YEAR WHERE x < (SELECT count() FROM tmp_05317_ttl); -- { serverError UNKNOWN_TABLE }
ALTER TABLE t MODIFY TTL d + INTERVAL 2 YEAR, d + INTERVAL 1 YEAR WHERE x < (SELECT count() FROM numbers(10)); -- { serverError THERE_IS_NO_QUERY }

-- Tables of a database are found when the table is loaded.
ALTER TABLE t MODIFY TTL d + INTERVAL 1 YEAR WHERE x < (SELECT count() FROM t_build);
ALTER TABLE t MODIFY TTL d + INTERVAL 1 YEAR WHERE x IN (SELECT k FROM t_build);
ALTER TABLE t MODIFY TTL d + INTERVAL 1 YEAR WHERE x < (SELECT count() FROM t_probe AS p, t_build AS b WHERE p.k = b.k);

DETACH TABLE t;
ATTACH TABLE t;
SELECT count() FROM t;
SELECT replaceAll(extract(create_table_query, 'TTL (.*) SETTINGS '), currentDatabase() || '.', '')
FROM system.tables WHERE database = currentDatabase() AND name = 't';

-- A replicated table: the TTL does not reach the table metadata in Keeper.
CREATE TABLE t_rmt (d DateTime, x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_rmt', 'r1') ORDER BY tuple();
ALTER TABLE t_rmt MODIFY TTL d + INTERVAL 1 YEAR WHERE x < (SELECT count() FROM numbers(10)) SETTINGS alter_sync = 0; -- { serverError THERE_IS_NO_QUERY }
SELECT countIf(value LIKE '%\nttl: %'), count()
FROM system.zookeeper WHERE path = '/clickhouse/tables/' || currentDatabase() || '/t_rmt' AND name = 'metadata';

-- A table of a Replicated database: the ALTER is rejected, and the database loads again.
SET distributed_ddl_output_mode = 'none';
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Replicated('/test/05317/{database}', 's1', 'r1');
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_rdb (d DateTime, x UInt64) ENGINE = ReplicatedMergeTree ORDER BY tuple();
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_rdb MODIFY TTL d + INTERVAL 1 YEAR WHERE x < (SELECT count() FROM numbers(10)); -- { serverError THERE_IS_NO_QUERY }
DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
SELECT count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_rdb;
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

DROP TABLE t;
DROP TABLE t_rmt;
DROP TABLE pv;
DROP TABLE v_numbers;
DROP TABLE t_probe;
DROP TABLE t_build;
