-- Tags: zookeeper, no-replicated-database
--       no-replicated-database: `DETACH DATABASE` / `ATTACH DATABASE`.

-- A scalar subquery with a JOIN in a TTL expression or in a CHECK constraint is executed when the table is
-- created, and again when the table is loaded, here by re-attaching the database. The tables are named with
-- their database, because a TTL expression is not analysed in the current database of the statement.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic;

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_probe (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t_probe SELECT number FROM numbers(10000);
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_build (k UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t_build SELECT number * 1000 FROM numbers(10);

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_ttl_where (d DateTime, x UInt64) ENGINE = MergeTree ORDER BY tuple()
TTL d + INTERVAL 1 YEAR WHERE x < (SELECT count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_probe AS p, {CLICKHOUSE_DATABASE_1:Identifier}.t_build AS b WHERE p.k = b.k);

-- A table function is still not allowed in such a subquery.
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_ttl_table_function (d DateTime, x UInt64) ENGINE = MergeTree ORDER BY tuple()
TTL d + INTERVAL 1 YEAR WHERE x < (SELECT count() FROM numbers(10)); -- { serverError THERE_IS_NO_QUERY }

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_check
(
    x UInt64,
    CONSTRAINT c CHECK x < (SELECT count() + 1000 FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_probe AS p, {CLICKHOUSE_DATABASE_1:Identifier}.t_build AS b WHERE p.k = b.k)
)
ENGINE = MergeTree ORDER BY tuple();

-- The JOIN is only in a scalar subquery nested in the one of the constraint.
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_check_nested
(
    x UInt64,
    CONSTRAINT c CHECK x < (SELECT count() + 1000 FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_build
        WHERE k <= (SELECT max(b.k) FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_probe AS p, {CLICKHOUSE_DATABASE_1:Identifier}.t_build AS b WHERE p.k = b.k))
)
ENGINE = MergeTree ORDER BY tuple();

-- A replicated table analyses its new TTL once more, when the replica applies the ALTER.
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_ttl_replicated (d DateTime, x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_ttl_replicated', 'r1') ORDER BY tuple();
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_ttl_replicated
MODIFY TTL d + INTERVAL 1 YEAR WHERE x < (SELECT count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_probe AS p, {CLICKHOUSE_DATABASE_1:Identifier}.t_build AS b WHERE p.k = b.k)
SETTINGS alter_sync = 2;

-- In a Replicated database the replica applies the ALTER with a query context of its own.
SET distributed_ddl_output_mode = 'none';
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_2:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_2:Identifier} ENGINE = Replicated('/test/05292/{database}', 's1', 'r1');
CREATE TABLE {CLICKHOUSE_DATABASE_2:Identifier}.t_ttl_rdb (d DateTime, x UInt64) ENGINE = ReplicatedMergeTree ORDER BY tuple();
ALTER TABLE {CLICKHOUSE_DATABASE_2:Identifier}.t_ttl_rdb
MODIFY TTL d + INTERVAL 1 YEAR WHERE x < (SELECT count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_probe AS p, {CLICKHOUSE_DATABASE_1:Identifier}.t_build AS b WHERE p.k = b.k)
SETTINGS alter_sync = 2;
SELECT 't_ttl_rdb', count() FROM {CLICKHOUSE_DATABASE_2:Identifier}.t_ttl_rdb;
DROP DATABASE {CLICKHOUSE_DATABASE_2:Identifier};

DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

SELECT 't_ttl_where', count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_ttl_where;
SELECT 't_check', count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_check;
SELECT 't_check_nested', count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_check_nested;
SELECT 't_ttl_replicated', count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_ttl_replicated;

-- When the table was created, the subquery of the TTL built a join runtime filter and the index analysis of its read used it.
SYSTEM FLUSH LOGS query_log;
SELECT 't_ttl_where runtime filter', ProfileEvents['RuntimeFiltersCreated'] > 0, ProfileEvents['RuntimeFilterGranulesConsidered'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish'
    AND query_kind = 'Create' AND query LIKE '%t_ttl_where%';

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
