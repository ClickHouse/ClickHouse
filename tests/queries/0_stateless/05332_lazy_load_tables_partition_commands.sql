-- Tags: no-replicated-database
-- Tag no-replicated-database: re-attaches an `Atomic` database with `lazy_load_tables`.

-- Partition commands work with a lazily loaded source or destination table, and a source that is not `MergeTree` is still refused.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS lazy_load_tables = 1;

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_src (n UInt64) ENGINE = MergeTree ORDER BY n;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_dst (n UInt64) ENGINE = MergeTree ORDER BY n;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_moved (n UInt64) ENGINE = MergeTree ORDER BY n;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_log (n UInt64) ENGINE = Log;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t_src SELECT number FROM numbers(10);
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t_dst SELECT number FROM numbers(100, 3);
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t_log SELECT number FROM numbers(5);

-- Re-attach the database so the tables become unloaded lazy proxies.
DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
USE {CLICKHOUSE_DATABASE_1:Identifier};

SELECT name, engine FROM system.tables WHERE database = currentDatabase() ORDER BY name;

SELECT 'ATTACH PARTITION FROM an unloaded source';
ALTER TABLE t_dst ATTACH PARTITION tuple() FROM t_src;
SELECT count(), sum(n) FROM t_dst;

SELECT 'MOVE PARTITION TO an unloaded destination';
ALTER TABLE t_src MOVE PARTITION tuple() TO TABLE t_moved;
SELECT count() FROM t_src;
SELECT count(), sum(n) FROM t_moved;

SELECT 'REPLACE PARTITION FROM a loaded source';
ALTER TABLE t_dst REPLACE PARTITION tuple() FROM t_moved;
SELECT count(), sum(n) FROM t_dst;

SELECT 'a source that is not MergeTree is refused';
ALTER TABLE t_dst ATTACH PARTITION tuple() FROM t_log; -- { serverError NOT_IMPLEMENTED }
SELECT count(), sum(n) FROM t_dst;

DROP TABLE t_src;
DROP TABLE t_dst;
DROP TABLE t_moved;
DROP TABLE t_log;

USE {CLICKHOUSE_DATABASE:Identifier};
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
