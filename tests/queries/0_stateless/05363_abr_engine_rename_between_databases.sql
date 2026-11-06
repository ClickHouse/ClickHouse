-- Tags: no-replicated-database
-- no-replicated-database: moves tables between databases and re-attaches an `Atomic` database with `lazy_load_tables`.

-- The source tables of an `ABR` table are found in its own database when it is loaded, so moving it to another database
-- would make it read other tables, or none, after it is attached again. Such a move is rejected, also for a lazily loaded
-- table and from an `Ordinary` database, and the table keeps reading its source tables after it is attached again.

SET allow_experimental_abr = 1;
SET session_timezone = 'UTC';

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_2:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS lazy_load_tables = 1;

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.src_1 (ts DateTime, value UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY ts;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.src_10 (ts DateTime, value UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY ts;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.src_1 VALUES ('2026-01-01 00:00:00', 1);
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.src_10 VALUES ('2026-01-01 00:00:00', 10);
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.abr ENGINE = ABR('src', [1, 10], ('ts'));

-- Tables with the same names and other data in the destination, which the moved table would read after it is attached again.
CREATE TABLE src_1 (ts DateTime, value UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY ts;
CREATE TABLE src_10 (ts DateTime, value UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY ts;
INSERT INTO src_1 VALUES ('2026-01-01 00:00:00', 100);
INSERT INTO src_10 VALUES ('2026-01-01 00:00:00', 1000);
CREATE TABLE other (ts DateTime, value UInt64) ENGINE = MergeTree ORDER BY ts;

SELECT 'move';
RENAME TABLE {CLICKHOUSE_DATABASE_1:Identifier}.abr TO abr; -- { serverError NOT_IMPLEMENTED }
EXCHANGE TABLES {CLICKHOUSE_DATABASE_1:Identifier}.abr AND other; -- { serverError NOT_IMPLEMENTED }

SELECT 'rename within the database';
RENAME TABLE {CLICKHOUSE_DATABASE_1:Identifier}.abr TO {CLICKHOUSE_DATABASE_1:Identifier}.abr_renamed;
SELECT max(value) FROM {CLICKHOUSE_DATABASE_1:Identifier}.abr_renamed WHERE ts >= toDateTime('2026-01-01 00:00:00');

-- The catalog now returns the `ABR` table through `StorageTableProxy`.
DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

SELECT 'move a lazily loaded table';
RENAME TABLE {CLICKHOUSE_DATABASE_1:Identifier}.abr_renamed TO abr; -- { serverError NOT_IMPLEMENTED }
SELECT max(value) FROM {CLICKHOUSE_DATABASE_1:Identifier}.abr_renamed WHERE ts >= toDateTime('2026-01-01 00:00:00');
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'abr';

SELECT 'move from an Ordinary database';
SET allow_deprecated_database_ordinary = 1;
CREATE DATABASE {CLICKHOUSE_DATABASE_2:Identifier} ENGINE = Ordinary;
CREATE TABLE {CLICKHOUSE_DATABASE_2:Identifier}.src_1 (ts DateTime, value UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY ts;
INSERT INTO {CLICKHOUSE_DATABASE_2:Identifier}.src_1 VALUES ('2026-01-01 00:00:00', 2);
CREATE TABLE {CLICKHOUSE_DATABASE_2:Identifier}.abr ENGINE = ABR('src', [1], ('ts'));
RENAME TABLE {CLICKHOUSE_DATABASE_2:Identifier}.abr TO abr; -- { serverError NOT_IMPLEMENTED }
DETACH TABLE {CLICKHOUSE_DATABASE_2:Identifier}.abr;
ATTACH TABLE {CLICKHOUSE_DATABASE_2:Identifier}.abr;
SELECT max(value) FROM {CLICKHOUSE_DATABASE_2:Identifier}.abr WHERE ts >= toDateTime('2026-01-01 00:00:00');

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
DROP DATABASE {CLICKHOUSE_DATABASE_2:Identifier};
