-- Tags: no-replicated-database
-- no-replicated-database: re-attaches an `Atomic` database with `lazy_load_tables`.

-- `ABR` is read-only and does not support `TRUNCATE`, so `TRUNCATE ALL TABLES` skips it and truncates its source tables,
-- also when the table is lazily loaded.

SET allow_experimental_abr = 1;
SET session_timezone = 'UTC';

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS lazy_load_tables = 1;

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.src_1 (ts DateTime, value UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY ts;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.src_10 (ts DateTime, value UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY ts;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.abr ENGINE = ABR('src', [1, 10], ('ts'));

SELECT 'TRUNCATE the ABR table';
TRUNCATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.abr; -- { serverError NOT_IMPLEMENTED }

INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.src_1 VALUES ('2026-01-01 00:00:00', 1);
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.src_10 VALUES ('2026-01-01 00:00:00', 10);

SELECT 'TRUNCATE ALL TABLES';
TRUNCATE ALL TABLES FROM {CLICKHOUSE_DATABASE_1:Identifier};
SELECT count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.src_1;
SELECT count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.src_10;
SELECT count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.abr WHERE ts >= toDateTime('2026-01-01 00:00:00');

INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.src_1 VALUES ('2026-01-01 00:00:00', 1);
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.src_10 VALUES ('2026-01-01 00:00:00', 10);

-- The catalog now returns the `ABR` table through `StorageTableProxy`.
DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

SELECT 'TRUNCATE ALL TABLES with a lazily loaded ABR table';
TRUNCATE ALL TABLES FROM {CLICKHOUSE_DATABASE_1:Identifier};
SELECT count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.src_1;
SELECT count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.src_10;
SELECT count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.abr WHERE ts >= toDateTime('2026-01-01 00:00:00');

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
