-- Tags: no-replicated-database
-- no-replicated-database: re-attaches an `Atomic` database with `lazy_load_tables`.

-- `ABR` routes a query by the filter on its time column, which the planner collects for it. It must also do so when
-- the table is read through an `Alias`, or through the proxy of a lazily loaded table.

SET allow_experimental_abr = 1;
SET session_timezone = 'UTC';
-- Keep the filter in `WHERE`, which only reaches `ABR` when the planner collects it.
SET optimize_move_to_prewhere = 0;

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS lazy_load_tables = 1;
USE {CLICKHOUSE_DATABASE_1:Identifier};

CREATE TABLE events_1 (ts DateTime, value UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY ts;
CREATE TABLE events_10 (ts DateTime, value UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY ts;

-- Interval 1 has data from 2026-01-01, interval 10 from 2025-12-01. `max(value)` tells which table was read.
INSERT INTO events_1 VALUES ('2026-01-01 00:00:00', 1), ('2026-01-02 00:00:00', 1);
INSERT INTO events_10 VALUES ('2025-12-01 00:00:00', 10), ('2026-01-01 00:00:00', 10), ('2026-01-02 00:00:00', 10);

CREATE TABLE abr ENGINE = ABR('events', [1, 10], ('ts'));
CREATE TABLE abr_alias ENGINE = Alias('abr');

SELECT 'alias', count(), max(value) FROM abr_alias WHERE ts >= toDateTime('2026-01-01 00:00:00') SETTINGS enable_analyzer = 1;
SELECT 'alias', count(), max(value) FROM abr_alias WHERE ts >= toDateTime('2025-12-01 00:00:00') SETTINGS enable_analyzer = 1;
SELECT 'alias, old analyzer', count(), max(value) FROM abr_alias WHERE ts >= toDateTime('2026-01-01 00:00:00') SETTINGS enable_analyzer = 0;

-- The catalog now returns the `ABR` table and its alias through `StorageTableProxy`.
DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

SELECT 'lazy abr', count(), max(value) FROM abr WHERE ts >= toDateTime('2026-01-01 00:00:00') SETTINGS enable_analyzer = 1;
SELECT 'lazy abr', count(), max(value) FROM abr WHERE ts >= toDateTime('2025-12-01 00:00:00') SETTINGS enable_analyzer = 1;
SELECT 'lazy abr, old analyzer', count(), max(value) FROM abr WHERE ts >= toDateTime('2026-01-01 00:00:00') SETTINGS enable_analyzer = 0;

DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

SELECT 'lazy alias', count(), max(value) FROM abr_alias WHERE ts >= toDateTime('2026-01-01 00:00:00') SETTINGS enable_analyzer = 1;
SELECT 'lazy alias', count(), max(value) FROM abr_alias WHERE ts >= toDateTime('2025-12-01 00:00:00') SETTINGS enable_analyzer = 1;
SELECT 'lazy alias, old analyzer', count(), max(value) FROM abr_alias WHERE ts >= toDateTime('2026-01-01 00:00:00') SETTINGS enable_analyzer = 0;

USE {CLICKHOUSE_DATABASE:Identifier};
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
