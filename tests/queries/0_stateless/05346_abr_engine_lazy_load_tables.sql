-- Tags: no-replicated-database
-- no-replicated-database: re-attaches an `Atomic` database with `lazy_load_tables`.

SET session_timezone = 'UTC';

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS lazy_load_tables = 1;

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.events_1
(
    ts DateTime,
    value UInt64,
    _sample_interval UInt64 DEFAULT 1
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY ts;

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.events_10
(
    ts DateTime,
    value UInt64,
    _sample_interval UInt64 DEFAULT 10
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY ts;

INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.events_1 (ts, value) VALUES
    ('2026-01-01 00:00:00', 1), ('2026-01-02 00:00:00', 2);
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.events_10 (ts, value) VALUES
    ('2025-12-01 00:00:00', 10), ('2026-01-01 00:00:00', 20);

-- The catalog now returns `StorageTableProxy` objects for the underlying tables.
DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
USE {CLICKHOUSE_DATABASE_1:Identifier};

CREATE TABLE abr ENGINE = ABR('events', [1, 10], ('ts'));

SELECT _sample_interval, sum(value) FROM abr WHERE ts >= toDateTime('2026-01-01 00:00:00') GROUP BY _sample_interval;
SELECT _sample_interval, sum(value) FROM abr WHERE ts >= toDateTime('2025-12-01 00:00:00') GROUP BY _sample_interval;

SET enable_analyzer = 0;

SELECT _sample_interval, sum(value) FROM abr WHERE ts >= toDateTime('2026-01-01 00:00:00') GROUP BY _sample_interval;
SELECT _sample_interval, sum(value) FROM abr WHERE ts >= toDateTime('2025-12-01 00:00:00') GROUP BY _sample_interval;

USE {CLICKHOUSE_DATABASE:Identifier};
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
