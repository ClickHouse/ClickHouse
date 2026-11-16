-- Tags: no-replicated-database
-- Tag no-replicated-database: a `Replicated` database only allows `DETACH TABLE ... PERMANENTLY`.

-- The `ABR` table resolves its source tables for each query instead of keeping them alive, so they can be
-- detached and dropped with `SYNC`, which waits until nothing holds the table anymore.

SET allow_experimental_abr = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS abr;
DROP TABLE IF EXISTS src_1;
DROP TABLE IF EXISTS src_10;

CREATE TABLE src_1 (ts DateTime, value UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY ts;
CREATE TABLE src_10 (ts DateTime, value UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY ts;
INSERT INTO src_1 SELECT toDateTime('2026-01-10 00:00:00') + INTERVAL number DAY, 1 FROM numbers(10);
INSERT INTO src_10 SELECT toDateTime('2026-01-01 00:00:00') + INTERVAL number DAY, 10 FROM numbers(20);

CREATE TABLE abr ENGINE = ABR('src', [1, 10], ('ts'));
SELECT max(value) FROM abr WHERE ts >= toDateTime('2026-01-12 00:00:00');

-- A source table can be detached while the `ABR` table exists, and is used again once attached.
DETACH TABLE src_10 SYNC;
SELECT max(value) FROM abr WHERE ts >= toDateTime('2026-01-12 00:00:00'); -- { serverError UNKNOWN_TABLE }
ATTACH TABLE src_10;
SELECT max(value) FROM abr WHERE ts >= toDateTime('2026-01-12 00:00:00');

-- A source table can be dropped while the `ABR` table exists, once the dependency check is disabled.
SET check_table_dependencies = 0;
DROP TABLE src_1 SYNC;
SELECT max(value) FROM abr WHERE ts >= toDateTime('2026-01-12 00:00:00'); -- { serverError UNKNOWN_TABLE }

-- A source table created again under the same name is used from the next query on.
CREATE TABLE src_1 (ts DateTime, value UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY ts;
INSERT INTO src_1 SELECT toDateTime('2026-01-10 00:00:00') + INTERVAL number DAY, 2 FROM numbers(10);
SELECT max(value) FROM abr WHERE ts >= toDateTime('2026-01-12 00:00:00');

DROP TABLE abr;
DROP TABLE src_1;
DROP TABLE src_10;
