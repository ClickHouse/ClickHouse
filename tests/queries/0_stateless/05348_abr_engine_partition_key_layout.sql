-- The positions of the retention columns in the part minmax index are resolved for each source table,
-- so the partition keys of the source tables may differ, as long as each one contains the retention columns.

SET allow_experimental_abr = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS abr;
DROP TABLE IF EXISTS layout_mismatch_1;
DROP TABLE IF EXISTS layout_mismatch_10;
DROP TABLE IF EXISTS layout_not_direct_1;
DROP TABLE IF EXISTS layout_not_direct_10;
DROP TABLE IF EXISTS layout_time_only_1;
DROP TABLE IF EXISTS layout_time_only_10;
DROP TABLE IF EXISTS layout_with_region_1;
DROP TABLE IF EXISTS layout_with_region_10;

-- Partition keys over different columns: the minmax index is [ts] for one table and [region, ts] for the other.
CREATE TABLE layout_mismatch_1 (ts DateTime, region UInt64, value UInt64) ENGINE = MergeTree PARTITION BY toDate(ts) ORDER BY ts;
CREATE TABLE layout_mismatch_10 (ts DateTime, region UInt64, value UInt64) ENGINE = MergeTree PARTITION BY (toDate(ts), region) ORDER BY ts;
INSERT INTO layout_mismatch_1 SELECT toDateTime('2026-01-10 00:00:00') + INTERVAL number DAY, 1, 1 FROM numbers(11);
INSERT INTO layout_mismatch_10 SELECT toDateTime('2026-01-01 00:00:00') + INTERVAL number DAY, 1, 10 FROM numbers(20);

CREATE TABLE abr ENGINE = ABR('layout_mismatch', [1, 10], ('ts'));
-- Covered by the first table.
SELECT max(value) FROM abr WHERE ts >= toDateTime('2026-01-12 00:00:00');
-- Only covered by the second table.
SELECT max(value) FROM abr WHERE ts >= toDateTime('2026-01-05 00:00:00');
DROP TABLE abr;

-- A retention column must be part of the partition key of every source table.
CREATE TABLE abr ENGINE = ABR('layout_mismatch', [1, 10], ('ts', 'region')); -- { serverError BAD_ARGUMENTS }

-- The same columns, but a retention column is not a direct column of the partition key of the second table.
CREATE TABLE layout_not_direct_1 (ts DateTime, region UInt64, value UInt64) ENGINE = MergeTree PARTITION BY (toDate(ts), region) ORDER BY ts;
CREATE TABLE layout_not_direct_10 (ts DateTime, region UInt64, value UInt64) ENGINE = MergeTree PARTITION BY (toDate(ts), region % 4) ORDER BY ts;
CREATE TABLE abr ENGINE = ABR('layout_not_direct', [1, 10], ('ts', 'region')); -- { serverError BAD_ARGUMENTS }

-- Different expressions over the same time column are allowed, e.g. coarser partitions for the coarser table.
CREATE TABLE layout_time_only_1 (ts DateTime, value UInt64) ENGINE = MergeTree PARTITION BY toYYYYMMDD(ts) ORDER BY ts;
CREATE TABLE layout_time_only_10 (ts DateTime, value UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY ts;
INSERT INTO layout_time_only_1 SELECT toDateTime('2026-01-10 00:00:00') + INTERVAL number DAY, 1 FROM numbers(11);
INSERT INTO layout_time_only_10 SELECT toDateTime('2026-01-01 00:00:00') + INTERVAL number DAY, 10 FROM numbers(20);

CREATE TABLE abr ENGINE = ABR('layout_time_only', [1, 10], ('ts'));
-- Covered by the first table.
SELECT max(value) FROM abr WHERE ts >= toDateTime('2026-01-12 00:00:00');
-- Only covered by the second table.
SELECT max(value) FROM abr WHERE ts >= toDateTime('2026-01-05 00:00:00');
DROP TABLE abr;

-- Different expressions and a different order over the same columns give the same minmax index [region, ts].
CREATE TABLE layout_with_region_1 (ts DateTime, region UInt64, value UInt64) ENGINE = MergeTree PARTITION BY (toYYYYMMDD(ts), region) ORDER BY ts;
CREATE TABLE layout_with_region_10 (ts DateTime, region UInt64, value UInt64) ENGINE = MergeTree PARTITION BY (region, toYYYYMM(ts)) ORDER BY ts;
INSERT INTO layout_with_region_1 SELECT toDateTime('2026-01-10 00:00:00') + INTERVAL number DAY, 1, 1 FROM numbers(11);
INSERT INTO layout_with_region_10 SELECT toDateTime('2026-01-01 00:00:00') + INTERVAL number DAY, 1, 10 FROM numbers(20);

CREATE TABLE abr ENGINE = ABR('layout_with_region', [1, 10], ('ts', 'region'));
-- Covered by the first table.
SELECT max(value) FROM abr WHERE region = 1 AND ts >= toDateTime('2026-01-12 00:00:00');
-- Only covered by the second table.
SELECT max(value) FROM abr WHERE region = 1 AND ts >= toDateTime('2026-01-05 00:00:00');
DROP TABLE abr;

DROP TABLE layout_mismatch_1;
DROP TABLE layout_mismatch_10;
DROP TABLE layout_not_direct_1;
DROP TABLE layout_not_direct_10;
DROP TABLE layout_time_only_1;
DROP TABLE layout_time_only_10;
DROP TABLE layout_with_region_1;
DROP TABLE layout_with_region_10;
