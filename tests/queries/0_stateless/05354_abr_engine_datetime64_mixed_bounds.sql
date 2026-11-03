-- A query on a `DateTime64` time column can bound it with a `DateTime` or an integer, which `KeyCondition` leaves
-- as a whole number of seconds rather than a `DateTime64`. Such bounds must be routed like `DateTime64` ones,
-- inclusive and exclusive.

SET allow_experimental_abr = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS abr;
DROP TABLE IF EXISTS src_1;
DROP TABLE IF EXISTS src_10;

CREATE TABLE src_1 (ts DateTime64(3), value UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY ts;
CREATE TABLE src_10 (ts DateTime64(3), value UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY ts;

-- The first table covers the data since 2026-01-10, the second one since 2026-01-01.
INSERT INTO src_1 SELECT toDateTime64('2026-01-10 00:00:00', 3) + INTERVAL number DAY, 1 FROM numbers(10);
INSERT INTO src_10 SELECT toDateTime64('2026-01-01 00:00:00', 3) + INTERVAL number DAY, 10 FROM numbers(20);

CREATE TABLE abr ENGINE = ABR('src', [1, 10], ('ts'));

-- `max(value)` tells the table the query was routed to.
SELECT 'DateTime, inclusive';
SELECT max(value) FROM abr WHERE ts >= toDateTime('2026-01-12 00:00:00');
SELECT max(value) FROM abr WHERE ts >= toDateTime('2026-01-05 00:00:00');

SELECT 'DateTime, exclusive';
SELECT max(value) FROM abr WHERE ts > toDateTime('2026-01-12 00:00:00');
SELECT max(value) FROM abr WHERE ts > toDateTime('2026-01-05 00:00:00');

SELECT 'UInt64, inclusive';
SELECT max(value) FROM abr WHERE ts >= 1768176000; -- 2026-01-12 00:00:00
SELECT max(value) FROM abr WHERE ts >= 1767571200; -- 2026-01-05 00:00:00

SELECT 'Int64, exclusive';
SELECT max(value) FROM abr WHERE ts > toInt64(1768176000);
SELECT max(value) FROM abr WHERE ts > toInt64(1767571200);

-- The first row of the first table is at exactly 2026-01-10 00:00:00.000. An exclusive bound at that second
-- starts one tick later, which the first table still covers, and excludes that row.
SELECT 'Boundary of the first table';
SELECT max(value), count() FROM abr WHERE ts >= toDateTime('2026-01-10 00:00:00');
SELECT max(value), count() FROM abr WHERE ts > toDateTime('2026-01-10 00:00:00');

DROP TABLE abr;
DROP TABLE src_1;
DROP TABLE src_10;
