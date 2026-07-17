-- Tags: no-old-analyzer

-- SEMI/ANTI with a few wide ranges against many narrow ranges, so that every matched left row
-- has thousands of matching right rows: SEMI must emit each matched row once; ANTI must emit
-- the rows before the narrow window plus the NULL-keyed rows. Shapes with a third (tail)
-- predicate are covered by 04522 and 04554 (it becomes a residual condition).

SET join_algorithm = 'direct,parallel_hash,hash,ie_join';

DROP TABLE IF EXISTS wide_ranges;
DROP TABLE IF EXISTS narrow_ranges;
DROP TABLE IF EXISTS wide_ranges_anti;
DROP TABLE IF EXISTS narrow_ranges_anti;

CREATE TABLE wide_ranges ENGINE = MergeTree ORDER BY id AS
SELECT number AS id,
       toDateTime('2026-01-01 00:00:00', 'UTC') + toIntervalSecond(intDiv(number * 5 * 2048, 7)) AS start,
       toDateTime('2026-01-01 00:00:00', 'UTC') + toIntervalSecond(intDiv((number + 1) * 5 * 2048, 7)) AS stop,
       char(65 + number % 26) AS symbol,
       149.5 + number * 5 * 2048 / 700.0 AS price
FROM numbers(6);

CREATE TABLE narrow_ranges ENGINE = MergeTree ORDER BY id AS
SELECT number AS id,
       toDateTime('2026-01-01 00:00:00', 'UTC') + toIntervalSecond(number) AS start,
       toDateTime('2026-01-01 00:00:00', 'UTC') + toIntervalSecond(number + 1) AS stop,
       char(65 + number % 26) AS symbol,
       150.0 + number / 100.0 AS bid,
       number % 2 = 1 AS active
FROM numbers(2048 * 5);

-- The ANTI fixture shifts the narrow ranges past the first three wide ranges and NULLs one key
-- of the last two wide ranges
CREATE TABLE wide_ranges_anti ENGINE = MergeTree ORDER BY id AS
SELECT number AS id,
       if(number = 6, NULL, toDateTime('2026-01-01 00:00:00', 'UTC') + toIntervalSecond(intDiv(number * 5 * 2048, 7))) AS start,
       if(number = 7, NULL, toDateTime('2026-01-01 00:00:00', 'UTC') + toIntervalSecond(intDiv((number + 1) * 5 * 2048, 7))) AS stop,
       char(65 + number % 26) AS symbol,
       149.5 + number * 5 * 2048 / 700.0 AS price
FROM numbers(8);

CREATE TABLE narrow_ranges_anti ENGINE = MergeTree ORDER BY id AS
SELECT number AS id,
       toDateTime('2026-01-01 00:00:00', 'UTC') + toIntervalSecond(number + intDiv(15 * 2048, 7)) AS start,
       toDateTime('2026-01-01 00:00:00', 'UTC') + toIntervalSecond(number + intDiv(15 * 2048, 7) + 1) AS stop,
       char(65 + number % 26) AS symbol,
       150.0 + number / 100.0 AS bid,
       number % 2 = 1 AS active
FROM numbers(2048 * 5);

SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT l.id FROM wide_ranges l LEFT SEMI JOIN narrow_ranges r ON l.start < r.stop AND r.start < l.stop) WHERE explain LIKE '%IEJoin%';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT l.id FROM wide_ranges_anti l LEFT ANTI JOIN narrow_ranges_anti r ON l.start < r.stop AND r.start < l.stop) WHERE explain LIKE '%IEJoin%';

SELECT 'semi';
SELECT l.id, l.start, l.stop, l.symbol, l.price
FROM wide_ranges l LEFT SEMI JOIN narrow_ranges r ON l.start < r.stop AND r.start < l.stop
ORDER BY l.id;

SELECT 'anti';
SELECT l.id, l.start, l.stop, l.symbol, l.price
FROM wide_ranges_anti l LEFT ANTI JOIN narrow_ranges_anti r ON l.start < r.stop AND r.start < l.stop
ORDER BY l.id;

DROP TABLE wide_ranges;
DROP TABLE narrow_ranges;
DROP TABLE wide_ranges_anti;
DROP TABLE narrow_ranges_anti;
