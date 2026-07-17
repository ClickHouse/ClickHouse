-- Tags: no-old-analyzer

-- Port of the tail-predicate sections of DuckDB test/sql/join/iejoin/test_iesemijoin.test and
-- test_ieantijoin.test (previously skipped): SEMI/ANTI with a third equality or arbitrary
-- conjunct, evaluated inside the operator as a residual condition. The plain two-predicate
-- variants live in 04522/04530. `ie_join` is listed first so the equality tails are not
-- claimed as hash join keys.

SET join_algorithm = 'ie_join,hash';

DROP TABLE IF EXISTS left_small;
DROP TABLE IF EXISTS right_small;
DROP TABLE IF EXISTS wide_ranges;
DROP TABLE IF EXISTS narrow_ranges;
DROP TABLE IF EXISTS wide_ranges_anti;
DROP TABLE IF EXISTS narrow_ranges_anti;

CREATE TABLE left_small (id Int32, start Nullable(Date), stop Nullable(Date), symbol String, price Float64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE right_small (id Int32, start Nullable(Date), stop Nullable(Date), symbol String, bid Float64, active Bool) ENGINE = MergeTree ORDER BY id;

INSERT INTO left_small VALUES
    (1, '2026-01-01', '2026-01-02', 'A', 150.00),
    (2, '2026-01-02', '2026-01-03', 'A', 151.00),
    (3, '2026-01-03', '2026-01-04', 'B', 380.00),
    (4, '2026-01-05', NULL, 'C', 410.0),
    (5, NULL, '2026-01-06', 'C', 420.0);

INSERT INTO right_small VALUES
    (1, '2026-01-01', '2026-01-03', 'A', 149.50, true),
    (2, '2026-01-03', '2026-01-04', 'A', 150.50, false),
    (3, '2026-01-04', '2026-01-05', 'B', 379.00, true);

SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT l.id FROM left_small l LEFT SEMI JOIN right_small r ON l.start < r.stop AND r.start < l.stop AND l.symbol = r.symbol) WHERE explain LIKE '%IEJoin%';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT l.id FROM left_small l LEFT SEMI JOIN right_small r ON l.start < r.stop AND r.start < l.stop AND l.symbol = r.symbol) WHERE explain LIKE '%Residual filter%';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT l.id FROM left_small l LEFT ANTI JOIN right_small r ON l.start < r.stop AND r.start < l.stop AND l.price + r.bid > 300) WHERE explain LIKE '%Residual filter%';

SELECT 'semi symbol';
SELECT l.id, l.start, l.stop FROM left_small l LEFT SEMI JOIN right_small r
    ON l.start < r.stop AND r.start < l.stop AND l.symbol = r.symbol ORDER BY 1;
SELECT 'semi price';
SELECT l.id, l.start, l.stop FROM left_small l LEFT SEMI JOIN right_small r
    ON l.start < r.stop AND r.start < l.stop AND l.price + r.bid > 300 ORDER BY 1;
SELECT 'semi all';
SELECT l.id, l.start, l.stop FROM left_small l LEFT SEMI JOIN right_small r
    ON l.start < r.stop AND r.start < l.stop AND l.symbol = r.symbol AND l.price + r.bid > 300 ORDER BY 1;

SELECT 'anti symbol';
SELECT l.id, l.start, l.stop FROM left_small l LEFT ANTI JOIN right_small r
    ON l.start < r.stop AND r.start < l.stop AND l.symbol = r.symbol ORDER BY 1;
SELECT 'anti price';
SELECT l.id, l.start, l.stop FROM left_small l LEFT ANTI JOIN right_small r
    ON l.start < r.stop AND r.start < l.stop AND l.price + r.bid > 300 ORDER BY 1;
SELECT 'anti all';
SELECT l.id, l.start, l.stop FROM left_small l LEFT ANTI JOIN right_small r
    ON l.start < r.stop AND r.start < l.stop AND l.symbol = r.symbol AND l.price + r.bid > 300 ORDER BY 1;

-- Multiple matches: a few wide ranges against many narrow ranges (fixtures as in 04530)
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

SELECT 'wide semi symbol';
SELECT l.id, l.start, l.stop, l.symbol, l.price FROM wide_ranges l LEFT SEMI JOIN narrow_ranges r
    ON l.start < r.stop AND r.start < l.stop AND r.symbol = l.symbol ORDER BY id;
SELECT 'wide semi price';
SELECT l.id, l.start, l.stop, l.symbol, l.price FROM wide_ranges l LEFT SEMI JOIN narrow_ranges r
    ON l.start < r.stop AND r.start < l.stop AND l.price + r.bid < 400 ORDER BY id;
SELECT 'wide semi all';
SELECT l.id, l.start, l.stop, l.symbol, l.price FROM wide_ranges l LEFT SEMI JOIN narrow_ranges r
    ON l.start < r.stop AND r.start < l.stop AND r.symbol = l.symbol AND l.price + r.bid < 375 ORDER BY id;

SELECT 'wide anti symbol';
SELECT l.id, l.start, l.stop, l.symbol, l.price FROM wide_ranges_anti l LEFT ANTI JOIN narrow_ranges_anti r
    ON l.start < r.stop AND r.start < l.stop AND r.symbol = l.symbol ORDER BY id;
SELECT 'wide anti price';
SELECT l.id, l.start, l.stop, l.symbol, l.price FROM wide_ranges_anti l LEFT ANTI JOIN narrow_ranges_anti r
    ON l.start < r.stop AND r.start < l.stop AND l.price + r.bid < 400 ORDER BY id;
SELECT 'wide anti all';
SELECT l.id, l.start, l.stop, l.symbol, l.price FROM wide_ranges_anti l LEFT ANTI JOIN narrow_ranges_anti r
    ON l.start < r.stop AND r.start < l.stop AND r.symbol = l.symbol AND l.price + r.bid < 375 ORDER BY id;

DROP TABLE left_small;
DROP TABLE right_small;
DROP TABLE wide_ranges;
DROP TABLE narrow_ranges;
DROP TABLE wide_ranges_anti;
DROP TABLE narrow_ranges_anti;
