-- TOLERANCE bounds how far back an ASOF JOIN may reach for a match.

DROP TABLE IF EXISTS trades;
DROP TABLE IF EXISTS quotes;

CREATE TABLE quotes (sym UInt32, t DateTime64(3), bid Float64) ENGINE = MergeTree ORDER BY (sym, t);
INSERT INTO quotes VALUES (1, toDateTime64('2024-01-01 09:59:58.000', 3), 9.5);
INSERT INTO quotes VALUES (2, toDateTime64('2024-01-01 09:00:00.000', 3), 8.5);

CREATE TABLE trades (sym UInt32, t DateTime64(3)) ENGINE = MergeTree ORDER BY (sym, t);
INSERT INTO trades VALUES (1, toDateTime64('2024-01-01 10:00:00.000', 3));
INSERT INTO trades VALUES (2, toDateTime64('2024-01-01 10:00:00.000', 3));

SELECT '-- sym 1 is 2s stale and matches, sym 2 is an hour stale and does not';
SELECT tr.sym, q.bid
FROM trades AS tr ASOF LEFT JOIN quotes AS q
ON tr.sym = q.sym AND tr.t >= q.t TOLERANCE INTERVAL 5 SECOND
ORDER BY tr.sym
SETTINGS join_use_nulls = 1;

SELECT '-- without TOLERANCE both match, including the stale one';
SELECT tr.sym, q.bid
FROM trades AS tr ASOF LEFT JOIN quotes AS q
ON tr.sym = q.sym AND tr.t >= q.t
ORDER BY tr.sym
SETTINGS join_use_nulls = 1;

SELECT '-- INNER drops the out-of-tolerance row instead of nulling it';
SELECT tr.sym, q.bid
FROM trades AS tr ASOF JOIN quotes AS q
ON tr.sym = q.sym AND tr.t >= q.t TOLERANCE INTERVAL 5 SECOND
ORDER BY tr.sym;

SELECT '-- the bound is inclusive';
SELECT count()
FROM (SELECT 1 AS sym, toDateTime64('2024-01-01 10:00:00.000', 3) AS t) AS tr
ASOF JOIN (SELECT 1 AS sym, toDateTime64('2024-01-01 09:59:55.000', 3) AS t) AS q
ON tr.sym = q.sym AND tr.t >= q.t TOLERANCE INTERVAL 5 SECOND;

SELECT '-- one millisecond past the bound is not a match';
SELECT count()
FROM (SELECT 1 AS sym, toDateTime64('2024-01-01 10:00:00.000', 3) AS t) AS tr
ASOF JOIN (SELECT 1 AS sym, toDateTime64('2024-01-01 09:59:54.999', 3) AS t) AS q
ON tr.sym = q.sym AND tr.t >= q.t TOLERANCE INTERVAL 5 SECOND;

SELECT '-- a bare number is in the units of the ASOF key, here milliseconds';
SELECT count()
FROM (SELECT 1 AS sym, toDateTime64('2024-01-01 10:00:00.000', 3) AS t) AS tr
ASOF JOIN (SELECT 1 AS sym, toDateTime64('2024-01-01 09:59:58.000', 3) AS t) AS q
ON tr.sym = q.sym AND tr.t >= q.t TOLERANCE 5000;

SELECT '-- rejections';
-- TOLERANCE only applies to ASOF
SELECT * FROM (SELECT 1 AS k) AS l JOIN (SELECT 1 AS k) AS r ON l.k = r.k TOLERANCE 5; -- { serverError SYNTAX_ERROR }
-- months are not a fixed length of time
SELECT count() FROM trades AS tr ASOF JOIN quotes AS q ON tr.sym = q.sym AND tr.t >= q.t TOLERANCE INTERVAL 1 MONTH; -- { serverError INVALID_JOIN_ON_EXPRESSION }
-- an interval means nothing against a non-temporal key
SELECT count() FROM (SELECT 1 AS s, 100 AS t) AS l ASOF JOIN (SELECT 1 AS s, 98 AS t) AS r ON l.s = r.s AND l.t >= r.t TOLERANCE INTERVAL 5 SECOND; -- { serverError INVALID_JOIN_ON_EXPRESSION }
-- finer than the key's resolution cannot be represented
SELECT count() FROM trades AS tr ASOF JOIN quotes AS q ON tr.sym = q.sym AND tr.t >= q.t TOLERANCE INTERVAL 1 MICROSECOND; -- { serverError INVALID_JOIN_ON_EXPRESSION }
-- a negative bound can never be satisfied
SELECT count() FROM trades AS tr ASOF JOIN quotes AS q ON tr.sym = q.sym AND tr.t >= q.t TOLERANCE -5; -- { serverError INVALID_JOIN_ON_EXPRESSION }

DROP TABLE trades;
DROP TABLE quotes;
