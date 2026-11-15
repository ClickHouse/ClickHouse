-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/105462
-- Server segfaults on `uniqStateOrNull(Nullable) ... GROUP BY ... WITH ROLLUP`
-- (and the CUBE / TOTALS / OrDefault variants).
-- The synthesised "all-grouped-up" row holds an `-OrNull`/`-OrDefault`-wrapped
-- state whose inner `UniquesHashSet` was never properly constructed, and the
-- subsequent `write()` of that empty hash set dereferenced a null `buf`.

DROP TABLE IF EXISTS t_105462;

CREATE TABLE t_105462 (h Nullable(UInt16)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_105462 SELECT if(number % 5 = 0, NULL, toUInt16(number)) FROM numbers(20);

-- Each query must complete without crashing the server. We do not assert
-- specific lengths because they encode internal `UniquesHashSet` layout.

SELECT 'uniqStateOrNull WITH ROLLUP', count() FROM (
    SELECT hex(uniqStateOrNull(h)) AS s FROM t_105462 GROUP BY h WITH ROLLUP);

SELECT 'uniqStateOrNull WITH CUBE', count() FROM (
    SELECT hex(uniqStateOrNull(h)) AS s FROM t_105462 GROUP BY h WITH CUBE);

SELECT 'uniqStateOrNull WITH TOTALS', count() FROM (
    SELECT hex(uniqStateOrNull(h)) AS s FROM t_105462 GROUP BY h WITH TOTALS);

SELECT 'uniqStateOrDefault WITH ROLLUP', count() FROM (
    SELECT hex(uniqStateOrDefault(h)) AS s FROM t_105462 GROUP BY h WITH ROLLUP);

SELECT 'uniqOrNullState WITH ROLLUP', count() FROM (
    SELECT hex(uniqOrNullState(h)) AS s FROM t_105462 GROUP BY h WITH ROLLUP);

-- Sanity check: the same query without the rollup modifier still works.
SELECT 'plain GROUP BY', count() FROM (
    SELECT hex(uniqStateOrNull(h)) AS s FROM t_105462 GROUP BY h);

DROP TABLE t_105462;
