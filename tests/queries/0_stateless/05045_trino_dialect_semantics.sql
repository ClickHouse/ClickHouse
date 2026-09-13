-- The Trino semantics that are not expressible in the translated SQL alone:
-- the settings that back them must survive an explicit `SETTINGS` clause and a
-- wrapping statement, joined `UNNEST` must pad with NULLs, and the aggregates
-- must return NULL (not the type default) over an empty input.

SET allow_experimental_trino_dialect = 1;
SET dialect = 'trino';

SELECT '-- the semantic settings survive an explicit SETTINGS clause';
SELECT l.x, r.y
FROM (VALUES 1, 2) AS l(x)
LEFT JOIN (VALUES (1, 10)) AS r(x, y) ON l.x = r.x
ORDER BY l.x
SETTINGS max_threads = 1;

SELECT '-- ... and a set operation keeps the numeric supertype, not Variant';
SELECT toTypeName(x) FROM (SELECT CAST(1 AS INTEGER) AS x UNION ALL SELECT 2.5E0 AS x) AS t LIMIT 1 SETTINGS max_threads = 1;

SELECT '-- ... and hold for a wrapping INSERT ... SELECT';
CREATE TEMPORARY TABLE t_trino_wrap (y Nullable(BIGINT));
INSERT INTO t_trino_wrap SELECT r.y FROM (VALUES 1) AS l(x) LEFT JOIN (VALUES (2, 10)) AS r(x, y) ON l.x = r.x;
SELECT * FROM t_trino_wrap;

SELECT '-- LEFT JOIN UNNEST over an empty array yields NULL, not the default';
SELECT s.id, u.x
FROM (VALUES (1, ARRAY[10, 20]), (2, CAST(ARRAY[] AS ARRAY(INTEGER)))) AS s(id, a)
LEFT JOIN UNNEST(s.a) AS u(x) ON TRUE
ORDER BY s.id, u.x;

SELECT '-- CROSS JOIN UNNEST of arrays of different lengths pads with NULLs';
SELECT t.x, t.y
FROM (VALUES 1) AS s(d)
CROSS JOIN UNNEST(ARRAY[1, 2, 3], ARRAY['x', 'y']) AS t(x, y)
ORDER BY t.x;

SELECT '-- the column aliases of a joined UNNEST are reachable through its table alias';
SELECT t.x, t.n
FROM (VALUES 1) AS s(d)
CROSS JOIN UNNEST(ARRAY['a', 'b']) WITH ORDINALITY AS t(x, n)
ORDER BY t.n;

SELECT '-- array_agg keeps NULL elements';
SELECT array_agg(x) FROM (VALUES 1, NULL, 2) AS t(x);
SELECT array_agg(DISTINCT x) FROM (VALUES 1, NULL, 1) AS t(x);

SELECT '-- the aggregates return NULL over an empty input, also with DISTINCT';
SELECT sum(DISTINCT x), avg(DISTINCT x), min(DISTINCT x), max(DISTINCT x)
FROM (VALUES 1, 2) AS t(x)
WHERE x > 100;

SELECT '-- ... and over an empty window frame';
SELECT sum(x) OVER (ORDER BY x ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS s
FROM (VALUES 1, 2) AS t(x)
ORDER BY x;

SELECT '-- ... and for an EXPLAIN wrapper';
SELECT count() > 0 FROM (
    EXPLAIN QUERY TREE
    SELECT r.y FROM (VALUES 1) AS l(x) LEFT JOIN (VALUES (2, 10)) AS r(x, y) ON l.x = r.x
) WHERE explain LIKE '%Nullable%';

SELECT '-- LEFT JOIN UNNEST of an element type that cannot be Nullable is rejected';
SELECT u.x
FROM (VALUES 1) AS s(d)
LEFT JOIN UNNEST(ARRAY[ARRAY[1, 2]]) AS u(x) ON TRUE; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
