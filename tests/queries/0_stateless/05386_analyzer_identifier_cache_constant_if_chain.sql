-- A GROUP BY alias of a nested `if`/`multiIf` with constant conditions that is referenced again (ORDER BY, HAVING,
-- LIMIT BY, GROUPING SETS) must return the same rows with `enable_identifier_resolve_cache` on and off.
-- https://github.com/ClickHouse/ClickHouse/issues/122404

DROP TABLE IF EXISTS u2;
DROP TABLE IF EXISTS u3;
DROP TABLE IF EXISTS t;

CREATE TABLE u2 (p String, a1 LowCardinality(String), a2 String, a8 String) ENGINE = MergeTree ORDER BY p;
CREATE TABLE u3 (p String, g1 LowCardinality(String), g2 String) ENGINE = MergeTree ORDER BY p;
INSERT INTO u2 SELECT toString(number % 20), 'v' || toString(number % 7), 'a', 'f' FROM numbers(100);
INSERT INTO u3 SELECT toString(number), 'm', 'n' FROM numbers(20);

-- The query from the issue.
SELECT multiIf(('x2' IN ('x1')), q2.g1, multiIf(('x2' IN ('x2')), q1.a1, multiIf(('x2' IN ('x3')), q1.a2, multiIf(('x2' IN ('x4')), q1.a8, multiIf(('x2' IN ('x5')), q2.g2, q1.a1))))) AS r1
FROM u2 AS q1 INNER JOIN u3 AS q2 ON q1.p = q2.p GROUP BY r1 ORDER BY r1 ASC
SETTINGS enable_identifier_resolve_cache = 1;

SELECT multiIf(('x2' IN ('x1')), q2.g1, multiIf(('x2' IN ('x2')), q1.a1, multiIf(('x2' IN ('x3')), q1.a2, multiIf(('x2' IN ('x4')), q1.a8, multiIf(('x2' IN ('x5')), q2.g2, q1.a1))))) AS r1
FROM u2 AS q1 INNER JOIN u3 AS q2 ON q1.p = q2.p GROUP BY r1 ORDER BY r1 ASC
SETTINGS enable_identifier_resolve_cache = 0;

CREATE TABLE t (s String) ENGINE = MergeTree ORDER BY s;
INSERT INTO t SELECT toString(number % 3) FROM numbers(9);

SELECT if(0, s, if(0, s, if(0, s, if(0, s, s)))) AS r FROM t GROUP BY r ORDER BY r SETTINGS enable_identifier_resolve_cache = 1;
SELECT if(0, s, if(0, s, if(0, s, if(0, s, s)))) AS r FROM t GROUP BY r HAVING r != '1' ORDER BY r SETTINGS enable_identifier_resolve_cache = 1;
SELECT if(0, s, if(0, s, if(0, s, if(0, s, s)))) AS r FROM t GROUP BY r ORDER BY r LIMIT 1 BY r SETTINGS enable_identifier_resolve_cache = 1;
SELECT if(0, s, if(0, s, if(0, s, if(0, s, s)))) AS r, count() FROM t GROUP BY GROUPING SETS ((r), ()) ORDER BY r SETTINGS enable_identifier_resolve_cache = 1;
SELECT r FROM t GROUP BY if(0, s, if(0, s, if(0, s, if(0, s, s)))) AS r HAVING r != '' ORDER BY r SETTINGS enable_identifier_resolve_cache = 1;
SELECT if(0, s, if(0, s, if(0, s, if(0, s, s)))) AS r, r || 'x' AS q FROM t GROUP BY r ORDER BY r SETTINGS enable_identifier_resolve_cache = 1;

-- The query tree is the same with and without the cache.
SELECT
    (SELECT countIf(explain ILIKE '%function_name: if,%' OR explain ILIKE '%function_name: multiIf,%') FROM (EXPLAIN QUERY TREE SELECT if(0, s, if(0, s, if(0, s, if(0, s, s)))) AS r FROM t GROUP BY r ORDER BY r SETTINGS enable_identifier_resolve_cache = 1)),
    (SELECT countIf(explain ILIKE '%function_name: if,%' OR explain ILIKE '%function_name: multiIf,%') FROM (EXPLAIN QUERY TREE SELECT if(0, s, if(0, s, if(0, s, if(0, s, s)))) AS r FROM t GROUP BY r ORDER BY r SETTINGS enable_identifier_resolve_cache = 0));

-- A chain of constant conditions is folded completely.
SELECT countIf(explain ILIKE '%function_name: if,%' OR explain ILIKE '%function_name: multiIf,%') FROM (EXPLAIN QUERY TREE SELECT if(0, number, if(0, number, if(0, number, if(0, number, number)))) FROM numbers(1));

DROP TABLE t;
DROP TABLE u3;
DROP TABLE u2;
