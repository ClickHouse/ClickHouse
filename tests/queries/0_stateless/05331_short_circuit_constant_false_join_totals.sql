-- Tags: shard
-- A join input skipped by the constant-false ON short-circuit is kept when it can produce totals.

SET query_plan_short_circuit_constant_false_join = 1;

SELECT 'LEFT JOIN, right side WITH TOTALS';
SELECT * FROM (SELECT number % 2 AS k FROM numbers(4)) AS l
LEFT JOIN (SELECT number % 3 AS k, count() AS c FROM numbers(6) GROUP BY k WITH TOTALS) AS r ON l.k = r.k AND 1 = 2
ORDER BY ALL;

SELECT 'LEFT JOIN, both sides WITH TOTALS';
SELECT * FROM (SELECT number % 2 AS k, sum(number) AS s FROM numbers(10) GROUP BY k WITH TOTALS) AS l
LEFT JOIN (SELECT number % 3 AS k, count() AS c FROM numbers(6) GROUP BY k WITH TOTALS) AS r ON l.k = r.k AND 1 = 2
ORDER BY ALL;

SELECT 'RIGHT JOIN, left side WITH TOTALS';
SELECT * FROM (SELECT number % 3 AS k, count() AS c FROM numbers(6) GROUP BY k WITH TOTALS) AS l
RIGHT JOIN (SELECT number % 2 AS k FROM numbers(4)) AS r ON l.k = r.k AND 1 = 2
ORDER BY ALL;

SELECT 'INNER JOIN: only the input without totals becomes an empty source';
SELECT count() FROM (
    EXPLAIN SELECT * FROM (SELECT number % 2 AS k FROM numbers(4)) AS l
    INNER JOIN (SELECT number % 3 AS k, count() AS c FROM numbers(6) GROUP BY k WITH TOTALS) AS r ON l.k = r.k AND 1 = 2
) WHERE explain ILIKE '%ReadNothing%';

SELECT 'an aggregation over a WITH TOTALS subquery has no totals, so it is still an empty source';
SELECT count() FROM (
    EXPLAIN SELECT * FROM (SELECT number % 2 AS k FROM numbers(4)) AS l
    LEFT JOIN (SELECT count() AS n FROM (SELECT number % 3 AS k, count() AS c FROM numbers(6) GROUP BY k WITH TOTALS)) AS r
    ON l.k = r.n AND 1 = 2
) WHERE explain ILIKE '%ReadNothing%';

SELECT 'totals from a remote server';
SELECT * FROM (SELECT number % 2 AS k FROM numbers(4)) AS l
LEFT JOIN (SELECT number % 3 AS k, count() AS c FROM remote('127.0.0.2', numbers(6)) GROUP BY k WITH TOTALS) AS r ON l.k = r.k AND 1 = 2
ORDER BY ALL;

SELECT 'a remote input without totals is still an empty source';
SELECT count() FROM (
    EXPLAIN SELECT * FROM (SELECT number % 2 AS k FROM numbers(4)) AS l
    LEFT JOIN (SELECT number % 3 AS k FROM remote('127.0.0.2', numbers(6))) AS r ON l.k = r.k AND 1 = 2
) WHERE explain ILIKE '%ReadNothing%';

SELECT 'totals from a view with SQL SECURITY DEFINER';
DROP VIEW IF EXISTS v_05331;
CREATE VIEW v_05331 DEFINER = CURRENT_USER SQL SECURITY DEFINER AS
    SELECT number % 3 AS k, count() AS c FROM numbers(6) GROUP BY k WITH TOTALS;
SELECT * FROM (SELECT number % 2 AS k FROM numbers(4)) AS l
LEFT JOIN v_05331 AS r ON l.k = r.k AND 1 = 2
ORDER BY ALL;

SELECT 'totals through a Merge table';
SELECT * FROM (SELECT number % 2 AS k FROM numbers(4)) AS l
LEFT JOIN merge(currentDatabase(), '^v_05331$') AS r ON l.k = r.k AND 1 = 2
ORDER BY ALL;
DROP VIEW v_05331;

SELECT 'totals from the query result cache';
-- The first run writes the subquery result to the cache, the second one reads it from there.
SELECT * FROM (SELECT number % 2 AS k FROM numbers(4)) AS l
LEFT JOIN (SELECT number % 3 AS k, count() AS c FROM numbers(6) GROUP BY k WITH TOTALS SETTINGS use_query_cache = 1) AS r ON l.k = r.k AND 1 = 2
ORDER BY ALL SETTINGS log_comment = '05331_cache_1';
SELECT * FROM (SELECT number % 2 AS k FROM numbers(4)) AS l
LEFT JOIN (SELECT number % 3 AS k, count() AS c FROM numbers(6) GROUP BY k WITH TOTALS SETTINGS use_query_cache = 1) AS r ON l.k = r.k AND 1 = 2
ORDER BY ALL SETTINGS log_comment = '05331_cache_2';
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['QueryCacheHits'] > 0 FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05331_cache_2';
