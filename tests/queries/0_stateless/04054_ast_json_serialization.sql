-- Test basic round-trip: parseQueryToJSON -> formatQueryFromJSON should preserve query semantics

-- Simple SELECT
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT 1'));

-- SELECT with columns
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT a, b, c FROM t'));

-- SELECT with WHERE
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT x FROM t WHERE x > 1'));

-- SELECT with alias
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT x AS y FROM t'));

-- SELECT with function
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT count(*), sum(x) FROM t'));

-- SELECT with JOIN
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT a FROM t1 INNER JOIN t2 ON t1.id = t2.id'));

-- SELECT with ORDER BY
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT a FROM t ORDER BY a DESC'));

-- SELECT with LIMIT
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT a FROM t LIMIT 10 OFFSET 5'));

-- SELECT with GROUP BY and HAVING
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT a, count() FROM t GROUP BY a HAVING count() > 1'));

-- SELECT with subquery
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT * FROM (SELECT 1 AS x)'));

-- SELECT with DISTINCT
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT DISTINCT a FROM t'));

-- SELECT with UNION ALL
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT 1 UNION ALL SELECT 2'));

-- Asterisk
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT * FROM t'));

-- Literal types
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT 42, -1, 3.14, \'hello\', NULL'));

-- Array and tuple literals
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT [1, 2, 3], (1, \'a\')'));

-- Nested functions
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT if(x > 0, x, -x) FROM t'));

-- ARRAY JOIN
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT x FROM t ARRAY JOIN arr AS x'));

-- LEFT ARRAY JOIN
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT x FROM t LEFT ARRAY JOIN arr AS x'));

-- Window function
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT row_number() OVER (PARTITION BY a ORDER BY b) FROM t'));

-- GROUP BY WITH TOTALS
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT a, count() FROM t GROUP BY a WITH TOTALS'));

-- DDL round-trips
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (a UInt64, b String) ENGINE = MergeTree ORDER BY a'));
SELECT formatQueryFromJSON(parseQueryToJSON('DROP TABLE IF EXISTS t'));
SELECT formatQueryFromJSON(parseQueryToJSON('INSERT INTO t SELECT 1, 2'));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t ADD COLUMN c Float64'));
SELECT formatQueryFromJSON(parseQueryToJSON('EXPLAIN SELECT 1'));

-- Test that the JSON output is valid JSON (should not throw)
SELECT length(parseQueryToJSON('SELECT 1')) > 0;

-- Test round-trip idempotence: applying the round-trip twice gives the same result
SELECT formatQueryFromJSON(parseQueryToJSON(formatQueryFromJSON(parseQueryToJSON('SELECT a, b FROM t WHERE x > 1 ORDER BY a')))) = formatQueryFromJSON(parseQueryToJSON('SELECT a, b FROM t WHERE x > 1 ORDER BY a'));
