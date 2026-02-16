-- Test that AST formatting is consistent for a wide variety of SQL constructs.
-- In debug builds, the server verifies that format(parse(query)) == format(parse(format(parse(query)))),
-- and aborts on inconsistency. This test simply runs many SQL constructs to trigger that check.

-- Basic SELECT
SELECT 1;
SELECT 1 AS x;
SELECT *, 1 FROM system.one;
SELECT DISTINCT 1;
SELECT 1 WHERE 1;
SELECT 1 HAVING 1;
SELECT 1 LIMIT 1;
SELECT 1 LIMIT 1 OFFSET 0;
SELECT 1 SETTINGS max_threads = 1;

-- Expressions
SELECT 1 + 2;
SELECT 1 - 2;
SELECT 2 * 3;
SELECT 6 / 2;
SELECT 7 % 3;
SELECT 1 = 1;
SELECT 1 != 2;
SELECT 1 < 2;
SELECT 1 > 0;
SELECT 1 <= 1;
SELECT 1 >= 1;
SELECT 1 AND 1;
SELECT 1 OR 0;
SELECT NOT 0;
SELECT -1;

-- IS NULL / IS NOT NULL
SELECT NULL IS NULL;
SELECT 1 IS NOT NULL;

-- BETWEEN
SELECT 5 BETWEEN 1 AND 10;
SELECT 5 NOT BETWEEN 100 AND 200;

-- LIKE / ILIKE
SELECT 'hello' LIKE '%ello';
SELECT 'hello' NOT LIKE 'world%';
SELECT 'hello' ILIKE '%ELLO';
SELECT 'hello' NOT ILIKE 'WORLD%';

-- CASE
SELECT CASE WHEN 1 THEN 'a' WHEN 0 THEN 'b' ELSE 'c' END;
SELECT CASE 1 WHEN 1 THEN 'a' WHEN 2 THEN 'b' END;

-- CAST
SELECT CAST(1 AS String);
SELECT 1::String;
SELECT NULL::Nullable(UInt8);
SELECT CAST(1 AS Nullable(UInt8));

-- IN / NOT IN / GLOBAL IN
SELECT 1 WHERE 1 IN (1, 2, 3);
SELECT 1 WHERE 1 NOT IN (1, 2, 3);
SELECT 1 WHERE 1 IN (SELECT 1);
SELECT 1 WHERE 1 NOT IN (SELECT 1);
SELECT 1 WHERE 1 GLOBAL IN (SELECT 1);
SELECT 1 WHERE 1 GLOBAL NOT IN (SELECT 1);
SELECT 1 WHERE (1, 2) IN ((1, 2), (3, 4));
SELECT 1 WHERE (1, 2) NOT IN ((1, 2));
SELECT 1 WHERE 1 IN tuple(1, 2, 3);

-- ANY / ALL subquery quantifiers
SELECT 1 WHERE 1 = ANY (SELECT 1);
SELECT 1 WHERE 1 = ALL (SELECT 1);
SELECT 1 WHERE 1 >= ANY (SELECT 0);
SELECT 1 WHERE 1 > ANY (SELECT 0);
SELECT 1 WHERE 1 != ALL (SELECT 2);

-- Special float values
SELECT nan, -nan, inf, -inf;

-- EXISTS
SELECT 1 WHERE EXISTS (SELECT 1);

-- Literals
SELECT [1, 2, 3];
SELECT (1, 2, 3);
SELECT map('a', 1, 'b', 2);

-- Element access
SELECT [1, 2, 3][1];
SELECT map('a', 1, 'b', 2)['a'];
SELECT (1, 'hello', [1, 2]).1;
SELECT (1, 'hello', [1, 2]).2;
SELECT tupleElement(('hello', 'world'), 3, []);

-- Lambda
SELECT arrayMap(x -> x + 1, [1, 2, 3]);
SELECT arrayFilter(x -> x > 1, [1, 2, 3]);
SELECT arraySort((x, y) -> y, [1, 2, 3], [3, 2, 1]);

-- Functions
SELECT if(1, 2, 3);
SELECT multiIf(0, 'a', 1, 'b', 'c');
SELECT coalesce(NULL, NULL, 1);
SELECT greatest(1, 2, 3);
SELECT least(1, 2, 3);
SELECT concat('a', 'b', 'c');
SELECT substring('hello', 2, 3);
SELECT toDate('2024-01-01');
SELECT toDateTime('2024-01-01 12:00:00');

-- Aggregate functions
SELECT count() FROM numbers(10);
SELECT sum(number) FROM numbers(10);
SELECT avg(number) FROM numbers(10);
SELECT min(number) FROM numbers(10);
SELECT max(number) FROM numbers(10);
SELECT countIf(number, number > 5) FROM numbers(10);
SELECT sumIf(number, number > 5) FROM numbers(10);
SELECT groupArray(number) FROM numbers(5);
SELECT groupArrayIf(number, number > 2) FROM numbers(5);
SELECT quantile(0.5)(number) FROM numbers(10);
SELECT quantiles(0.25, 0.5, 0.75)(number) FROM numbers(10);

-- GROUP BY
SELECT number % 2, count() FROM numbers(10) GROUP BY number % 2 ORDER BY ALL;
SELECT number % 2, count() FROM numbers(10) GROUP BY ALL ORDER BY ALL;
SELECT number % 2, count() FROM numbers(10) GROUP BY number % 2 WITH ROLLUP ORDER BY ALL;
SELECT number % 2, count() FROM numbers(10) GROUP BY number % 2 WITH CUBE ORDER BY ALL;
SELECT number % 2, count() FROM numbers(10) GROUP BY number % 2 WITH TOTALS ORDER BY ALL;
SELECT number % 2, count() FROM numbers(10) GROUP BY GROUPING SETS ((number % 2)) ORDER BY ALL;
SELECT number % 2, number % 3, count() FROM numbers(10) GROUP BY GROUPING SETS ((number % 2), (number % 3)) ORDER BY 1, 2, 3;

-- HAVING
SELECT number % 3 AS n, count() FROM numbers(10) GROUP BY n HAVING count() > 3 ORDER BY ALL;

-- ORDER BY
SELECT number FROM numbers(5) ORDER BY ALL;
SELECT number FROM numbers(5) ORDER BY ALL DESC;
SELECT number FROM numbers(5) ORDER BY ALL ASC NULLS FIRST;
SELECT number FROM numbers(5) ORDER BY ALL DESC NULLS LAST;
SELECT number FROM numbers(5) ORDER BY 1 DESC;
SELECT number, number * 2 AS x FROM numbers(5) ORDER BY x DESC, number ASC;
SELECT number FROM numbers(5) ORDER BY number WITH FILL FROM 0 TO 7 STEP 1;

-- LIMIT BY
SELECT * FROM numbers(10) ORDER BY ALL LIMIT 1 BY number % 3;

-- DISTINCT ON
SELECT DISTINCT ON (number % 2) number FROM numbers(5) ORDER BY number;

-- Set operations
SELECT * FROM (SELECT 1 UNION ALL SELECT 2) ORDER BY 1;
SELECT * FROM (SELECT 1 UNION DISTINCT SELECT 2) ORDER BY 1;
SELECT * FROM (SELECT 1 INTERSECT SELECT 1) ORDER BY 1;
SELECT * FROM (SELECT 1 INTERSECT ALL SELECT 1) ORDER BY 1;
SELECT * FROM (SELECT 1 EXCEPT SELECT 2) ORDER BY 1;
SELECT * FROM (SELECT 1 EXCEPT ALL SELECT 2) ORDER BY 1;
SELECT * FROM (SELECT 1 EXCEPT DISTINCT SELECT 2) ORDER BY 1;
SELECT * FROM (SELECT 1 UNION ALL SELECT 2 EXCEPT SELECT 3) ORDER BY 1;
SELECT * FROM (SELECT 1 INTERSECT SELECT 1 EXCEPT SELECT 2) ORDER BY 1;
SELECT * FROM (SELECT 1 UNION ALL (SELECT 2 UNION ALL SELECT 3)) ORDER BY 1;
SELECT * FROM ((SELECT 1) EXCEPT (SELECT 2)) ORDER BY 1;
SELECT * FROM (SELECT number FROM numbers(5) EXCEPT SELECT number FROM numbers(3, 5)) ORDER BY 1;

-- Subqueries
SELECT * FROM (SELECT 1 AS x) AS t;
SELECT (SELECT 1);
SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT 1)));
SELECT * FROM (SELECT 1 EXCEPT SELECT 2);
SELECT 1 IN (SELECT 1);
SELECT * FROM numbers(5) WHERE number IN (SELECT * FROM numbers(3) EXCEPT SELECT * FROM numbers(1));

-- CTE
WITH 1 AS x SELECT x;
WITH t AS (SELECT number FROM numbers(5)) SELECT * FROM t ORDER BY ALL;

-- Window functions
SELECT number, sum(number) OVER (ORDER BY number) FROM numbers(5);
SELECT number, row_number() OVER (ORDER BY number) FROM numbers(5);
SELECT number, rank() OVER (ORDER BY number) FROM numbers(5);
SELECT number, dense_rank() OVER (ORDER BY number) FROM numbers(5);
SELECT number, lag(number) OVER (ORDER BY number) FROM numbers(5);
SELECT number, lead(number) OVER (ORDER BY number) FROM numbers(5);
SELECT number, sum(number) OVER w FROM numbers(5) WINDOW w AS (ORDER BY number);

-- Table functions
SELECT * FROM numbers(5) ORDER BY ALL;
SELECT * FROM zeros(3);

-- JOINs
SELECT * FROM numbers(1) AS a CROSS JOIN numbers(1) AS b;
SELECT * FROM numbers(1) AS a INNER JOIN numbers(1) AS b ON a.number = b.number;
SELECT * FROM numbers(1) AS a LEFT JOIN numbers(1) AS b ON a.number = b.number;
SELECT * FROM numbers(1) AS a RIGHT JOIN numbers(1) AS b ON a.number = b.number;
SELECT * FROM numbers(1) AS a FULL JOIN numbers(1) AS b ON a.number = b.number;
SELECT * FROM numbers(1) AS a ANTI JOIN numbers(1) AS b ON a.number = b.number;
SELECT * FROM numbers(1) AS a SEMI JOIN numbers(1) AS b ON a.number = b.number;

-- ARRAY JOIN
SELECT * FROM numbers(3) ARRAY JOIN [1, 2, 3] AS x;

-- Column modifiers
SELECT COLUMNS('n.*') FROM numbers(3);
SELECT * REPLACE (number + 1 AS number) FROM numbers(3);
SELECT * EXCEPT (number) FROM (SELECT number, number + 1 AS x FROM numbers(3));
SELECT * APPLY (toString) FROM numbers(3);

-- QUALIFY
SELECT number, row_number() OVER (ORDER BY number) AS rn FROM numbers(10) QUALIFY rn <= 3;

-- Complex WHERE
SELECT 1 WHERE 1 AND 1;
SELECT 1 WHERE 1 OR 0;
SELECT 1 WHERE NOT 0;
SELECT 1 WHERE (1 AND 0) OR 1;

-- EXPLAIN
EXPLAIN SELECT 1;
EXPLAIN AST SELECT 1;
EXPLAIN SYNTAX SELECT 1;
EXPLAIN PLAN SELECT 1;

-- Data types with empty arguments
CREATE TABLE test_fmt_empty_tuple (x Array(Tuple())) ENGINE = Memory;
DROP TABLE test_fmt_empty_tuple;

-- DDL
CREATE TABLE test_fmt_mt (x UInt8) ENGINE = MergeTree ORDER BY x;
CREATE TABLE test_fmt_mt2 (x UInt8, y String DEFAULT 'foo') ENGINE = MergeTree ORDER BY x;
CREATE TABLE test_fmt_mem (x UInt8) ENGINE = Memory;
CREATE TABLE test_fmt_codec (x UInt8 CODEC(LZ4)) ENGINE = MergeTree ORDER BY x;
CREATE TABLE IF NOT EXISTS test_fmt_mem2 (x UInt8) ENGINE = Memory;
CREATE VIEW test_fmt_view AS SELECT 1;
CREATE TABLE test_fmt_replacing (x UInt8, y String) ENGINE = ReplacingMergeTree ORDER BY x;
CREATE MATERIALIZED VIEW test_fmt_mv ENGINE = MergeTree ORDER BY x AS SELECT x FROM test_fmt_mem;

-- INSERT + SELECT
INSERT INTO test_fmt_mem SELECT number FROM numbers(5);
SELECT * FROM test_fmt_mem ORDER BY ALL;

-- ALTER
ALTER TABLE test_fmt_mt ADD COLUMN z UInt8 DEFAULT 0;

-- PREWHERE
INSERT INTO test_fmt_mt SELECT number, number FROM numbers(10);
SELECT x FROM test_fmt_mt PREWHERE x > 7 ORDER BY ALL;

-- FINAL
SELECT * FROM test_fmt_replacing FINAL;

-- DESCRIBE
DESCRIBE TABLE test_fmt_mt;

-- SHOW
SHOW CREATE TABLE test_fmt_mt;

-- DETACH / ATTACH
DETACH TABLE test_fmt_mem2;
ATTACH TABLE test_fmt_mem2;

-- SYSTEM
SYSTEM FLUSH LOGS query_log;

-- DROP
DROP VIEW test_fmt_mv;
DROP VIEW test_fmt_view;
DROP TABLE test_fmt_mt;
DROP TABLE test_fmt_mt2;
DROP TABLE test_fmt_mem;
DROP TABLE test_fmt_codec;
DROP TABLE test_fmt_mem2;
DROP TABLE test_fmt_replacing;

-- Access control
CREATE USER IF NOT EXISTS test_fmt_user1;
CREATE USER IF NOT EXISTS test_fmt_user2 IDENTIFIED WITH plaintext_password BY 'abc';
CREATE ROLE IF NOT EXISTS test_fmt_role1;
DROP ROLE IF EXISTS test_fmt_role1;
DROP USER IF EXISTS test_fmt_user1;
DROP USER IF EXISTS test_fmt_user2;

-- Database
CREATE DATABASE IF NOT EXISTS test_fmt_db;
DROP DATABASE IF EXISTS test_fmt_db;

-- FORMAT
SELECT 1 FORMAT Null;

-- SET
SET max_threads = 1;
SELECT 1;
