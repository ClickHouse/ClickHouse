-- Tags: distributed

-- `x IN cte` in the SELECT list of a distributed query: the shards get the CTE as a plain subquery and must name the
-- set like the initiator does, also when the CTE is a UNION, INTERSECT or EXCEPT query or is declared MATERIALIZED.

WITH a AS (SELECT number FROM numbers(3) UNION ALL SELECT number FROM numbers(3))
SELECT sum(number IN (a)) FROM remote('127.0.0.{1,2}', numbers(5));

WITH a AS (SELECT number FROM numbers(3) UNION ALL SELECT number FROM numbers(3))
SELECT sum(number IN (a)) FROM remote('127.0.0.{1,2}', numbers(5)) SETTINGS prefer_localhost_replica = 0;

WITH a AS (SELECT number FROM numbers(3) UNION DISTINCT SELECT number FROM numbers(3))
SELECT sum(number IN (a)) FROM remote('127.0.0.{1,2}', numbers(5));

WITH a AS (SELECT number FROM numbers(4) EXCEPT SELECT number FROM numbers(1))
SELECT sum(number IN (a)) FROM remote('127.0.0.{1,2}', numbers(5));

WITH a AS (SELECT number FROM numbers(3) INTERSECT SELECT number FROM numbers(4))
SELECT sum(number IN (a)) FROM remote('127.0.0.{1,2}', numbers(5));

WITH a AS (SELECT number FROM numbers(3) UNION ALL SELECT number FROM numbers(3)), b AS (SELECT number FROM a)
SELECT sum(number IN (b)) FROM remote('127.0.0.{1,2}', numbers(5));

WITH a AS (SELECT number FROM numbers(3) UNION ALL SELECT number FROM numbers(3))
SELECT sum(number IN (SELECT number FROM a)) FROM remote('127.0.0.{1,2}', numbers(5));

-- With enable_materialized_cte = 0 the MATERIALIZED keyword is ignored, with a warning.
SET send_logs_level = 'error';
SET enable_materialized_cte = 0;
WITH a AS MATERIALIZED (SELECT number FROM numbers(3))
SELECT sum(number IN (a)) FROM remote('127.0.0.{1,2}', numbers(5));

SET enable_materialized_cte = 1;
WITH a AS MATERIALIZED (SELECT number FROM numbers(3))
SELECT sum(number IN (a)) FROM remote('127.0.0.{1,2}', numbers(5));

WITH a AS MATERIALIZED (SELECT number FROM numbers(3) UNION ALL SELECT number FROM numbers(3))
SELECT sum(number IN (a)) FROM remote('127.0.0.{1,2}', numbers(5));
