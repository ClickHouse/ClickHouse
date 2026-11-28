-- A view() read plans its own MATERIALIZED CTE. Inside an IN set built at runtime, that CTE lost its only writer.

SET enable_materialized_cte = 1;

SELECT count() FROM numbers(15) WHERE number IN (SELECT x FROM view(WITH a AS MATERIALIZED (SELECT number AS x, rand64() AS r FROM numbers(10)) SELECT a.x AS x FROM a JOIN a AS b ON a.r = b.r));
SELECT count() FROM numbers(15) WHERE number NOT IN (SELECT x FROM view(WITH a AS MATERIALIZED (SELECT number AS x, rand64() AS r FROM numbers(10)) SELECT a.x AS x FROM a JOIN a AS b ON a.r = b.r));
SELECT count() FROM numbers(15) WHERE number IN (SELECT x FROM view(WITH a AS MATERIALIZED (SELECT number AS x, rand64() AS r FROM numbers(10)) SELECT x, r FROM a UNION ALL SELECT x, r FROM a) GROUP BY x HAVING uniqExact(r) = 1);
SELECT count() FROM numbers(15) WHERE number IN (SELECT x FROM remote('127.0.0.1', view(WITH a AS MATERIALIZED (SELECT number AS x, rand64() AS r FROM numbers(10)) SELECT a.x AS x FROM a JOIN a AS b ON a.r = b.r)));
SELECT count() FROM numbers(15) WHERE number IN (SELECT x FROM remote('127.0.0.{1,2}', view(WITH a AS MATERIALIZED (SELECT number AS x, rand64() AS r FROM numbers(10)) SELECT a.x AS x FROM a JOIN a AS b ON a.r = b.r)));

-- The set subquery's own CTE `c` (materialized by the enclosing plan) next to the view's CTE `a` (materialized inside the set).
SELECT count() FROM numbers(15) WHERE number IN (WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3)) SELECT x FROM c WHERE x IN (SELECT x FROM c) AND x IN (SELECT x FROM view(WITH a AS MATERIALIZED (SELECT number AS x, rand64() AS r FROM numbers(10)) SELECT a.x AS x FROM a JOIN a AS b ON a.r = b.r)));

-- A set inside the view's own query reads the view's CTE.
SELECT count() FROM numbers(15) WHERE number IN (SELECT x FROM view(WITH a AS MATERIALIZED (SELECT number AS x, rand64() AS r FROM numbers(10)) SELECT x FROM a WHERE r IN (SELECT r FROM a)));

SELECT count() FROM system.one WHERE exists((SELECT x FROM view(WITH a AS MATERIALIZED (SELECT number AS x, rand64() AS r FROM numbers(10)) SELECT a.x AS x FROM a JOIN a AS b ON a.r = b.r))) SETTINGS execute_exists_as_scalar_subquery = 0;
