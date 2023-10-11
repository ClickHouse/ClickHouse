set query_plan_optimize_lazy_projection=1;

DROP TABLE IF EXISTS optimize_lazy_projection;
CREATE TABLE optimize_lazy_projection (a UInt64, b UInt64, c UInt64, d UInt64) ENGINE MergeTree() PARTITION BY b ORDER BY a;
INSERT INTO optimize_lazy_projection SELECT number, number%2, number, number%3 from numbers(0, 100);
INSERT INTO optimize_lazy_projection SELECT number, number%2, number, number%3 from numbers(100, 100);

-- { echoOn }
SELECT * FROM optimize_lazy_projection ORDER BY c LIMIT 3;
-- queries with filter
SELECT * FROM optimize_lazy_projection WHERE d > 1 ORDER BY c LIMIT 3;
SELECT * FROM optimize_lazy_projection PREWHERE d > 1 ORDER BY c LIMIT 3;
-- queries with function in order by
SELECT * FROM optimize_lazy_projection WHERE d > 1 ORDER BY -c LIMIT 3;
SELECT * FROM optimize_lazy_projection WHERE d > 1 ORDER BY -toFloat64(c) LIMIT 3;
SELECT * FROM optimize_lazy_projection WHERE d > 1 ORDER BY c+1 LIMIT 3;
-- queries with function in filter
SELECT * FROM optimize_lazy_projection WHERE d%3 > 1 ORDER BY c LIMIT 3;
-- queries with aliases
SELECT a AS a, b AS b, c AS c, d AS d FROM optimize_lazy_projection WHERE d > 1 ORDER BY c LIMIT 3;
SELECT a AS a, b AS b, c AS c, d AS d FROM optimize_lazy_projection WHERE d > 1 ORDER BY c LIMIT 3;
SELECT a+1 AS a, b AS b, c+1 AS c, d+1 AS d FROM optimize_lazy_projection WHERE d > 1 ORDER BY c LIMIT 3;
SELECT a+1 AS a, b AS b, c+1 AS c, d+1 AS d FROM optimize_lazy_projection WHERE d > 1 ORDER BY c LIMIT 3;
-- queries with non-trivial action's chain in expression
SELECT y, z FROM (SELECT a as y, b as z FROM optimize_lazy_projection WHERE d > 1 ORDER BY c LIMIT 3) ORDER BY z + 1;
-- { echoOff }
DROP TABLE IF EXISTS optimize_lazy_projection;