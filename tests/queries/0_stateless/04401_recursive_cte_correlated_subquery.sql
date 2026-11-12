SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;

-- A recursive CTE is planned as a fixed-point over a temporary table and cannot be correlated
-- to an outer scope. When an identifier in a recursive term leaks to an outer recursive CTE's
-- column, it used to produce a ColumnNode with a dangling source node and abort with
-- "query tree node does not have valid source node" (LOGICAL_ERROR) instead of a clean error.

SELECT count(*)
FROM (WITH RECURSIVE t AS (SELECT toUInt64(1) AS n UNION ALL SELECT n + 1 FROM t WHERE n < 5) SELECT * FROM t) AS t
WHERE n < (SELECT count(*) FROM (WITH RECURSIVE s AS (SELECT (SELECT toUInt64(1) AS n) UNION ALL SELECT n + 1 FROM s WHERE n < 3) SELECT * FROM s) AS s); -- { serverError UNSUPPORTED_METHOD }

-- The same unsupported contract applies when only the seed (non-recursive) term is correlated:
-- the whole union is lowered to ReadFromRecursiveCTEStep with no mechanism to supply the outer
-- binding, so it must also be rejected with a clean error rather than aborting.
SELECT count(*)
FROM (WITH RECURSIVE t AS (SELECT toUInt64(1) AS n UNION ALL SELECT n + 1 FROM t WHERE n < 5) SELECT * FROM t) AS t
WHERE n < (SELECT count(*) FROM (WITH RECURSIVE s AS (SELECT n AS m UNION ALL SELECT m + 1 FROM s WHERE m < 3) SELECT * FROM s) AS s); -- { serverError UNSUPPORTED_METHOD }

-- A non-correlated recursive CTE nested inside another still works.
SELECT count(*)
FROM (WITH RECURSIVE t AS (SELECT toUInt64(1) AS n UNION ALL SELECT n + 1 FROM t WHERE n < 5) SELECT * FROM t) AS t
WHERE n < (SELECT count(*) FROM (WITH RECURSIVE s AS (SELECT toUInt64(1) AS m UNION ALL SELECT m + 1 FROM s WHERE m < 3) SELECT * FROM s) AS s);

-- A correlated reference to an outer recursive CTE from a non-recursive inner subquery still works.
SELECT count(*)
FROM (WITH RECURSIVE t AS (SELECT toUInt64(1) AS n UNION ALL SELECT n + 1 FROM t WHERE n < 5) SELECT * FROM t) AS t
WHERE n < (SELECT count(*) FROM numbers(3) AS i WHERE i.number < n);
