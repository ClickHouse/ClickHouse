-- Test that materialized CTEs with dependencies are executed in correct order.
-- Scalar subqueries referencing materialized CTEs must materialize them first.
-- Before the fix, evaluateScalarSubqueryIfNeeded() executed scalar subquery
-- pipelines during the QueryAnalyzer phase without materializing CTEs,
-- causing: "Logical error: Reading from materialized CTE before materialization"

SET enable_materialized_cte = 1;

-- Case 1: scalar subquery referencing a CTE that depends on another CTE
WITH y AS MATERIALIZED (SELECT number AS a FROM numbers(5)),
     w AS MATERIALIZED (SELECT * FROM y WHERE a > 0)
SELECT count() FROM y WHERE a != (SELECT min(a) FROM w);

-- Case 2: 3-level CTE chain with scalar subquery
WITH y AS MATERIALIZED (SELECT number AS a FROM numbers(10)),
     w AS MATERIALIZED (SELECT * FROM y WHERE a > 2),
     z AS MATERIALIZED (SELECT * FROM w WHERE a < 8)
SELECT count() FROM z WHERE a != (SELECT min(a) FROM y);

-- Case 3: multiple scalar subqueries referencing different CTEs
WITH y AS MATERIALIZED (SELECT number AS a FROM numbers(5)),
     w AS MATERIALIZED (SELECT * FROM y WHERE a > 0)
SELECT (SELECT min(a) FROM w), (SELECT max(a) FROM y), count() FROM y;

-- Case 4: simple CTE dependency without scalar subquery (should still work)
WITH y AS MATERIALIZED (SELECT number AS a FROM numbers(5)),
     w AS MATERIALIZED (SELECT * FROM y WHERE a > 0)
SELECT count() FROM w;

-- Case 5: same CTE referenced multiple times via scalar subqueries
WITH y AS MATERIALIZED (SELECT number AS a FROM numbers(5))
SELECT (SELECT count() FROM y), (SELECT sum(a) FROM y), count() FROM y WHERE a != (SELECT min(a) FROM y);

-- Case 6: dependent CTEs, both referenced multiple times with scalar subqueries
WITH y AS MATERIALIZED (SELECT number AS a FROM numbers(5)),
     w AS MATERIALIZED (SELECT * FROM y WHERE a > 0)
SELECT (SELECT count() FROM y), (SELECT count() FROM w), count() FROM w WHERE a != (SELECT min(a) FROM w);

-- Case 7: CTE used in self-join with scalar subquery
WITH y AS MATERIALIZED (SELECT number AS a FROM numbers(5))
SELECT count() FROM y AS y1 INNER JOIN y AS y2 ON y1.a = y2.a WHERE y1.a > (SELECT min(a) FROM y);
