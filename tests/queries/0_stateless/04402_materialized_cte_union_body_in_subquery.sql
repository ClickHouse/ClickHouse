-- Test: UNION-bodied MATERIALIZED CTE used directly as the right-hand side of IN
-- exercises the `resolveUnion` branch of the identifier-resolution materialized-CTE path.
SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;
SET enable_materialized_cte = 1;

-- Non-correlated UNION body: must materialize and work.
WITH t AS MATERIALIZED (SELECT number FROM numbers(3) UNION ALL SELECT number FROM numbers(2))
SELECT number FROM numbers(8) WHERE number IN (t) ORDER BY number;

-- Correlated UNION body (inner `number` resolves to outer `numbers`): must be rejected.
WITH t AS MATERIALIZED (SELECT number FROM zeros(8) UNION ALL SELECT number FROM zeros(8))
SELECT * FROM numbers(8) WHERE number IN (t); -- { serverError UNSUPPORTED_METHOD }
