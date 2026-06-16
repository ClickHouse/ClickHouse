-- The Cascades optimizer forces an IN->JOIN rewrite, but that only applies to IN (subquery).
-- A constant or tuple RHS needs no rewrite and must not require correlated subqueries.
SET enable_analyzer = 1;
SET make_distributed_plan = 1;
SET enable_cascades_optimizer = 1;
SET distributed_plan_execute_locally = 1;
SET allow_experimental_correlated_subqueries = 0;

SELECT 1 IN (1, 2);
SELECT 3 IN (1, 2);
SELECT 1 NOT IN (1, 2);
SELECT number IN (1, 2) FROM numbers(4) ORDER BY number;
