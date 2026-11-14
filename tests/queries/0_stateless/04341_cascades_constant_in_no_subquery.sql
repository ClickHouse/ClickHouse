-- Tags: no-darwin
-- no-darwin: distributed execution uses the streaming exchange, which is implemented only on Linux.
-- The Cascades optimizer forces an `IN` -> `JOIN` rewrite, but that only applies to `IN (subquery)`.
-- A constant or tuple right-hand side needs no rewrite and must not require correlated subqueries.
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET enable_analyzer = 1;
SET make_distributed_plan = 1;
SET enable_cascades_optimizer = 1;
SET distributed_plan_execute_locally = 1;
SET allow_experimental_correlated_subqueries = 0;

SELECT 1 IN (1, 2);
SELECT 3 IN (1, 2);
SELECT 1 NOT IN (1, 2);
SELECT number IN (1, 2) FROM numbers(4) ORDER BY number;
