-- DPhyp does not support outer joins; it must throw EXPERIMENTAL_FEATURE_ERROR
-- when used alone.  The dphyp,greedy combination succeeds by falling back to
-- greedy for the outer join.

SET allow_experimental_analyzer = 1;
SET enable_join_runtime_filters = 0;

-- dphyp alone fails on outer join.
SELECT 1 FROM (SELECT 1 c0) t0 LEFT JOIN (SELECT 1 c0) t1 ON t0.c0 = t1.c0
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp', enable_parallel_replicas = 0; -- { serverError EXPERIMENTAL_FEATURE_ERROR }

-- dphyp,greedy succeeds.
SELECT 1 FROM (SELECT 1 c0) t0 LEFT JOIN (SELECT 1 c0) t1 ON t0.c0 = t1.c0
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp,greedy', enable_parallel_replicas = 0;
