-- Range predicate: A.val < B.val is not an equijoin and does not form a join
-- edge between A and B in the query graph, leaving A disconnected.
-- solveDPhyp returns nullptr; solve() throws EXPERIMENTAL_FEATURE_ERROR.
-- With dphyp,greedy the greedy algorithm handles the disconnected component
-- via a cross product and then applies the filter, producing the correct result.

SET allow_experimental_analyzer = 1;
SET query_plan_optimize_join_order_limit = 10;
SET use_statistics = 1;
SET query_plan_join_swap_table = 'auto';
SET enable_join_runtime_filters = 0;

CREATE TABLE rng_a (id UInt32, val UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE rng_b (id UInt32, val UInt32, c_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE rng_c (id UInt32, val UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';

INSERT INTO rng_a SELECT number, number * 2 FROM numbers(10);
INSERT INTO rng_b SELECT number, number * 3, number % 5 FROM numbers(10);
INSERT INTO rng_c SELECT number, number * 5 FROM numbers(5);

-- dphyp alone fails: A has no equijoin connecting it to B or C.
SELECT count()
FROM rng_a a, rng_b b, rng_c c
WHERE a.val < b.val AND b.c_id = c.id
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp', enable_parallel_replicas = 0; -- { serverError EXPERIMENTAL_FEATURE_ERROR }

-- dphyp,greedy succeeds: greedy handles A as a disconnected component.
SELECT count()
FROM rng_a a, rng_b b, rng_c c
WHERE a.val < b.val AND b.c_id = c.id
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp,greedy', enable_parallel_replicas = 0;

-- Greedy must produce the same result.
SELECT count()
FROM rng_a a, rng_b b, rng_c c
WHERE a.val < b.val AND b.c_id = c.id
SETTINGS query_plan_optimize_join_order_algorithm = 'greedy', enable_parallel_replicas = 0;

DROP TABLE rng_a;
DROP TABLE rng_b;
DROP TABLE rng_c;
