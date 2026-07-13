-- A single-table predicate merged into a join condition by
-- query_plan_merge_filter_into_join_condition is unsupported by DPhyp: it returns
-- nullptr so the next algorithm in the chain handles the query and keeps the predicate.

SET allow_experimental_analyzer = 1;
SET use_statistics = 1;
SET enable_parallel_replicas = 0;
SET cross_to_inner_join_rewrite = 0;
SET query_plan_merge_filter_into_join_condition = 1;
-- Keep the single-table filter above the join so it gets merged into the join condition.
SET query_plan_filter_push_down = 0;
-- Pin the relation limit so the joins are reordered (randomized settings may lower it).
SET query_plan_optimize_join_order_limit = 10;

CREATE TABLE mff_a (id UInt32, val UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE mff_b (id UInt32, a_id UInt32, val UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE mff_c (id UInt32, b_id UInt32, val UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';

INSERT INTO mff_a SELECT number, number * 10 FROM numbers(10);
INSERT INTO mff_b SELECT number, number % 10, number * 100 FROM numbers(20);
INSERT INTO mff_c SELECT number, number % 20, number * 1000 FROM numbers(30);

-- dphyp alone cannot attach the merged single-table predicate.
SELECT count()
FROM mff_a a, mff_b b, mff_c c
WHERE a.id = b.a_id AND b.id = c.b_id AND a.val >= 50
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp'; -- { serverError EXPERIMENTAL_FEATURE_ERROR }

-- dphyp,greedy falls back to greedy, which keeps the predicate.
SELECT count()
FROM mff_a a, mff_b b, mff_c c
WHERE a.id = b.a_id AND b.id = c.b_id AND a.val >= 50
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp,greedy';

-- greedy alone must agree.
SELECT count()
FROM mff_a a, mff_b b, mff_c c
WHERE a.id = b.a_id AND b.id = c.b_id AND a.val >= 50
SETTINGS query_plan_optimize_join_order_algorithm = 'greedy';

DROP TABLE mff_a;
DROP TABLE mff_b;
DROP TABLE mff_c;
