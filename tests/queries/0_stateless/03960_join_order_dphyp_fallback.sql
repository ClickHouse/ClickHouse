-- DPhyp fallback scenarios: when DPhyp alone cannot produce a plan,
-- the fallback chain (dphyp,greedy or dphyp,dpsize,greedy) must succeed.

SET allow_experimental_analyzer = 1;
SET use_statistics = 1;
SET query_plan_join_swap_table = 'auto';
SET enable_join_runtime_filters = 0;
SET enable_parallel_replicas = 0;
SET cross_to_inner_join_rewrite = 0;
SET query_plan_merge_filter_into_join_condition = 0;
SET query_plan_optimize_join_order_limit = 10;

-- =====================================================================
-- 1. Disconnected graph: two independent 2-table components.
--    DPhyp/DPsize cannot span all four relations; greedy uses cross product.
-- =====================================================================

CREATE TABLE fb_a (id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE fb_b (id UInt32, a_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE fb_c (id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE fb_d (id UInt32, c_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';

INSERT INTO fb_a SELECT number FROM numbers(3);
INSERT INTO fb_b SELECT number, number % 3 FROM numbers(6);
INSERT INTO fb_c SELECT number FROM numbers(4);
INSERT INTO fb_d SELECT number, number % 4 FROM numbers(8);

-- dphyp alone fails.
SELECT count(a.id) FROM fb_a a, fb_b b, fb_c c, fb_d d
WHERE a.id = b.a_id AND c.id = d.c_id
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp'; -- { serverError EXPERIMENTAL_FEATURE_ERROR }

-- dphyp,greedy succeeds.
SELECT count(a.id) FROM fb_a a, fb_b b, fb_c c, fb_d d
WHERE a.id = b.a_id AND c.id = d.c_id
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp,greedy';

-- dphyp,dpsize also fails on disconnected graph.
SELECT count(a.id) FROM fb_a a, fb_b b, fb_c c, fb_d d
WHERE a.id = b.a_id AND c.id = d.c_id
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp,dpsize'; -- { serverError EXPERIMENTAL_FEATURE_ERROR }

-- Full triple chain: dphyp,dpsize,greedy succeeds.
SELECT count(a.id) FROM fb_a a, fb_b b, fb_c c, fb_d d
WHERE a.id = b.a_id AND c.id = d.c_id
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp,dpsize,greedy';

DROP TABLE fb_a;
DROP TABLE fb_b;
DROP TABLE fb_c;
DROP TABLE fb_d;

-- =====================================================================
-- 2. Outer join: DPhyp does not support outer joins.
-- =====================================================================

-- dphyp alone fails on LEFT JOIN.
SELECT 1 FROM (SELECT 1 c0) t0 LEFT JOIN (SELECT 1 c0) t1 ON t0.c0 = t1.c0
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp'; -- { serverError EXPERIMENTAL_FEATURE_ERROR }

-- dphyp,greedy succeeds.
SELECT 1 FROM (SELECT 1 c0) t0 LEFT JOIN (SELECT 1 c0) t1 ON t0.c0 = t1.c0
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp,greedy';

-- =====================================================================
-- 3. Range predicate: inequality does not form a join edge, leaving a
--    table disconnected.
-- =====================================================================

CREATE TABLE fb_ra (id UInt32, val UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE fb_rb (id UInt32, val UInt32, c_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE fb_rc (id UInt32, val UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';

INSERT INTO fb_ra SELECT number, number * 2 FROM numbers(10);
INSERT INTO fb_rb SELECT number, number * 3, number % 5 FROM numbers(10);
INSERT INTO fb_rc SELECT number, number * 5 FROM numbers(5);

-- dphyp alone fails: fb_ra has no equijoin edge.
SELECT count()
FROM fb_ra a, fb_rb b, fb_rc c
WHERE a.val < b.val AND b.c_id = c.id
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp'; -- { serverError EXPERIMENTAL_FEATURE_ERROR }

-- dphyp,greedy succeeds.
SELECT count()
FROM fb_ra a, fb_rb b, fb_rc c
WHERE a.val < b.val AND b.c_id = c.id
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp,greedy';

-- greedy alone must agree.
SELECT count()
FROM fb_ra a, fb_rb b, fb_rc c
WHERE a.val < b.val AND b.c_id = c.id
SETTINGS query_plan_optimize_join_order_algorithm = 'greedy';

DROP TABLE fb_ra;
DROP TABLE fb_rb;
DROP TABLE fb_rc;
