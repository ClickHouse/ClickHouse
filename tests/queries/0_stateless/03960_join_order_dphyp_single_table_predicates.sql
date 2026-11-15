-- Verify that single-table and constant predicates are applied correctly
-- when DPhyp reorders joins. These predicates stay in filter steps and
-- must not be lost during join reordering.

SET allow_experimental_analyzer = 1;
SET use_statistics = 1;
SET query_plan_join_swap_table = 'auto';
SET enable_join_runtime_filters = 0;
SET enable_parallel_replicas = 0;

CREATE TABLE stp_a (id UInt32, val UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE stp_b (id UInt32, a_id UInt32, val UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE stp_c (id UInt32, b_id UInt32, val UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';

INSERT INTO stp_a SELECT number, number * 10 FROM numbers(10);
INSERT INTO stp_b SELECT number, number % 10, number * 100 FROM numbers(20);
INSERT INTO stp_c SELECT number, number % 20, number * 1000 FROM numbers(30);

-- 1. Single-table filter on the first table
SELECT count()
FROM stp_a a, stp_b b, stp_c c
WHERE a.id = b.a_id AND b.id = c.b_id AND a.val >= 50
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp';

SELECT count()
FROM stp_a a, stp_b b, stp_c c
WHERE a.id = b.a_id AND b.id = c.b_id AND a.val >= 50
SETTINGS query_plan_optimize_join_order_algorithm = 'greedy';

-- 2. Single-table filter on a middle table
SELECT count()
FROM stp_a a, stp_b b, stp_c c
WHERE a.id = b.a_id AND b.id = c.b_id AND b.val < 500
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp';

SELECT count()
FROM stp_a a, stp_b b, stp_c c
WHERE a.id = b.a_id AND b.id = c.b_id AND b.val < 500
SETTINGS query_plan_optimize_join_order_algorithm = 'greedy';

-- 3. Single-table filters on multiple tables simultaneously
SELECT count()
FROM stp_a a, stp_b b, stp_c c
WHERE a.id = b.a_id AND b.id = c.b_id AND a.val >= 50 AND c.val < 15000
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp';

SELECT count()
FROM stp_a a, stp_b b, stp_c c
WHERE a.id = b.a_id AND b.id = c.b_id AND a.val >= 50 AND c.val < 15000
SETTINGS query_plan_optimize_join_order_algorithm = 'greedy';

-- 4. Constant predicate alongside join predicates
SELECT count()
FROM stp_a a, stp_b b, stp_c c
WHERE a.id = b.a_id AND b.id = c.b_id AND 1
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp';

SELECT count()
FROM stp_a a, stp_b b, stp_c c
WHERE a.id = b.a_id AND b.id = c.b_id AND 1
SETTINGS query_plan_optimize_join_order_algorithm = 'greedy';

-- 5. Expression-based single-table filter (not a simple column comparison)
SELECT count()
FROM stp_a a, stp_b b, stp_c c
WHERE a.id = b.a_id AND b.id = c.b_id AND a.id + a.val > 55
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp';

SELECT count()
FROM stp_a a, stp_b b, stp_c c
WHERE a.id = b.a_id AND b.id = c.b_id AND a.id + a.val > 55
SETTINGS query_plan_optimize_join_order_algorithm = 'greedy';

DROP TABLE stp_a;
DROP TABLE stp_b;
DROP TABLE stp_c;
