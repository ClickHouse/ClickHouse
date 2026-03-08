-- limit=0: optimization entirely disabled.
-- When query_plan_optimize_join_order_limit = 0, the optimizer skips the
-- reordering phase (early return in optimizeJoinLogicalImpl) and uses the
-- original written join order.  The result must still be correct.

SET allow_experimental_analyzer = 1;
SET use_statistics = 1;
SET query_plan_join_swap_table = 'auto';
SET enable_join_runtime_filters = 0;
SET enable_parallel_replicas = 0;

CREATE TABLE ld_a (id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE ld_b (id UInt32, a_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE ld_c (id UInt32, b_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';

INSERT INTO ld_a SELECT number FROM numbers(4);
INSERT INTO ld_b SELECT number, number % 4 FROM numbers(8);
INSERT INTO ld_c SELECT number, number % 8 FROM numbers(16);

-- With limit=0 the query executes in the written order; count = 16.
SELECT count()
FROM ld_a a
JOIN ld_b b ON a.id = b.a_id
JOIN ld_c c ON b.id = c.b_id
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp',
         query_plan_optimize_join_order_limit = 0;

-- Greedy must agree.
SELECT count()
FROM ld_a a
JOIN ld_b b ON a.id = b.a_id
JOIN ld_c c ON b.id = c.b_id
SETTINGS query_plan_optimize_join_order_algorithm = 'greedy',
         query_plan_optimize_join_order_limit = 3;

DROP TABLE ld_a;
DROP TABLE ld_b;
DROP TABLE ld_c;
