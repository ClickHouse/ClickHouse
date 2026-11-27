-- Edge cases for `query_plan_optimize_join_order_limit` setting.

SET allow_experimental_analyzer = 1;
SET enable_parallel_replicas = 0;

-- =====================================================================
-- 1. limit > 64 throws INVALID_SETTING_VALUE (BitSet is 64-bit).
-- =====================================================================

CREATE TABLE lim_a (id UInt32) ENGINE = MergeTree() PRIMARY KEY id;
CREATE TABLE lim_b (id UInt32, a_id UInt32) ENGINE = MergeTree() PRIMARY KEY id;
INSERT INTO lim_a VALUES (1);
INSERT INTO lim_b VALUES (1, 1);

SELECT count() FROM lim_a a JOIN lim_b b ON a.id = b.a_id
SETTINGS query_plan_optimize_join_order_limit = 65; -- { serverError INVALID_SETTING_VALUE }

DROP TABLE lim_a;
DROP TABLE lim_b;

-- =====================================================================
-- 2. limit = 0 disables optimization; original join order is preserved.
-- =====================================================================

SET use_statistics = 1;
SET query_plan_join_swap_table = 'auto';
SET enable_join_runtime_filters = 0;

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
