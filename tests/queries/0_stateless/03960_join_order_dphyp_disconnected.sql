-- Disconnected components: two independent 2-table joins with no predicate
-- connecting them.  solveDPhyp returns nullptr; solve() throws
-- EXPERIMENTAL_FEATURE_ERROR.  The dphyp,greedy fallback succeeds via a
-- cross product between the components.

SET allow_experimental_analyzer = 1;
SET query_plan_optimize_join_order_limit = 10;
SET use_statistics = 1;
SET query_plan_join_swap_table = 'auto';
SET enable_join_runtime_filters = 0;

CREATE TABLE dc_a (id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE dc_b (id UInt32, a_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE dc_c (id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE dc_d (id UInt32, c_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';

INSERT INTO dc_a SELECT number FROM numbers(3);
INSERT INTO dc_b SELECT number, number % 3 FROM numbers(6);
INSERT INTO dc_c SELECT number FROM numbers(4);
INSERT INTO dc_d SELECT number, number % 4 FROM numbers(8);

-- dphyp alone fails: no CSG covers all four relations.
SELECT count() FROM dc_a a, dc_b b, dc_c c, dc_d d
WHERE a.id = b.a_id AND c.id = d.c_id
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp', enable_parallel_replicas = 0; -- { serverError EXPERIMENTAL_FEATURE_ERROR }

-- dphyp,greedy succeeds: greedy falls back to a cross product between the components.
SELECT count() FROM dc_a a, dc_b b, dc_c c, dc_d d
WHERE a.id = b.a_id AND c.id = d.c_id
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp,greedy', enable_parallel_replicas = 0;

DROP TABLE dc_a;
DROP TABLE dc_b;
DROP TABLE dc_c;
DROP TABLE dc_d;
