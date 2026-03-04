-- dphyp,dpsize,greedy - full fallback chain on a disconnected graph.
-- Two independent 2-table components: neither DPhyp nor DPsize can span all
-- four relations (no connected subgraph covers them all), so both return
-- nullptr.  Greedy succeeds via a cross product between the two components.
--
-- dphyp,dpsize alone must throw EXPERIMENTAL_FEATURE_ERROR.
-- dphyp,dpsize,greedy must succeed with count = 3 * 6 * (2 * 4) = 24.

SET allow_experimental_analyzer = 1;
SET use_statistics = 1;
SET query_plan_join_swap_table = 'auto';
SET enable_join_runtime_filters = 0;
SET enable_parallel_replicas = 0;

CREATE TABLE fc_a (id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE fc_b (id UInt32, a_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE fc_c (id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE fc_d (id UInt32, c_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';

INSERT INTO fc_a SELECT number FROM numbers(3);
INSERT INTO fc_b SELECT number, number % 3 FROM numbers(6);
INSERT INTO fc_c SELECT number FROM numbers(2);
INSERT INTO fc_d SELECT number, number % 2 FROM numbers(4);

-- Both DPhyp and DPsize fail on the disconnected graph.
SELECT count() FROM fc_a a, fc_b b, fc_c c, fc_d d
WHERE a.id = b.a_id AND c.id = d.c_id
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp,dpsize'; -- { serverError EXPERIMENTAL_FEATURE_ERROR }

-- Greedy succeeds as the final fallback.
SELECT count() FROM fc_a a, fc_b b, fc_c c, fc_d d
WHERE a.id = b.a_id AND c.id = d.c_id
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp,dpsize,greedy';

DROP TABLE fc_a;
DROP TABLE fc_b;
DROP TABLE fc_c;
DROP TABLE fc_d;
