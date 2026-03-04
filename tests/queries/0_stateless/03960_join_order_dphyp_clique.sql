-- Clique topology: 4 tables, all 6 pairs connected.
-- emitCsgCmp must not emit duplicate (S,T) pairs despite every 2-node
-- subset having a direct edge.  isConnectedInGraph must accept all subsets.
-- count() = 3 because only rows where all four ids coincide survive.

SET allow_experimental_analyzer = 1;
SET use_statistics = 1;
SET query_plan_join_swap_table = 'auto';
SET enable_join_runtime_filters = 0;
SET enable_parallel_replicas = 0;

CREATE TABLE cl_a (id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE cl_b (id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE cl_c (id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE cl_d (id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';

INSERT INTO cl_a SELECT number FROM numbers(3);
INSERT INTO cl_b SELECT number FROM numbers(3);
INSERT INTO cl_c SELECT number FROM numbers(3);
INSERT INTO cl_d SELECT number FROM numbers(3);

SELECT count()
FROM cl_a a, cl_b b, cl_c c, cl_d d
WHERE a.id = b.id
  AND a.id = c.id
  AND a.id = d.id
  AND b.id = c.id
  AND b.id = d.id
  AND c.id = d.id
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp',
         query_plan_optimize_join_order_limit = 4;

-- DPsize must agree.
SELECT count()
FROM cl_a a, cl_b b, cl_c c, cl_d d
WHERE a.id = b.id
  AND a.id = c.id
  AND a.id = d.id
  AND b.id = c.id
  AND b.id = d.id
  AND c.id = d.id
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsize',
         query_plan_optimize_join_order_limit = 4;

DROP TABLE cl_a;
DROP TABLE cl_b;
DROP TABLE cl_c;
DROP TABLE cl_d;
