-- Regression test for overly restrictive exclusion in complement enumeration.
--
-- Topology (4-cycle):  A - B - C - D - A
--
-- With CSG={A}, neighbourhood={B,D}. The buggy code excluded D when recursing
-- from B (and vice versa), so it could never discover the complement {B,C,D}.
-- The partition ({A}, {B,C,D}) is the optimal root-level split when A is tiny
-- and B,C,D form a chain.
--
-- We engineer cardinalities so that:
--   A: 1 row, B: 100 rows, C: 10 rows, D: 50 rows
-- The optimal plan joins B-C-D first (small intermediate results),
-- then joins with A last. DPhyp and DPsize must produce the same result.

SET allow_experimental_analyzer = 1;
SET query_plan_optimize_join_order_limit = 10;
SET use_statistics = 1;
SET query_plan_join_swap_table = 'auto';
SET enable_join_runtime_filters = 0;

CREATE TABLE cyc_a (id UInt32, d_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE cyc_b (id UInt32, a_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE cyc_c (id UInt32, b_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE cyc_d (id UInt32, c_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';

INSERT INTO cyc_a SELECT 0, 0;
INSERT INTO cyc_b SELECT number, 0 FROM numbers(100);
INSERT INTO cyc_c SELECT number, number % 100 FROM numbers(10);
INSERT INTO cyc_d SELECT number, number % 10 FROM numbers(50);

-- DPhyp must produce the same join order as DPsize.
EXPLAIN
SELECT sum(sipHash64(a.id, b.id, c.id, d.id))
FROM cyc_a a, cyc_b b, cyc_c c, cyc_d d
WHERE a.id = b.a_id
  AND b.id = c.b_id
  AND c.id = d.c_id
  AND d.id = a.d_id
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp', enable_parallel_replicas = 0;

EXPLAIN
SELECT sum(sipHash64(a.id, b.id, c.id, d.id))
FROM cyc_a a, cyc_b b, cyc_c c, cyc_d d
WHERE a.id = b.a_id
  AND b.id = c.b_id
  AND c.id = d.c_id
  AND d.id = a.d_id
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsize', enable_parallel_replicas = 0;

DROP TABLE cyc_a;
DROP TABLE cyc_b;
DROP TABLE cyc_c;
DROP TABLE cyc_d;
