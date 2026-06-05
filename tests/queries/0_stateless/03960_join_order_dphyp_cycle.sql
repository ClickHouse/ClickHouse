-- Cycle topology: 4 tables forming a ring (A - B - C - D - A).
-- Every 2-node set is a valid complement of another 2-node set.
-- Tests that `emitCsgCmp` does not emit duplicate pairs and that
-- the extra edge closing the cycle is handled correctly.
-- DPhyp and DPsize must return the same result hash.

SET allow_experimental_analyzer = 1;
SET query_plan_optimize_join_order_limit = 10;
SET use_statistics = 1;
SET query_plan_join_swap_table = 'auto';
SET enable_join_runtime_filters = 0;

-- A has a foreign key to D to close the cycle.
CREATE TABLE cy4_a (id UInt32, d_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE cy4_b (id UInt32, a_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE cy4_c (id UInt32, b_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE cy4_d (id UInt32, c_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';

INSERT INTO cy4_a SELECT number, (10 - number) % 10 FROM numbers(10);
INSERT INTO cy4_b SELECT number, number % 10 FROM numbers(20);
INSERT INTO cy4_c SELECT number, number % 20 FROM numbers(30);
INSERT INTO cy4_d SELECT number, number % 30 FROM numbers(10);

SELECT sum(sipHash64(a.id, b.id, c.id, d.id))
FROM cy4_a a, cy4_b b, cy4_c c, cy4_d d
WHERE a.id = b.a_id
  AND b.id = c.b_id
  AND c.id = d.c_id
  AND d.id = a.d_id
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp', enable_parallel_replicas = 0;

-- DPsize must produce the same result.
SELECT sum(sipHash64(a.id, b.id, c.id, d.id))
FROM cy4_a a, cy4_b b, cy4_c c, cy4_d d
WHERE a.id = b.a_id
  AND b.id = c.b_id
  AND c.id = d.c_id
  AND d.id = a.d_id
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsize', enable_parallel_replicas = 0;

DROP TABLE cy4_a;
DROP TABLE cy4_b;
DROP TABLE cy4_c;
DROP TABLE cy4_d;

-- =====================================================================
-- Regression test for complement enumeration.
-- With CSG={A}, neighbourhood={B,D}. A buggy exclusion would prevent
-- discovering the complement {B,C,D}, so DPhyp would fail to build a plan
-- over all four relations. The EXPLAINs below confirm both algorithms
-- produce a complete 4-table plan that joins the B-C-D chain before the
-- single-row A. The B-C-D orderings have equal estimated cost, so DPhyp
-- and DPsize may pick different ones; each plan is recorded separately.
-- =====================================================================

CREATE TABLE cyc_a (id UInt32, d_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE cyc_b (id UInt32, a_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE cyc_c (id UInt32, b_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE cyc_d (id UInt32, c_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';

INSERT INTO cyc_a SELECT 0, 0;
INSERT INTO cyc_b SELECT number, 0 FROM numbers(100);
INSERT INTO cyc_c SELECT number, number % 100 FROM numbers(10);
INSERT INTO cyc_d SELECT number, number % 10 FROM numbers(50);

-- DPhyp builds a complete plan over all four relations (joins single-row A last).
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
