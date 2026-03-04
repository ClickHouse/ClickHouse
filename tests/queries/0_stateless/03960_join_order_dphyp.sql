SET allow_experimental_analyzer = 1;
SET query_plan_optimize_join_order_limit = 10;
SET use_statistics = 1;
SET query_plan_join_swap_table='auto';
SET enable_join_runtime_filters = 0;

CREATE TABLE R1 (
    A_ID UInt32,
    A_Description String
) ENGINE = MergeTree()
PRIMARY KEY (A_ID)
SETTINGS auto_statistics_types = 'uniq';

CREATE TABLE R2 (
    B_ID UInt32,
    R1_A_ID UInt32,
    B_Data Float64
) ENGINE = MergeTree()
PRIMARY KEY (B_ID)
SETTINGS auto_statistics_types = 'uniq';

CREATE TABLE R3 (
    C_ID UInt32,
    R1_A_ID UInt32,
    R4_D_ID UInt32,
    C_Value Int32
) ENGINE = MergeTree()
PRIMARY KEY (C_ID)
SETTINGS auto_statistics_types = 'uniq';

CREATE TABLE R4 (
    D_ID UInt32,
    D_LookupCode String
) ENGINE = MergeTree()
PRIMARY KEY (D_ID)
SETTINGS auto_statistics_types = 'uniq';

INSERT INTO R1 (A_ID, A_Description) VALUES
(1, 'Type A'), (2, 'Type B'), (3, 'Type C'), (4, 'Type D'), (5, 'Type E'),
(6, 'Type F'), (7, 'Type G'), (8, 'Type H'), (9, 'Type I'), (10, 'Type J');

INSERT INTO R4 (D_ID, D_LookupCode) VALUES
(101, 'Lookup X'), (102, 'Lookup Y'), (103, 'Lookup Z'), (104, 'Lookup W'), (105, 'Lookup V'),
(106, 'Lookup U'), (107, 'Lookup T'), (108, 'Lookup S'), (109, 'Lookup R'), (110, 'Lookup Q');

INSERT INTO R2 (B_ID, R1_A_ID, B_Data)
SELECT
    number AS B_ID,
    (number % 10) + 1 AS R1_A_ID,
    number / 100
FROM numbers(1000);

INSERT INTO R3 (C_ID, R1_A_ID, R4_D_ID, C_Value)
SELECT
    number AS C_ID,
    (number % 10) + 1 AS R1_A_ID,
    (number % 10) + 101 AS R4_D_ID,
    (number * 10) AS C_Value
FROM numbers(1000);

SELECT '=========================================';
SELECT 'Plan with DPhyp algorithm';
EXPLAIN
SELECT
    T1.A_Description,
    T2.B_Data,
    T3.C_Value,
    T4.D_LookupCode
FROM R1 AS T1, R2 AS T2, R3 AS T3, R4 AS T4
WHERE
    T1.A_ID = T2.R1_A_ID
    AND T1.A_ID = T3.R1_A_ID
    AND T3.R4_D_ID = T4.D_ID
    AND T1.A_Description = 'Type H'
    AND T4.D_LookupCode = 'Lookup S'
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp', enable_parallel_replicas = 0;

-- DPhyp result must match greedy/dpsize
SELECT sum(sipHash64(
    T1.A_Description,
    T2.B_Data,
    T3.C_Value,
    T4.D_LookupCode))
FROM R1 AS T1, R2 AS T2, R3 AS T3, R4 AS T4
WHERE
    T1.A_ID = T2.R1_A_ID
    AND T1.A_ID = T3.R1_A_ID
    AND T3.R4_D_ID = T4.D_ID
    AND T1.A_Description = 'Type H'
    AND T4.D_LookupCode = 'Lookup S'
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp';

SELECT '=========================================';
SELECT 'Fallback to greedy for outer joins';

-- DPhyp alone fails on outer join
SELECT 1 FROM (SELECT 1 c0) t0 LEFT JOIN (SELECT 1 c0) t1 ON t0.c0 = t1.c0
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp', enable_parallel_replicas=0; --{serverError EXPERIMENTAL_FEATURE_ERROR}

-- dphyp,greedy succeeds
SELECT 1 FROM (SELECT 1 c0) t0 LEFT JOIN (SELECT 1 c0) t1 ON t0.c0 = t1.c0
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp,greedy', enable_parallel_replicas=0;

DROP TABLE R1;
DROP TABLE R2;
DROP TABLE R3;
DROP TABLE R4;

-- ===========================================================================
-- Test: Chain of 5 tables
-- Topology: T1 - T2 - T3 - T4 - T5
-- Exercises enumerateCmpRec to full depth: complement of {T1} must grow
-- through three recursive levels to reach {T5}.
-- ===========================================================================
SELECT '=========================================';
SELECT 'Test: Chain of 5 tables';

CREATE TABLE ch5_t1 (id UInt32, val UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE ch5_t2 (id UInt32, t1_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE ch5_t3 (id UInt32, t2_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE ch5_t4 (id UInt32, t3_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE ch5_t5 (id UInt32, t4_id UInt32, val UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';

INSERT INTO ch5_t1 SELECT number, number * 10 FROM numbers(10);
INSERT INTO ch5_t2 SELECT number, number % 10 FROM numbers(50);
INSERT INTO ch5_t3 SELECT number, number % 50 FROM numbers(100);
INSERT INTO ch5_t4 SELECT number, number % 100 FROM numbers(200);
INSERT INTO ch5_t5 SELECT number, number % 200, number * 7 FROM numbers(500);

SELECT sum(sipHash64(t1.val, t5.val))
FROM ch5_t1 t1
JOIN ch5_t2 t2 ON t1.id = t2.t1_id
JOIN ch5_t3 t3 ON t2.id = t3.t2_id
JOIN ch5_t4 t4 ON t3.id = t4.t3_id
JOIN ch5_t5 t5 ON t4.id = t5.t4_id
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp', enable_parallel_replicas = 0;

-- DPsize must produce the same result
SELECT sum(sipHash64(t1.val, t5.val))
FROM ch5_t1 t1
JOIN ch5_t2 t2 ON t1.id = t2.t1_id
JOIN ch5_t3 t3 ON t2.id = t3.t2_id
JOIN ch5_t4 t4 ON t3.id = t4.t3_id
JOIN ch5_t5 t5 ON t4.id = t5.t4_id
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsize', enable_parallel_replicas = 0;

DROP TABLE ch5_t1;
DROP TABLE ch5_t2;
DROP TABLE ch5_t3;
DROP TABLE ch5_t4;
DROP TABLE ch5_t5;

-- ===========================================================================
-- Test: Star topology — Hub with 5 spokes
-- Topology: Hub connected to S1..S5 (spokes connect only to Hub, not each other)
-- Exercises getNeighborhood on a high-degree center node.
-- Also exercises isConnectedInGraph: {S1,S2} is rejected as a CSG because
-- there is no direct predicate between S1 and S2.
-- ===========================================================================
SELECT '=========================================';
SELECT 'Test: Star topology';

CREATE TABLE st5_hub (id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE st5_s1  (id UInt32, hub_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE st5_s2  (id UInt32, hub_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE st5_s3  (id UInt32, hub_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE st5_s4  (id UInt32, hub_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE st5_s5  (id UInt32, hub_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';

INSERT INTO st5_hub SELECT number FROM numbers(100);
INSERT INTO st5_s1  SELECT number, number % 100 FROM numbers(10);
INSERT INTO st5_s2  SELECT number, number % 100 FROM numbers(30);
INSERT INTO st5_s3  SELECT number, number % 100 FROM numbers(60);
INSERT INTO st5_s4  SELECT number, number % 100 FROM numbers(80);
INSERT INTO st5_s5  SELECT number, number % 100 FROM numbers(100);

SELECT sum(sipHash64(s1.id, s2.id, s3.id, s4.id, s5.id))
FROM st5_hub hub
JOIN st5_s1 s1 ON hub.id = s1.hub_id
JOIN st5_s2 s2 ON hub.id = s2.hub_id
JOIN st5_s3 s3 ON hub.id = s3.hub_id
JOIN st5_s4 s4 ON hub.id = s4.hub_id
JOIN st5_s5 s5 ON hub.id = s5.hub_id
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp', enable_parallel_replicas = 0;

-- DPsize must produce the same result
SELECT sum(sipHash64(s1.id, s2.id, s3.id, s4.id, s5.id))
FROM st5_hub hub
JOIN st5_s1 s1 ON hub.id = s1.hub_id
JOIN st5_s2 s2 ON hub.id = s2.hub_id
JOIN st5_s3 s3 ON hub.id = s3.hub_id
JOIN st5_s4 s4 ON hub.id = s4.hub_id
JOIN st5_s5 s5 ON hub.id = s5.hub_id
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsize', enable_parallel_replicas = 0;

DROP TABLE st5_hub;
DROP TABLE st5_s1;
DROP TABLE st5_s2;
DROP TABLE st5_s3;
DROP TABLE st5_s4;
DROP TABLE st5_s5;

-- ===========================================================================
-- Test: Cycle topology — 4 tables forming a ring
-- Topology: A - B - C - D - A
-- Every 2-node set is a valid complement of another 2-node set.
-- Tests that emitCsgCmp does not emit duplicate pairs and that
-- isConnectedInGraph correctly handles the extra edge in a cycle.
-- ===========================================================================
SELECT '=========================================';
SELECT 'Test: Cycle topology';

-- A has a foreign key to D to close the cycle
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

-- DPsize must produce the same result
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

-- ===========================================================================
-- Test: Two chains with a bridge
-- Topology: L1 - L2 - L3
--                      |   (bridge predicate)
--                     R3 - R2 - R1
-- The optimal bushy plan (L1⋈L2⋈L3) ⋈ (R1⋈R2⋈R3) is found by DPhyp/DPsize.
-- All three algorithms must return the same query result.
-- ===========================================================================
SELECT '=========================================';
SELECT 'Test: Two chains with a bridge';

CREATE TABLE br_l1 (id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE br_l2 (id UInt32, l1_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE br_l3 (id UInt32, l2_id UInt32, bridge_key UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE br_r1 (id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE br_r2 (id UInt32, r1_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE br_r3 (id UInt32, r2_id UInt32, bridge_key UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';

INSERT INTO br_l1 SELECT number FROM numbers(1);
INSERT INTO br_l2 SELECT number, number % 1 FROM numbers(10);
INSERT INTO br_l3 SELECT number, number % 10, number % 100 FROM numbers(100);
INSERT INTO br_r1 SELECT number FROM numbers(1);
INSERT INTO br_r2 SELECT number, number % 1 FROM numbers(10);
INSERT INTO br_r3 SELECT number, number % 10, number % 100 FROM numbers(100);

SELECT sum(sipHash64(l1.id, l3.id, r1.id, r3.id))
FROM br_l1 l1
JOIN br_l2 l2 ON l1.id = l2.l1_id
JOIN br_l3 l3 ON l2.id = l3.l2_id
JOIN br_r3 r3 ON l3.bridge_key = r3.bridge_key
JOIN br_r2 r2 ON r3.id = r2.r1_id
JOIN br_r1 r1 ON r2.r1_id = r1.id
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp', enable_parallel_replicas = 0;

-- DPsize must produce the same result
SELECT sum(sipHash64(l1.id, l3.id, r1.id, r3.id))
FROM br_l1 l1
JOIN br_l2 l2 ON l1.id = l2.l1_id
JOIN br_l3 l3 ON l2.id = l3.l2_id
JOIN br_r3 r3 ON l3.bridge_key = r3.bridge_key
JOIN br_r2 r2 ON r3.id = r2.r1_id
JOIN br_r1 r1 ON r2.r1_id = r1.id
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsize', enable_parallel_replicas = 0;

-- Greedy must produce the same result (even if via a different plan)
SELECT sum(sipHash64(l1.id, l3.id, r1.id, r3.id))
FROM br_l1 l1
JOIN br_l2 l2 ON l1.id = l2.l1_id
JOIN br_l3 l3 ON l2.id = l3.l2_id
JOIN br_r3 r3 ON l3.bridge_key = r3.bridge_key
JOIN br_r2 r2 ON r3.id = r2.r1_id
JOIN br_r1 r1 ON r2.r1_id = r1.id
SETTINGS query_plan_optimize_join_order_algorithm = 'greedy', enable_parallel_replicas = 0;

DROP TABLE br_l1;
DROP TABLE br_l2;
DROP TABLE br_l3;
DROP TABLE br_r1;
DROP TABLE br_r2;
DROP TABLE br_r3;

-- ===========================================================================
-- Test: Chain of 10 tables (maximum limit)
-- DPhyp: O(N²) CSG-CP pairs for a chain; DPsize: O(3^N) = 59049 pairs.
-- Both must return the same result at the query_plan_optimize_join_order_limit boundary.
-- ===========================================================================
SELECT '=========================================';
SELECT 'Test: Chain of 10 tables';

CREATE TABLE ch10_t1  (id UInt32, val UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE ch10_t2  (id UInt32, prev_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE ch10_t3  (id UInt32, prev_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE ch10_t4  (id UInt32, prev_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE ch10_t5  (id UInt32, prev_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE ch10_t6  (id UInt32, prev_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE ch10_t7  (id UInt32, prev_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE ch10_t8  (id UInt32, prev_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE ch10_t9  (id UInt32, prev_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE ch10_t10 (id UInt32, prev_id UInt32, val UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';

INSERT INTO ch10_t1  SELECT number, number FROM numbers(5);
INSERT INTO ch10_t2  SELECT number, number % 5 FROM numbers(10);
INSERT INTO ch10_t3  SELECT number, number % 10 FROM numbers(20);
INSERT INTO ch10_t4  SELECT number, number % 20 FROM numbers(30);
INSERT INTO ch10_t5  SELECT number, number % 30 FROM numbers(40);
INSERT INTO ch10_t6  SELECT number, number % 40 FROM numbers(50);
INSERT INTO ch10_t7  SELECT number, number % 50 FROM numbers(60);
INSERT INTO ch10_t8  SELECT number, number % 60 FROM numbers(70);
INSERT INTO ch10_t9  SELECT number, number % 70 FROM numbers(80);
INSERT INTO ch10_t10 SELECT number, number % 80, number * 3 FROM numbers(100);

SELECT sum(sipHash64(t1.val, t10.val))
FROM ch10_t1 t1
JOIN ch10_t2  t2  ON t1.id  = t2.prev_id
JOIN ch10_t3  t3  ON t2.id  = t3.prev_id
JOIN ch10_t4  t4  ON t3.id  = t4.prev_id
JOIN ch10_t5  t5  ON t4.id  = t5.prev_id
JOIN ch10_t6  t6  ON t5.id  = t6.prev_id
JOIN ch10_t7  t7  ON t6.id  = t7.prev_id
JOIN ch10_t8  t8  ON t7.id  = t8.prev_id
JOIN ch10_t9  t9  ON t8.id  = t9.prev_id
JOIN ch10_t10 t10 ON t9.id  = t10.prev_id
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp', enable_parallel_replicas = 0;

-- DPsize must produce the same result
SELECT sum(sipHash64(t1.val, t10.val))
FROM ch10_t1 t1
JOIN ch10_t2  t2  ON t1.id  = t2.prev_id
JOIN ch10_t3  t3  ON t2.id  = t3.prev_id
JOIN ch10_t4  t4  ON t3.id  = t4.prev_id
JOIN ch10_t5  t5  ON t4.id  = t5.prev_id
JOIN ch10_t6  t6  ON t5.id  = t6.prev_id
JOIN ch10_t7  t7  ON t6.id  = t7.prev_id
JOIN ch10_t8  t8  ON t7.id  = t8.prev_id
JOIN ch10_t9  t9  ON t8.id  = t9.prev_id
JOIN ch10_t10 t10 ON t9.id  = t10.prev_id
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsize', enable_parallel_replicas = 0;

DROP TABLE ch10_t1;
DROP TABLE ch10_t2;
DROP TABLE ch10_t3;
DROP TABLE ch10_t4;
DROP TABLE ch10_t5;
DROP TABLE ch10_t6;
DROP TABLE ch10_t7;
DROP TABLE ch10_t8;
DROP TABLE ch10_t9;
DROP TABLE ch10_t10;

-- ===========================================================================
-- Test: Disconnected components — DPhyp fallback
-- Two independent 2-table joins with no predicate connecting them.
-- solveDPhyp returns nullptr; solve() throws EXPERIMENTAL_FEATURE_ERROR.
-- The dphyp,greedy fallback succeeds via a cross product.
-- ===========================================================================
SELECT '=========================================';
SELECT 'Test: Disconnected components';

CREATE TABLE dc_a (id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE dc_b (id UInt32, a_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE dc_c (id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE dc_d (id UInt32, c_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';

INSERT INTO dc_a SELECT number FROM numbers(3);
INSERT INTO dc_b SELECT number, number % 3 FROM numbers(6);
INSERT INTO dc_c SELECT number FROM numbers(4);
INSERT INTO dc_d SELECT number, number % 4 FROM numbers(8);

-- dphyp alone fails: no CSG covers all four relations
SELECT count() FROM dc_a a, dc_b b, dc_c c, dc_d d
WHERE a.id = b.a_id AND c.id = d.c_id
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp', enable_parallel_replicas = 0; --{serverError EXPERIMENTAL_FEATURE_ERROR}

-- dphyp,greedy succeeds: greedy falls back to a cross product between the components
SELECT count() FROM dc_a a, dc_b b, dc_c c, dc_d d
WHERE a.id = b.a_id AND c.id = d.c_id
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp,greedy', enable_parallel_replicas = 0;

DROP TABLE dc_a;
DROP TABLE dc_b;
DROP TABLE dc_c;
DROP TABLE dc_d;

-- ===========================================================================
-- Test: Range predicate — DPhyp fallback
-- A.val < B.val is a range predicate that does not form a join edge between A
-- and B in the query graph (it is applied as a filter, not an equijoin key).
-- This leaves A disconnected: solveDPhyp returns nullptr, and solve() throws
-- EXPERIMENTAL_FEATURE_ERROR when dphyp is used alone.
-- With dphyp,greedy the greedy algorithm handles the disconnected component
-- via a cross product and then applies the filter, producing the correct result.
-- ===========================================================================
SELECT '=========================================';
SELECT 'Test: Range predicate (DPhyp fallback)';

CREATE TABLE rng_a (id UInt32, val UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE rng_b (id UInt32, val UInt32, c_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE rng_c (id UInt32, val UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';

INSERT INTO rng_a SELECT number, number * 2 FROM numbers(10);
INSERT INTO rng_b SELECT number, number * 3, number % 5 FROM numbers(10);
INSERT INTO rng_c SELECT number, number * 5 FROM numbers(5);

-- dphyp alone fails: A has no equijoin connecting it to B or C
SELECT count()
FROM rng_a a, rng_b b, rng_c c
WHERE a.val < b.val AND b.c_id = c.id
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp', enable_parallel_replicas = 0; --{serverError EXPERIMENTAL_FEATURE_ERROR}

-- dphyp,greedy succeeds: greedy handles A as a disconnected component
SELECT count()
FROM rng_a a, rng_b b, rng_c c
WHERE a.val < b.val AND b.c_id = c.id
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp,greedy', enable_parallel_replicas = 0;

-- Greedy must produce the same result
SELECT count()
FROM rng_a a, rng_b b, rng_c c
WHERE a.val < b.val AND b.c_id = c.id
SETTINGS query_plan_optimize_join_order_algorithm = 'greedy', enable_parallel_replicas = 0;

DROP TABLE rng_a;
DROP TABLE rng_b;
DROP TABLE rng_c;
