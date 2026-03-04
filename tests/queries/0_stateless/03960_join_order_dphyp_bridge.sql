-- Two chains with a bridge.
-- Topology: L1 - L2 - L3
--                      |   (bridge predicate)
--                     R3 - R2 - R1
-- The optimal bushy plan (L1JOINL2JOINL3) JOIN (R1JOINR2JOINR3) is found by DPhyp/DPsize.
-- All three algorithms must return the same query result.

SET allow_experimental_analyzer = 1;
SET query_plan_optimize_join_order_limit = 10;
SET use_statistics = 1;
SET query_plan_join_swap_table = 'auto';
SET enable_join_runtime_filters = 0;

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

-- DPsize must produce the same result.
SELECT sum(sipHash64(l1.id, l3.id, r1.id, r3.id))
FROM br_l1 l1
JOIN br_l2 l2 ON l1.id = l2.l1_id
JOIN br_l3 l3 ON l2.id = l3.l2_id
JOIN br_r3 r3 ON l3.bridge_key = r3.bridge_key
JOIN br_r2 r2 ON r3.id = r2.r1_id
JOIN br_r1 r1 ON r2.r1_id = r1.id
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsize', enable_parallel_replicas = 0;

-- Greedy must produce the same result.
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
