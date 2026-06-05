-- Triangle topology: 3 tables, all 3 pairs connected (hub-arm1, hub-arm2, arm1-arm2).
-- This is the smallest clique. With CSG = {hub} and neighbourhood {arm1, arm2}, DPhyp must
-- enumerate the complement {arm1, arm2} (grown from a single neighbour seed) so that every valid
-- CSG-CMP pair is considered. DPhyp and DPsize explore the same set of pairs and must produce
-- identical results, which the result-hash checks below verify.
--
-- All three joins have equal estimated cost on this data (any plan scans `hub` once), so the chosen
-- order is one of several equal-cost plans; the EXPLAIN pins the plan currently produced rather than
-- a uniquely-optimal one.
--
-- Topology:
--   arm1 (10 rows) -- hub (10 000 rows) -- arm2 (10 rows)
--        \______________________________________/
--                    (arm1 -- arm2 edge)

SET allow_experimental_analyzer = 1;
SET use_statistics = 1;
SET query_plan_join_swap_table = 'auto';
SET enable_join_runtime_filters = 0;
SET query_plan_optimize_join_order_limit = 10;

CREATE TABLE tri_hub  (id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE tri_arm1 (id UInt32, hub_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE tri_arm2 (id UInt32, hub_id UInt32, a1_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';

INSERT INTO tri_hub  SELECT number FROM numbers(10000);
INSERT INTO tri_arm1 SELECT number, number FROM numbers(10);
INSERT INTO tri_arm2 SELECT number, number, number FROM numbers(10);

-- EXPLAIN of the (equal-cost) plan DPhyp produces on the triangle.
EXPLAIN
SELECT hub.id, a1.id, a2.id
FROM tri_hub hub, tri_arm1 a1, tri_arm2 a2
WHERE hub.id = a1.hub_id
  AND hub.id = a2.hub_id
  AND a1.id  = a2.a1_id
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp', enable_parallel_replicas = 0;

-- Correctness: result must match DPsize regardless of join order.
SELECT sum(sipHash64(hub.id, a1.id, a2.id))
FROM tri_hub hub, tri_arm1 a1, tri_arm2 a2
WHERE hub.id = a1.hub_id
  AND hub.id = a2.hub_id
  AND a1.id  = a2.a1_id
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp', enable_parallel_replicas = 0;

SELECT sum(sipHash64(hub.id, a1.id, a2.id))
FROM tri_hub hub, tri_arm1 a1, tri_arm2 a2
WHERE hub.id = a1.hub_id
  AND hub.id = a2.hub_id
  AND a1.id  = a2.a1_id
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsize', enable_parallel_replicas = 0;

DROP TABLE tri_hub;
DROP TABLE tri_arm1;
DROP TABLE tri_arm2;
