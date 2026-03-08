-- Triangle topology: 3 tables, all 3 pairs connected (hub–arm1, hub–arm2, arm1–arm2).
--
-- This topology tests the "all-subsets direct emit" enhancement in emitCsg.
--
-- The DPhyp paper (Moerkotte & Neumann, Algorithm EmitCsg) emits only
-- individual neighbour nodes {v} as direct complement seeds, then calls
-- EnumerateCmpRec for out-of-neighbourhood extensions. For a triangle
-- with CSG = {hub} and N = {arm1, arm2}, the paper emits {arm1} and
-- {arm2} directly, then calls EnumerateCmpRec({hub}, {arm1},
-- excl = {hub, arm1, arm2}). Since arm2 is already in the exclusion,
-- it is never added to the complement starting from arm1 alone, so the
-- pair ({hub}, {arm1, arm2}) would be missed by the strict paper
-- algorithm.
--
-- Our implementation emits every non-empty SUBSET of N as a direct seed
-- via forEachNonEmptySubset, which immediately finds ({hub}, {arm1, arm2})
-- and allows the optimizer to consider the bushy split. Both DPhyp and
-- DPsize must explore the same set of valid CSG-CP pairs and therefore
-- produce identical result sets.
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

-- DPhyp must find the same optimal plan as DPsize (bushy: hub JOIN (arm1 JOIN arm2)).
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
