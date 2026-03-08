-- Regression test for emitCsg bug: complement neighborhood enumeration on tree topologies.
--
-- Bug: emitCsg called enumerateCmpRec once with the entire neighborhood as the complement
-- instead of iterating over each individual neighbour node. This caused DPhyp to miss
-- valid CSG-CP pairs like ({hub}, {la, ll}) on tree topologies, producing suboptimal plans.
--
-- Topology (node indices in parentheses):
--
--   ll(3) -- la(1) -- hub(0) -- ra(2) -- rl(4)
--
-- la and ra are large (10 000 rows); ll and rl are tiny (1 row each).
-- The optimal intermediate split for {hub, la, ll} is ({hub}, {la JOIN ll}):
--   la JOIN ll  -> 1 row  ->  hub JOIN 1 row  (cheap)
-- The suboptimal split found by the buggy code is ({hub, la}, {ll}):
--   hub JOIN la -> 10 000 rows  ->  result JOIN ll  (expensive)
--
-- After the fix both algorithms must pick the same optimal join order.

SET allow_experimental_analyzer = 1;
SET use_statistics = 1;
SET query_plan_join_swap_table = 'auto';
SET enable_join_runtime_filters = 0;
SET query_plan_optimize_join_order_limit = 10;

CREATE TABLE tree_hub (id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE tree_la  (id UInt32, hub_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE tree_ra  (id UInt32, hub_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE tree_ll  (id UInt32, la_id  UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE tree_rl  (id UInt32, ra_id  UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';

INSERT INTO tree_hub SELECT number FROM numbers(100);
INSERT INTO tree_la  SELECT number, number % 100 FROM numbers(10000);
INSERT INTO tree_ra  SELECT number, number % 100 FROM numbers(10000);
INSERT INTO tree_ll  SELECT 0, 0;
INSERT INTO tree_rl  SELECT 0, 0;

-- DPhyp must find the same optimal plan as DPsize:
--   hub JOIN (la JOIN ll)  on the left branch  (not the suboptimal (la JOIN hub) JOIN ll)
--   ra  JOIN rl            on the right branch
EXPLAIN
SELECT hub.id, la.id, ra.id, ll.id, rl.id
FROM tree_hub hub, tree_la la, tree_ra ra, tree_ll ll, tree_rl rl
WHERE hub.id = la.hub_id
  AND hub.id = ra.hub_id
  AND la.id  = ll.la_id
  AND ra.id  = rl.ra_id
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp', enable_parallel_replicas = 0;

-- Correctness check: result must match DPsize regardless of join order.
SELECT sum(sipHash64(hub.id, la.id, ra.id, ll.id, rl.id))
FROM tree_hub hub, tree_la la, tree_ra ra, tree_ll ll, tree_rl rl
WHERE hub.id = la.hub_id
  AND hub.id = ra.hub_id
  AND la.id  = ll.la_id
  AND ra.id  = rl.ra_id
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp', enable_parallel_replicas = 0;

SELECT sum(sipHash64(hub.id, la.id, ra.id, ll.id, rl.id))
FROM tree_hub hub, tree_la la, tree_ra ra, tree_ll ll, tree_rl rl
WHERE hub.id = la.hub_id
  AND hub.id = ra.hub_id
  AND la.id  = ll.la_id
  AND ra.id  = rl.ra_id
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsize', enable_parallel_replicas = 0;

DROP TABLE tree_hub;
DROP TABLE tree_la;
DROP TABLE tree_ra;
DROP TABLE tree_ll;
DROP TABLE tree_rl;
