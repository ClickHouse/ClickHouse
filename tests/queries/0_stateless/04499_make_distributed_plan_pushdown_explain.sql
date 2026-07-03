-- Tags: no-random-settings
-- Pins the shape of Expression/Filter pushdown into the per-shard plan of a
-- ReadFromRemotePlanStep placeholder under make_distributed_plan = 1.
-- EXPLAIN distributed=1 must show the Filter and Expression steps INSIDE the
-- shard plan (above ReadFromTable); the plain (initiator) plan must keep only
-- ReadFromRemote, i.e. it does not re-run the filter/projection.

SET enable_analyzer = 1;
SET make_distributed_plan = 1;
SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS mdp_pd_local;
DROP TABLE IF EXISTS mdp_pd_dist;

CREATE TABLE mdp_pd_local (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE mdp_pd_dist AS mdp_pd_local ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), mdp_pd_local);

SELECT '-- Filter and Expression pushed into the per-shard plan';
EXPLAIN distributed = 1 SELECT x * 2 AS y FROM mdp_pd_dist WHERE x % 3 = 1;

SELECT '-- Initiator plan keeps only ReadFromRemote';
EXPLAIN SELECT x * 2 AS y FROM mdp_pd_dist WHERE x % 3 = 1;

DROP TABLE mdp_pd_dist;
DROP TABLE mdp_pd_local;
