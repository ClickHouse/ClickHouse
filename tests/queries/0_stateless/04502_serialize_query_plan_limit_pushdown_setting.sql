-- Tags: no-random-settings
-- distributed_push_down_limit = 0 must disable the LIMIT copy-down into the per-shard
-- plan of a ReadFromRemotePlanStep placeholder (parity with the legacy path).

SET enable_analyzer = 1;
SET serialize_query_plan = 1;
SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS mdp_pdl_local;
DROP TABLE IF EXISTS mdp_pdl_dist;

CREATE TABLE mdp_pdl_local (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE mdp_pdl_dist AS mdp_pdl_local ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), mdp_pdl_local);

INSERT INTO mdp_pdl_local SELECT number FROM numbers(100);

SELECT '-- default (distributed_push_down_limit = 1): shard plans get a Limit copy';
EXPLAIN distributed = 1 SELECT x FROM mdp_pdl_dist LIMIT 5;

SELECT '-- distributed_push_down_limit = 0: no per-shard Limit';
EXPLAIN distributed = 1 SELECT x FROM mdp_pdl_dist LIMIT 5 SETTINGS distributed_push_down_limit = 0;

SELECT '-- correctness with the copy disabled';
SELECT count() FROM (SELECT x FROM mdp_pdl_dist LIMIT 5) SETTINGS distributed_push_down_limit = 0;

DROP TABLE mdp_pdl_dist;
DROP TABLE mdp_pdl_local;
