-- Tags: no-random-settings
-- Pins the LIMIT copy-down into the per-shard plan of a ReadFromRemotePlanStep
-- placeholder under make_distributed_plan = 1. The outer LimitStep must stay on
-- the initiator (a per-shard LIMIT is not a global LIMIT) while each shard plan
-- gets a copy with limit = limit + offset and offset = 0.

SET enable_analyzer = 1;
SET make_distributed_plan = 1;
SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS mdp_lim_local;
DROP TABLE IF EXISTS mdp_lim_dist;

CREATE TABLE mdp_lim_local (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE mdp_lim_dist AS mdp_lim_local ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), mdp_lim_local);

INSERT INTO mdp_lim_local SELECT number FROM numbers(100);

SELECT '-- Limit copied into the per-shard plan: shard Limit 10 (= 7 + 3), initiator keeps Limit 7 Offset 3';
EXPLAIN actions = 1, distributed = 1 SELECT x FROM mdp_lim_dist LIMIT 7 OFFSET 3;

SELECT '-- WITH TIES is not copied down (per-shard ties are not composable)';
EXPLAIN distributed = 1 SELECT x FROM mdp_lim_dist ORDER BY x LIMIT 3 WITH TIES;

SELECT '-- exact_rows_before_limit keeps the shard reading everything (no per-shard Limit)';
EXPLAIN distributed = 1 SELECT x FROM mdp_lim_dist LIMIT 7 SETTINGS exact_rows_before_limit = 1;

SELECT '-- Correctness: LIMIT with OFFSET returns the same number of rows as without the distributed plan';
SELECT count() FROM (SELECT x FROM mdp_lim_dist LIMIT 7 OFFSET 3);
SELECT count() FROM (SELECT x FROM mdp_lim_dist LIMIT 7 OFFSET 3) SETTINGS make_distributed_plan = 0;

SELECT '-- Correctness: ORDER BY with LIMIT returns identical rows';
SELECT groupArray(x) FROM (SELECT x FROM mdp_lim_dist ORDER BY x LIMIT 7);
SELECT groupArray(x) FROM (SELECT x FROM mdp_lim_dist ORDER BY x LIMIT 7) SETTINGS make_distributed_plan = 0;

SELECT '-- Correctness: LIMIT larger than the data set';
SELECT count(), sum(x) FROM (SELECT x FROM mdp_lim_dist LIMIT 1000000);

SELECT '-- Correctness: exact rows_before_limit_at_least is preserved (200 = both shards read fully)';
SELECT 1 FROM mdp_lim_dist LIMIT 7 SETTINGS exact_rows_before_limit = 1, output_format_write_statistics = 0 FORMAT JSONCompact;

DROP TABLE mdp_lim_dist;
DROP TABLE mdp_lim_local;
