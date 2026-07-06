-- Regression tests for the plan-level distributed read over a Distributed table, gated on
-- serialize_query_plan. Plain cases pin the new path with no MPP machinery involved. The cases
-- that additionally set make_distributed_plan = 1 pin the MPP-interaction guards: the
-- initiator-side MPP conversion must be skipped for plans that already read from remote shards
-- (WITH TOTALS, ROLLUP), and the forwarded-settings reset in ClusterProxy must keep
-- make_distributed_plan off on shards so legacy fallback paths (shardNum,
-- distributed_group_by_no_merge) do not throw SUPPORT_IS_DISABLED.

SET enable_analyzer = 1;
SET serialize_query_plan = 1;

DROP TABLE IF EXISTS mdp_local;
DROP TABLE IF EXISTS mdp_dist;
DROP TABLE IF EXISTS mdp_dist2;

CREATE TABLE mdp_local (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE mdp_dist AS mdp_local ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), mdp_local);
CREATE TABLE mdp_dist2 AS mdp_local ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), mdp_dist);

INSERT INTO mdp_local SELECT number % 4 AS k, number AS v FROM numbers(20);

SELECT '-- plain aggregate through the new path';
SELECT count(), sum(v) FROM mdp_dist;

SELECT '-- WITH TOTALS through the new path (no MPP machinery)';
SELECT k, count() FROM mdp_dist GROUP BY k WITH TOTALS ORDER BY k;

SELECT '-- WITH TOTALS with make_distributed_plan = 1 too (initiator MPP guard)';
SELECT k, count() FROM mdp_dist GROUP BY k WITH TOTALS ORDER BY k SETTINGS make_distributed_plan = 1;

SELECT '-- ROLLUP with make_distributed_plan = 1 too (initiator MPP guard)';
SELECT k, count() AS c FROM mdp_dist GROUP BY k WITH ROLLUP ORDER BY k, c SETTINGS make_distributed_plan = 1;

SELECT '-- shardNum in select list (fallback to the legacy read path)';
SELECT shardNum() AS s, count() FROM mdp_dist GROUP BY s ORDER BY s;

SELECT '-- shardNum with make_distributed_plan = 1 too (legacy fallback + shard-side settings reset)';
SELECT shardNum() AS s, count() FROM mdp_dist GROUP BY s ORDER BY s SETTINGS make_distributed_plan = 1;

SELECT '-- distributed_group_by_no_merge = 1 (legacy path)';
-- Each shard returns its own groups without merging; sort on the initiator for determinism.
SELECT k, c FROM (SELECT k, count() AS c FROM mdp_dist GROUP BY k) ORDER BY k, c SETTINGS distributed_group_by_no_merge = 1;

SELECT '-- distributed_group_by_no_merge = 1 with make_distributed_plan = 1 too (settings reset)';
SELECT k, c FROM (SELECT k, count() AS c FROM mdp_dist GROUP BY k) ORDER BY k, c SETTINGS distributed_group_by_no_merge = 1, make_distributed_plan = 1;

SELECT '-- distributed-over-distributed';
SELECT count(), sum(v) FROM mdp_dist2;

SELECT '-- distributed-over-distributed with make_distributed_plan = 1 too';
SELECT count(), sum(v) FROM mdp_dist2 SETTINGS make_distributed_plan = 1;

-- Regression: subquery-level SETTINGS serialize_query_plan = 1 while the outer query is at the
-- default 0. The subquery is planned with its own context (the placeholder is planted), but the
-- merged plan is optimized once with the outer settings — the placeholder must still be finalized
-- into a `ReadFromRemote` step instead of reaching pipeline building (LOGICAL_ERROR).
SELECT '-- subquery-level SETTINGS serialize_query_plan = 1 with the outer query at default';
SET serialize_query_plan = 0;
SELECT * FROM (SELECT count(), sum(v) FROM mdp_dist SETTINGS serialize_query_plan = 1);
SELECT c + 1, sv FROM (SELECT count() AS c, sum(v) AS sv FROM mdp_dist SETTINGS serialize_query_plan = 1);

SELECT '-- the same through a view';
DROP VIEW IF EXISTS mdp_view;
CREATE VIEW mdp_view AS SELECT count() AS c, sum(v) AS sv FROM mdp_dist SETTINGS serialize_query_plan = 1;
SELECT * FROM mdp_view;
DROP VIEW mdp_view;

DROP TABLE mdp_dist2;
DROP TABLE mdp_dist;
DROP TABLE mdp_local;
