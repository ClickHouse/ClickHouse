-- Regression tests for the make_distributed_plan guards over a Distributed table.
-- The initiator-side guard must not throw for query shapes the distributed plan
-- cannot push down (WITH TOTALS, ROLLUP); the forwarded-settings reset in
-- ClusterProxy must keep make_distributed_plan off on shards so legacy AST
-- fallback paths (shardNum, distributed_group_by_no_merge) do not throw
-- SUPPORT_IS_DISABLED.

SET enable_analyzer = 1;
SET make_distributed_plan = 1;

DROP TABLE IF EXISTS mdp_local;
DROP TABLE IF EXISTS mdp_dist;
DROP TABLE IF EXISTS mdp_dist2;

CREATE TABLE mdp_local (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE mdp_dist AS mdp_local ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), mdp_local);
CREATE TABLE mdp_dist2 AS mdp_local ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), mdp_dist);

INSERT INTO mdp_local SELECT number % 4 AS k, number AS v FROM numbers(20);

SELECT '-- plain aggregate through the new path';
SELECT count(), sum(v) FROM mdp_dist;

SELECT '-- WITH TOTALS (initiator guard)';
SELECT k, count() FROM mdp_dist GROUP BY k WITH TOTALS ORDER BY k;

SELECT '-- ROLLUP (initiator guard)';
SELECT k, count() AS c FROM mdp_dist GROUP BY k WITH ROLLUP ORDER BY k, c;

SELECT '-- shardNum in select list (legacy fallback + settings reset)';
SELECT shardNum() AS s, count() FROM mdp_dist GROUP BY s ORDER BY s;

SELECT '-- distributed_group_by_no_merge = 1 (legacy path)';
-- Each shard returns its own groups without merging; sort on the initiator for determinism.
SELECT k, c FROM (SELECT k, count() AS c FROM mdp_dist GROUP BY k) ORDER BY k, c SETTINGS distributed_group_by_no_merge = 1;

SELECT '-- distributed-over-distributed';
SELECT count(), sum(v) FROM mdp_dist2;

-- Regression: subquery-level SETTINGS make_distributed_plan = 1 while the outer query is at the
-- default 0. The subquery is planned with its own context (the placeholder is planted), but the
-- merged plan is optimized once with the outer settings — the placeholder must still be finalized
-- into a `ReadFromRemote` step instead of reaching pipeline building (LOGICAL_ERROR).
SELECT '-- subquery-level SETTINGS make_distributed_plan = 1 with the outer query at default';
SET make_distributed_plan = 0;
SELECT * FROM (SELECT count(), sum(v) FROM mdp_dist SETTINGS make_distributed_plan = 1);
SELECT c + 1, sv FROM (SELECT count() AS c, sum(v) AS sv FROM mdp_dist SETTINGS make_distributed_plan = 1);

SELECT '-- the same through a view';
DROP VIEW IF EXISTS mdp_view;
CREATE VIEW mdp_view AS SELECT count() AS c, sum(v) AS sv FROM mdp_dist SETTINGS make_distributed_plan = 1;
SELECT * FROM mdp_view;
DROP VIEW mdp_view;
SET make_distributed_plan = 1;

DROP TABLE mdp_dist2;
DROP TABLE mdp_dist;
DROP TABLE mdp_local;
