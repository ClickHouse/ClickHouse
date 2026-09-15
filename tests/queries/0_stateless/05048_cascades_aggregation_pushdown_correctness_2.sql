-- Tags: no-old-analyzer
-- no-old-analyzer: distributed planning requires the analyzer.

-- Every scenario runs twice: through the cascades optimizer with `cascades_aggregation_pushdown`
-- steered towards the pushed plan by stat hints, and classically (`enable_cascades_optimizer = 0`,
-- `make_distributed_plan = 0`). The two result blocks of each scenario must be identical.
-- Scenarios 1-10 live in `04927_cascades_aggregation_pushdown_correctness`: the scenario list
-- is split in two so each half fits the flaky-check time budget.

DROP TABLE IF EXISTS t_corr_left;
DROP TABLE IF EXISTS t_corr_right_multi;
DROP TABLE IF EXISTS t_corr_right_uniq;
DROP TABLE IF EXISTS t_corr_empty;
DROP TABLE IF EXISTS t_corr_empty_right;

CREATE TABLE t_corr_left (k UInt32, p UInt32, v Int64, big UInt8) ENGINE = MergeTree ORDER BY k
  SETTINGS auto_statistics_types = '';
CREATE TABLE t_corr_right_multi (k UInt32, t Int64) ENGINE = MergeTree ORDER BY k
  SETTINGS auto_statistics_types = '';
CREATE TABLE t_corr_right_uniq (k UInt32, name String) ENGINE = MergeTree ORDER BY k
  SETTINGS auto_statistics_types = '';
CREATE TABLE t_corr_empty (k UInt32, v Int64) ENGINE = MergeTree ORDER BY k
  SETTINGS auto_statistics_types = '';
CREATE TABLE t_corr_empty_right (k UInt32, w Int64) ENGINE = MergeTree ORDER BY k
  SETTINGS auto_statistics_types = '';
-- a merge between planning and the worker read would invalidate the planned part names
SYSTEM STOP MERGES t_corr_left;
SYSTEM STOP MERGES t_corr_right_multi;
SYSTEM STOP MERGES t_corr_right_uniq;

-- 10 rows per key k = 0..9: v = k + 10*i for i = 0..9, so per key
-- count = 10, sum(v) = 450 + 10*k, min(v) = k, avg(v) = 45 + k, uniqExact(v) = 10,
-- countIf(big) = 5 (v >= 50 holds for i = 5..9); p = k % 2.
INSERT INTO t_corr_left SELECT number % 10, number % 2, number, number >= 50 FROM numbers(100);
-- fan-out: key 0 once (t=50), key 1 twice (30, 70), key 2 three times (10, 40, 90),
-- keys 3, 4, 5 once (55, 5, 95); keys 6-9 absent
INSERT INTO t_corr_right_multi VALUES (0, 50), (1, 30), (1, 70), (2, 10), (2, 40), (2, 90), (3, 55), (4, 5), (5, 95);
-- unique per key, keys 0-7
INSERT INTO t_corr_right_uniq SELECT number, concat('n_', toString(number)) FROM numbers(8);

SET make_distributed_plan = 1;
SET enable_cascades_optimizer = 1;
SET distributed_plan_execute_locally = 1;
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET max_rows_to_group_by = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET param__internal_cascades_cluster_node_count = 4;
-- The `t_corr_left` hint is kept identical to the first half's: the `p`, `big`, `v` NDVs are
-- pushed keys only in the first half's cases 3, 8, 9 and are inert here.
SET param__internal_join_table_stat_hints = '{"t_corr_left": {"cardinality": 100000000, "avg_row_bytes": 20, "distinct_keys": {"k": 100, "v": 1000, "p": 2, "big": 2}}, "t_corr_empty": {"cardinality": 100000000, "avg_row_bytes": 12, "distinct_keys": {"k": 100}}, "t_corr_right_multi": {"cardinality": 1000, "avg_row_bytes": 20, "distinct_keys": {"k": 1000}}, "t_corr_right_uniq": {"cardinality": 1000, "avg_row_bytes": 20, "distinct_keys": {"k": 1000}}, "t_corr_empty_right": {"cardinality": 1000, "avg_row_bytes": 12, "distinct_keys": {"k": 1000}}}';

-- Canaries: prove the stat hints actually steer the optimizer to the pushed shapes; otherwise
-- every on/off pair below would compare classic-vs-classic while staying green. The full legacy
-- EXPLAIN output is pinned: variant A = the merge-only `Aggregating` above the join and the
-- partial `Aggregating` below it, variant B = the whole aggregation below the join (nothing
-- but its own distribution split above it). The runtime-filter and prewhere settings, randomized
-- by the harness, decide the `BuildRuntimeFilter`/`Filter` lines of the INNER shape, so they
-- are pinned in the EXPLAIN's SETTINGS clause only - a session-level `SET` would leak into the
-- executed scenarios below.
SELECT '-- canary: variant A (partial pushdown) fires for case 1''s query';
EXPLAIN SELECT t1.k AS k, count() AS c, sum(t1.v) AS s FROM t_corr_left AS t1 INNER JOIN t_corr_right_multi AS t2 ON t1.k = t2.k GROUP BY t1.k ORDER BY k
SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1, explain_query_plan_default = 'legacy',
    enable_join_runtime_filters = 1, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;

SELECT '-- canary: variant B (full pushdown) fires for case 13''s query (single Aggregating, below the join)';
EXPLAIN SELECT t1.k AS k, count() AS c, sum(t1.v) AS s FROM t_corr_left AS t1 LEFT ANY JOIN t_corr_right_multi AS t2 ON t1.k = t2.k GROUP BY t1.k ORDER BY k
SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1, explain_query_plan_default = 'legacy';

SELECT '-- 11. empty left table';
SELECT t1.k AS k, count() AS c FROM t_corr_empty AS t1 INNER JOIN t_corr_right_uniq AS t2 ON t1.k = t2.k GROUP BY t1.k ORDER BY k;
SELECT t1.k AS k, count() AS c FROM t_corr_empty AS t1 INNER JOIN t_corr_right_uniq AS t2 ON t1.k = t2.k GROUP BY t1.k ORDER BY k
SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

SELECT '-- 12. empty right table with LEFT JOIN';
SELECT t1.k AS k, count() AS c, sum(t1.v) AS s FROM t_corr_left AS t1 LEFT JOIN t_corr_empty_right AS t2 ON t1.k = t2.k GROUP BY t1.k ORDER BY k;
SELECT t1.k AS k, count() AS c, sum(t1.v) AS s FROM t_corr_left AS t1 LEFT JOIN t_corr_empty_right AS t2 ON t1.k = t2.k GROUP BY t1.k ORDER BY k
SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

SELECT '-- 13. LEFT ANY, variant B (join keys subset of GROUP BY keys)';
SELECT t1.k AS k, count() AS c, sum(t1.v) AS s FROM t_corr_left AS t1 LEFT ANY JOIN t_corr_right_multi AS t2 ON t1.k = t2.k GROUP BY t1.k ORDER BY k;
SELECT t1.k AS k, count() AS c, sum(t1.v) AS s FROM t_corr_left AS t1 LEFT ANY JOIN t_corr_right_multi AS t2 ON t1.k = t2.k GROUP BY t1.k ORDER BY k
SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

-- deliberate never-pushed negative and semantics tripwire: `Inner` + `Any` is excluded from the
-- matrix (see `isPushdownAllowed`) because `setUsedOnce` claims the join key for the first
-- matching probe row, so the classic plan emits one row per KEY, not per pushed row; if this
-- combination were ever wrongly enabled, the pushed/classic pair below would mismatch.
SELECT '-- 14. INNER ANY is never pushed (at most one row per key, not per pushed row)';
-- Single node: `ANY`'s `setUsedOnce` dedups per node, not globally, so with more than one node
-- a key whose rows straddle a `ParallelReadImplementation` bucket boundary can be emitted once
-- per node under a randomized `index_granularity`/insert split, independent of this rule. The
-- tripwire still works single-node (a wrongly-pushed partial would still yield 10 vs 1).
SET param__internal_cascades_cluster_node_count = 1;
SELECT t1.k AS k, count() AS c FROM t_corr_left AS t1 INNER ANY JOIN t_corr_right_uniq AS t2 ON t1.k = t2.k GROUP BY t1.k ORDER BY k;
SET param__internal_cascades_cluster_node_count = 4;
SELECT t1.k AS k, count() AS c FROM t_corr_left AS t1 INNER ANY JOIN t_corr_right_uniq AS t2 ON t1.k = t2.k GROUP BY t1.k ORDER BY k
SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

SELECT '-- 15. LEFT SEMI, variant B';
SELECT t1.k AS k, count() AS c, sum(t1.v) AS s FROM t_corr_left AS t1 LEFT SEMI JOIN t_corr_right_multi AS t2 ON t1.k = t2.k GROUP BY t1.k ORDER BY k;
SELECT t1.k AS k, count() AS c, sum(t1.v) AS s FROM t_corr_left AS t1 LEFT SEMI JOIN t_corr_right_multi AS t2 ON t1.k = t2.k GROUP BY t1.k ORDER BY k
SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

SELECT '-- 16. LEFT ANTI, variant B';
SELECT t1.k AS k, count() AS c, sum(t1.v) AS s FROM t_corr_left AS t1 LEFT ANTI JOIN t_corr_right_multi AS t2 ON t1.k = t2.k GROUP BY t1.k ORDER BY k;
SELECT t1.k AS k, count() AS c, sum(t1.v) AS s FROM t_corr_left AS t1 LEFT ANTI JOIN t_corr_right_multi AS t2 ON t1.k = t2.k GROUP BY t1.k ORDER BY k
SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

SELECT '-- 17. RIGHT ALL with fan-out (variant A, push-right)';
SELECT t2.k AS k, count() AS c, sum(t2.v) AS s FROM t_corr_right_multi AS t1 RIGHT JOIN t_corr_left AS t2 ON t1.k = t2.k GROUP BY t2.k ORDER BY k;
SELECT t2.k AS k, count() AS c, sum(t2.v) AS s FROM t_corr_right_multi AS t1 RIGHT JOIN t_corr_left AS t2 ON t1.k = t2.k GROUP BY t2.k ORDER BY k
SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

SELECT '-- 18. RIGHT ANY, variant B (per-key-unique left side)';
SELECT t2.k AS k, count() AS c, sum(t2.v) AS s FROM t_corr_right_uniq AS t1 RIGHT ANY JOIN t_corr_left AS t2 ON t1.k = t2.k GROUP BY t2.k ORDER BY k;
SELECT t2.k AS k, count() AS c, sum(t2.v) AS s FROM t_corr_right_uniq AS t1 RIGHT ANY JOIN t_corr_left AS t2 ON t1.k = t2.k GROUP BY t2.k ORDER BY k
SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

SELECT '-- 19. RIGHT ANTI, variant B';
SELECT t2.k AS k, count() AS c FROM t_corr_right_multi AS t1 RIGHT ANTI JOIN t_corr_left AS t2 ON t1.k = t2.k GROUP BY t2.k ORDER BY k;
SELECT t2.k AS k, count() AS c FROM t_corr_right_multi AS t1 RIGHT ANTI JOIN t_corr_left AS t2 ON t1.k = t2.k GROUP BY t2.k ORDER BY k
SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

SELECT '-- 20. RIGHT SEMI, variant B, push-right (last enabled matrix cell without an executed scenario)';
SELECT t2.k AS k, count() AS c, sum(t2.v) AS s FROM t_corr_right_multi AS t1 RIGHT SEMI JOIN t_corr_left AS t2 ON t1.k = t2.k GROUP BY t2.k ORDER BY k;
SELECT t2.k AS k, count() AS c, sum(t2.v) AS s FROM t_corr_right_multi AS t1 RIGHT SEMI JOIN t_corr_left AS t2 ON t1.k = t2.k GROUP BY t2.k ORDER BY k
SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

SELECT '-- 21. global aggregation over a join on an empty left table keeps the single-row result';
SELECT count() FROM t_corr_empty AS t1 LEFT JOIN t_corr_right_uniq AS t2 ON t1.k = t2.k;
SELECT count() FROM t_corr_empty AS t1 LEFT JOIN t_corr_right_uniq AS t2 ON t1.k = t2.k
SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

-- keys 0-5 of t_corr_right_multi match; keys 6-9 of t_corr_left have no match and are dropped by
-- the INNER JOIN; GROUP BY dedups the fan-out (keys 1 and 2 match more than once) to one row/key.
SELECT '-- 22. keys-only GROUP BY, empty aggregate list (variant A, push-left)';
SELECT t1.k AS k FROM t_corr_left AS t1 INNER JOIN t_corr_right_multi AS t2 ON t1.k = t2.k GROUP BY t1.k ORDER BY k;
SELECT t1.k AS k FROM t_corr_left AS t1 INNER JOIN t_corr_right_multi AS t2 ON t1.k = t2.k GROUP BY t1.k ORDER BY k
SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

DROP TABLE t_corr_left;
DROP TABLE t_corr_right_multi;
DROP TABLE t_corr_right_uniq;
DROP TABLE t_corr_empty;
DROP TABLE t_corr_empty_right;
