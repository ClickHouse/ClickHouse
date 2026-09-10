-- Tags: no-old-analyzer
-- no-old-analyzer: distributed planning requires the analyzer.

-- Every scenario runs twice: through the cascades optimizer with `cascades_aggregation_pushdown`
-- steered towards the pushed plan by stat hints, and classically (`enable_cascades_optimizer = 0`,
-- `make_distributed_plan = 0`). The two result blocks of each scenario must be identical.
-- Scenarios 11-22 live in `05048_cascades_aggregation_pushdown_correctness_2`: the scenario list
-- is split in two so each half fits the flaky-check time budget.

DROP TABLE IF EXISTS t_corr_left;
DROP TABLE IF EXISTS t_corr_right_multi;
DROP TABLE IF EXISTS t_corr_right_uniq;
DROP TABLE IF EXISTS t_corr_right_2k;
DROP TABLE IF EXISTS t_corr_right_expr;

CREATE TABLE t_corr_left (k UInt32, p UInt32, v Int64, big UInt8) ENGINE = MergeTree ORDER BY k
  SETTINGS auto_statistics_types = '';
CREATE TABLE t_corr_right_multi (k UInt32, t Int64) ENGINE = MergeTree ORDER BY k
  SETTINGS auto_statistics_types = '';
CREATE TABLE t_corr_right_uniq (k UInt32, name String) ENGINE = MergeTree ORDER BY k
  SETTINGS auto_statistics_types = '';
CREATE TABLE t_corr_right_2k (k UInt32, p UInt32) ENGINE = MergeTree ORDER BY k
  SETTINGS auto_statistics_types = '';
CREATE TABLE t_corr_right_expr (b UInt32) ENGINE = MergeTree ORDER BY b
  SETTINGS auto_statistics_types = '';
-- a merge between planning and the worker read would invalidate the planned part names
SYSTEM STOP MERGES t_corr_left;
SYSTEM STOP MERGES t_corr_right_multi;
SYSTEM STOP MERGES t_corr_right_uniq;
SYSTEM STOP MERGES t_corr_right_2k;
SYSTEM STOP MERGES t_corr_right_expr;

-- 10 rows per key k = 0..9: v = k + 10*i for i = 0..9, so per key
-- count = 10, sum(v) = 450 + 10*k, min(v) = k, avg(v) = 45 + k, uniqExact(v) = 10,
-- countIf(big) = 5 (v >= 50 holds for i = 5..9); p = k % 2.
INSERT INTO t_corr_left SELECT number % 10, number % 2, number, number >= 50 FROM numbers(100);
-- fan-out: key 0 once (t=50), key 1 twice (30, 70), key 2 three times (10, 40, 90),
-- keys 3, 4, 5 once (55, 5, 95); keys 6-9 absent
INSERT INTO t_corr_right_multi VALUES (0, 50), (1, 30), (1, 70), (2, 10), (2, 40), (2, 90), (3, 55), (4, 5), (5, 95);
-- unique per key, keys 0-7
INSERT INTO t_corr_right_uniq SELECT number, concat('n_', toString(number)) FROM numbers(8);
-- two-column keys matching left keys 0-3 (left always has p = k % 2); (4, 1) never matches
INSERT INTO t_corr_right_2k VALUES (0, 0), (1, 1), (2, 0), (3, 1), (4, 1);
-- b = 1..5 matches left k = 0..4 through k + 1 = b
INSERT INTO t_corr_right_expr SELECT number + 1 FROM numbers(5);

SET make_distributed_plan = 1;
SET enable_cascades_optimizer = 1;
SET distributed_plan_execute_locally = 1;
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET max_rows_to_group_by = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET param__internal_cascades_cluster_node_count = 4;
-- `p` and `big` are extra pushed keys for cases 3, 8, 9 (a join-condition column and a
-- `GROUP BY` key not equal to the join key, respectively) - the cardinality gate in
-- `AggregationPushdown::buildPushdownAlternative` needs a real NDV for every pushed key, and
-- both happen to be genuinely low-cardinality (`p = k % 2`, `big` is boolean-like).
SET param__internal_join_table_stat_hints = '{"t_corr_left": {"cardinality": 100000000, "avg_row_bytes": 20, "distinct_keys": {"k": 100, "v": 1000, "p": 2, "big": 2}}, "t_corr_right_multi": {"cardinality": 1000, "avg_row_bytes": 20, "distinct_keys": {"k": 1000}}, "t_corr_right_uniq": {"cardinality": 1000, "avg_row_bytes": 20, "distinct_keys": {"k": 1000}}, "t_corr_right_2k": {"cardinality": 1000, "avg_row_bytes": 16, "distinct_keys": {"k": 1000}}, "t_corr_right_expr": {"cardinality": 1000, "avg_row_bytes": 8, "distinct_keys": {"b": 1000}}}';

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

SELECT '-- 1. INNER ALL with fan-out (variant A, push-left)';
SELECT t1.k AS k, count() AS c, sum(t1.v) AS s FROM t_corr_left AS t1 INNER JOIN t_corr_right_multi AS t2 ON t1.k = t2.k GROUP BY t1.k ORDER BY k;
SELECT t1.k AS k, count() AS c, sum(t1.v) AS s FROM t_corr_left AS t1 INNER JOIN t_corr_right_multi AS t2 ON t1.k = t2.k GROUP BY t1.k ORDER BY k
SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

SELECT '-- 2. LEFT ALL with unmatched left rows (keys 6-9)';
SELECT t1.k AS k, count() AS c, sum(t1.v) AS s FROM t_corr_left AS t1 LEFT JOIN t_corr_right_multi AS t2 ON t1.k = t2.k GROUP BY t1.k ORDER BY k;
SELECT t1.k AS k, count() AS c, sum(t1.v) AS s FROM t_corr_left AS t1 LEFT JOIN t_corr_right_multi AS t2 ON t1.k = t2.k GROUP BY t1.k ORDER BY k
SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

SELECT '-- 3. multi-key ON';
SELECT t1.k AS k, count() AS c FROM t_corr_left AS t1 INNER JOIN t_corr_right_2k AS t2 ON t1.k = t2.k AND t1.p = t2.p GROUP BY t1.k ORDER BY k;
SELECT t1.k AS k, count() AS c FROM t_corr_left AS t1 INNER JOIN t_corr_right_2k AS t2 ON t1.k = t2.k AND t1.p = t2.p GROUP BY t1.k ORDER BY k
SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

SELECT '-- 4. expression key';
SELECT t1.k AS k, count() AS c FROM t_corr_left AS t1 INNER JOIN t_corr_right_expr AS t2 ON t1.k + 1 = t2.b GROUP BY t1.k ORDER BY k;
SELECT t1.k AS k, count() AS c FROM t_corr_left AS t1 INNER JOIN t_corr_right_expr AS t2 ON t1.k + 1 = t2.b GROUP BY t1.k ORDER BY k
SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

SELECT '-- 4b. expression key on the other side (the pushed side groups by a plain column)';
SELECT t1.k AS k, count() AS c FROM t_corr_left AS t1 INNER JOIN t_corr_right_expr AS t2 ON t1.k = t2.b + 1 GROUP BY t1.k ORDER BY k;
SELECT t1.k AS k, count() AS c FROM t_corr_left AS t1 INNER JOIN t_corr_right_expr AS t2 ON t1.k = t2.b + 1 GROUP BY t1.k ORDER BY k
SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

SELECT '-- 5. residual condition';
SELECT t1.k AS k, count() AS c FROM t_corr_left AS t1 INNER JOIN t_corr_right_multi AS t2 ON t1.k = t2.k AND t1.v > t2.t GROUP BY t1.k ORDER BY k;
SELECT t1.k AS k, count() AS c FROM t_corr_left AS t1 INNER JOIN t_corr_right_multi AS t2 ON t1.k = t2.k AND t1.v > t2.t GROUP BY t1.k ORDER BY k
SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

SELECT '-- 6. aggregate battery';
SELECT t1.k AS k, count() AS c, sum(t1.v) AS s, min(t1.v) AS mn, avg(t1.v) AS a, uniqExact(t1.v) AS u, countIf(t1.big) AS cb
FROM t_corr_left AS t1 LEFT JOIN t_corr_right_uniq AS t2 ON t1.k = t2.k GROUP BY t1.k ORDER BY k;
SELECT t1.k AS k, count() AS c, sum(t1.v) AS s, min(t1.v) AS mn, avg(t1.v) AS a, uniqExact(t1.v) AS u, countIf(t1.big) AS cb
FROM t_corr_left AS t1 LEFT JOIN t_corr_right_uniq AS t2 ON t1.k = t2.k GROUP BY t1.k ORDER BY k
SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

SELECT '-- 7. GROUP BY keys from both sides';
SELECT t1.k AS k, t2.name AS n, count() AS c FROM t_corr_left AS t1 LEFT JOIN t_corr_right_uniq AS t2 ON t1.k = t2.k GROUP BY t1.k, t2.name ORDER BY k, n;
SELECT t1.k AS k, t2.name AS n, count() AS c FROM t_corr_left AS t1 LEFT JOIN t_corr_right_uniq AS t2 ON t1.k = t2.k GROUP BY t1.k, t2.name ORDER BY k, n
SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

SELECT '-- 8. LEFT SEMI, variant A (join key not a GROUP BY key)';
SELECT t1.big AS b, count() AS c FROM t_corr_left AS t1 LEFT SEMI JOIN t_corr_right_uniq AS t2 ON t1.k = t2.k GROUP BY t1.big ORDER BY b;
SELECT t1.big AS b, count() AS c FROM t_corr_left AS t1 LEFT SEMI JOIN t_corr_right_uniq AS t2 ON t1.k = t2.k GROUP BY t1.big ORDER BY b
SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

SELECT '-- 9. LEFT ANTI, variant A (join key not a GROUP BY key)';
SELECT t1.big AS b, count() AS c FROM t_corr_left AS t1 LEFT ANTI JOIN t_corr_right_uniq AS t2 ON t1.k = t2.k GROUP BY t1.big ORDER BY b;
SELECT t1.big AS b, count() AS c FROM t_corr_left AS t1 LEFT ANTI JOIN t_corr_right_uniq AS t2 ON t1.k = t2.k GROUP BY t1.big ORDER BY b
SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

SELECT '-- 10. LEFT ANY, variant A (per-key-unique right side, GROUP BY key from it)';
SELECT t1.k AS k, t2.name AS n, count() AS c FROM t_corr_left AS t1 LEFT ANY JOIN t_corr_right_uniq AS t2 ON t1.k = t2.k GROUP BY t1.k, t2.name ORDER BY k, n;
SELECT t1.k AS k, t2.name AS n, count() AS c FROM t_corr_left AS t1 LEFT ANY JOIN t_corr_right_uniq AS t2 ON t1.k = t2.k GROUP BY t1.k, t2.name ORDER BY k, n
SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

DROP TABLE t_corr_left;
DROP TABLE t_corr_right_multi;
DROP TABLE t_corr_right_uniq;
DROP TABLE t_corr_right_2k;
DROP TABLE t_corr_right_expr;
