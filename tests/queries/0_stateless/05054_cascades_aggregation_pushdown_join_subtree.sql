-- Tags: no-old-analyzer
-- no-old-analyzer: distributed planning requires the analyzer.

-- The pushed side of a `cascades_aggregation_pushdown` here is a JOIN SUBTREE, not a table scan
-- (the header, statistics and condition columns all come from a join), plus the repeated
-- pushdown through two joins. Every executed scenario runs twice - through the cascades
-- optimizer steered by stat hints, and classically - and the result blocks must match; the
-- canary conjuncts prove the pushed shapes actually fire (see the comment before them).

DROP TABLE IF EXISTS t_corr_left;
DROP TABLE IF EXISTS t_corr_right_multi;
DROP TABLE IF EXISTS t_corr_right_uniq;

CREATE TABLE t_corr_left (k UInt32, v Int64) ENGINE = MergeTree ORDER BY k
  SETTINGS auto_statistics_types = '';
CREATE TABLE t_corr_right_multi (k UInt32, t Int64) ENGINE = MergeTree ORDER BY k
  SETTINGS auto_statistics_types = '';
CREATE TABLE t_corr_right_uniq (k UInt32, name String) ENGINE = MergeTree ORDER BY k
  SETTINGS auto_statistics_types = '';
-- a merge between planning and the worker read would invalidate the planned part names
SYSTEM STOP MERGES t_corr_left;
SYSTEM STOP MERGES t_corr_right_multi;
SYSTEM STOP MERGES t_corr_right_uniq;

-- 10 rows per key k = 0..9: v = k + 10*i, so per key count = 10, sum(v) = 450 + 10*k
INSERT INTO t_corr_left SELECT number % 10, number FROM numbers(100);
-- fan-out: key 0 once, key 1 twice, key 2 three times, keys 3, 4, 5 once; keys 6-9 absent
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
-- `t_corr_left` huge with few distinct `k`, both right tables small, so the join reorderer
-- keeps the huge join subtree as an input of the top join and the pushdown pays off on it.
SET param__internal_join_table_stat_hints = '{"t_corr_left": {"cardinality": 100000000, "avg_row_bytes": 20, "distinct_keys": {"k": 100, "v": 1000}}, "t_corr_right_multi": {"cardinality": 1000, "avg_row_bytes": 20, "distinct_keys": {"k": 1000}}, "t_corr_right_uniq": {"cardinality": 1000, "avg_row_bytes": 20, "distinct_keys": {"k": 1000}}}';

-- Canaries (04927-style): the full legacy EXPLAIN output is pinned. The runtime-filter and
-- prewhere settings decide the `BuildRuntimeFilter`/`Filter` lines and are randomized by the
-- harness, so they are pinned in each EXPLAIN's SETTINGS clause only - a session-level `SET`
-- would leak into the executed on/off scenarios below.

-- Variant A pushed onto the join subtree: the merge-only `Aggregating` above both joins, the
-- partial `Aggregating` strictly between them (below the top join, above the inner one).
SELECT '-- canary 1: variant A onto the join subtree (case 1''s query)';
EXPLAIN SELECT t1.k AS k, count() AS c, sum(t1.v) AS s
FROM t_corr_left AS t1
INNER JOIN t_corr_right_multi AS t2 ON t1.k = t2.k
LEFT JOIN t_corr_right_uniq AS t3 ON t1.k = t3.k
GROUP BY t1.k ORDER BY k
SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1, explain_query_plan_default = 'legacy',
    enable_join_runtime_filters = 1, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;

-- Variant B onto the subtree under a LEFT SEMI top join - and the pushed final aggregation
-- immediately takes a SECOND, variant-A pushdown through the INNER join (its expression is new
-- to the memo and its child group is already explored, so the rule legally fires on an
-- expression it itself created): both `Aggregating` lines end up below the top join,
-- sandwiching the inner one.
SELECT '-- canary 2: variant B onto the subtree cascades into a second variant-A pushdown (case 2''s query)';
EXPLAIN SELECT t1.k AS k, count() AS c, sum(t1.v) AS s
FROM t_corr_left AS t1
INNER JOIN t_corr_right_multi AS t2 ON t1.k = t2.k
LEFT SEMI JOIN t_corr_right_uniq AS t3 ON t1.k = t3.k
GROUP BY t1.k ORDER BY k
SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1, explain_query_plan_default = 'legacy',
    enable_join_runtime_filters = 1, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;

-- Repeated variant-B pushdown through two LEFT SEMI joins: the rule fires a second time on the
-- final aggregation its first application created, leaving the aggregation below both
-- `JoinLogical` lines.
SELECT '-- canary 3: repeated variant B through two LEFT SEMI joins (case 3''s query)';
EXPLAIN SELECT t1.k AS k, count() AS c
FROM t_corr_left AS t1
LEFT SEMI JOIN t_corr_right_uniq AS t2 ON t1.k = t2.k
LEFT SEMI JOIN t_corr_right_multi AS t3 ON t1.k = t3.k
GROUP BY t1.k ORDER BY k
SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1, explain_query_plan_default = 'legacy',
    enable_join_runtime_filters = 1, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;

SELECT '-- 1. variant A onto the join subtree (INNER below, LEFT above)';
SELECT t1.k AS k, count() AS c, sum(t1.v) AS s
FROM t_corr_left AS t1
INNER JOIN t_corr_right_multi AS t2 ON t1.k = t2.k
LEFT JOIN t_corr_right_uniq AS t3 ON t1.k = t3.k
GROUP BY t1.k ORDER BY k;
SELECT t1.k AS k, count() AS c, sum(t1.v) AS s
FROM t_corr_left AS t1
INNER JOIN t_corr_right_multi AS t2 ON t1.k = t2.k
LEFT JOIN t_corr_right_uniq AS t3 ON t1.k = t3.k
GROUP BY t1.k ORDER BY k
SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

SELECT '-- 2. variant B onto the join subtree (INNER below, LEFT SEMI above)';
SELECT t1.k AS k, count() AS c, sum(t1.v) AS s
FROM t_corr_left AS t1
INNER JOIN t_corr_right_multi AS t2 ON t1.k = t2.k
LEFT SEMI JOIN t_corr_right_uniq AS t3 ON t1.k = t3.k
GROUP BY t1.k ORDER BY k;
SELECT t1.k AS k, count() AS c, sum(t1.v) AS s
FROM t_corr_left AS t1
INNER JOIN t_corr_right_multi AS t2 ON t1.k = t2.k
LEFT SEMI JOIN t_corr_right_uniq AS t3 ON t1.k = t3.k
GROUP BY t1.k ORDER BY k
SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

SELECT '-- 3. repeated variant B through two LEFT SEMI joins';
SELECT t1.k AS k, count() AS c
FROM t_corr_left AS t1
LEFT SEMI JOIN t_corr_right_uniq AS t2 ON t1.k = t2.k
LEFT SEMI JOIN t_corr_right_multi AS t3 ON t1.k = t3.k
GROUP BY t1.k ORDER BY k;
SELECT t1.k AS k, count() AS c
FROM t_corr_left AS t1
LEFT SEMI JOIN t_corr_right_uniq AS t2 ON t1.k = t2.k
LEFT SEMI JOIN t_corr_right_multi AS t3 ON t1.k = t3.k
GROUP BY t1.k ORDER BY k
SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

DROP TABLE t_corr_left;
DROP TABLE t_corr_right_multi;
DROP TABLE t_corr_right_uniq;
