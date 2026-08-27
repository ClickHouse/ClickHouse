-- Tags: no-old-analyzer
-- no-old-analyzer: distributed planning requires the analyzer.

-- Every scenario runs twice: through the cascades optimizer with `cascades_aggregation_pushdown`
-- steered towards the pushed plan by stat hints, and classically (`enable_cascades_optimizer = 0`,
-- `make_distributed_plan = 0`). The two result blocks of each scenario must be identical.

DROP TABLE IF EXISTS t_corr_left;
DROP TABLE IF EXISTS t_corr_right_multi;
DROP TABLE IF EXISTS t_corr_right_uniq;
DROP TABLE IF EXISTS t_corr_right_2k;
DROP TABLE IF EXISTS t_corr_right_expr;
DROP TABLE IF EXISTS t_corr_empty;
DROP TABLE IF EXISTS t_corr_empty_right;

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
CREATE TABLE t_corr_empty (k UInt32, v Int64) ENGINE = MergeTree ORDER BY k
  SETTINGS auto_statistics_types = '';
CREATE TABLE t_corr_empty_right (k UInt32, w Int64) ENGINE = MergeTree ORDER BY k
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
SET param__internal_join_table_stat_hints = '{"t_corr_left": {"cardinality": 100000000, "avg_row_bytes": 20, "distinct_keys": {"k": 100, "v": 1000, "p": 2, "big": 2}}, "t_corr_empty": {"cardinality": 100000000, "avg_row_bytes": 12, "distinct_keys": {"k": 100}}, "t_corr_right_multi": {"cardinality": 1000, "avg_row_bytes": 20, "distinct_keys": {"k": 1000}}, "t_corr_right_uniq": {"cardinality": 1000, "avg_row_bytes": 20, "distinct_keys": {"k": 1000}}, "t_corr_right_2k": {"cardinality": 1000, "avg_row_bytes": 16, "distinct_keys": {"k": 1000}}, "t_corr_right_expr": {"cardinality": 1000, "avg_row_bytes": 8, "distinct_keys": {"b": 1000}}, "t_corr_empty_right": {"cardinality": 1000, "avg_row_bytes": 12, "distinct_keys": {"k": 1000}}}';

-- Canaries: prove the stat hints above actually steer the cascades optimizer to the pushed
-- shapes for this file's tables, not to a classic plan that would make every on/off pair below
-- compare classic-vs-classic while staying green. The discriminators work on `Aggregating` line
-- COUNT and ORDER in the top-down `EXPLAIN` output. Pushed variant A is an aggregation sandwich -
-- the merge-only `Aggregating` ABOVE the join, the partial `Aggregating` BELOW it - and the only
-- shape with two `Aggregating` lines (classic two-stage and classic shuffle both have exactly
-- one: the classic merge prints as `MergingAggregated`); the order conjuncts pin the sandwich.
-- Pushed variant B keeps a single `Aggregating` with the first `JoinLogical` line ABOVE it, while
-- classic places its `Aggregating` above the join, so the order conjunct separates B from classic
-- and the count conjunct separates B from A. No `MergingAggregated` conjunct anywhere: pushed
-- variant B legitimately contains one `MergingAggregated` (the two-stage split of the pushed
-- final aggregation below the join), so its absence is not even a valid sanity check. `minIf`
-- returns the default value on no match, so the order checks alone would be illegible on an
-- absent node - hence the explicit presence conjuncts alongside them. `trimLeft(explain) LIKE
-- 'Aggregating%'` (anchored, like 04926's task-budget case) rather than a bare substring keeps
-- working even if some step's descriptive text ever contains the word `Aggregating`;
-- `explain_query_plan_default = 'legacy'` is pinned on the explained query because the anchor
-- relies on plain-text indentation, not this file's default pretty tree-drawing prefix.
SELECT '-- canary: variant A (partial pushdown) fires for case 1''s query';
SELECT
    countIf(explain LIKE '%JoinLogical%') > 0 AS has_join,
    countIf(trimLeft(explain) LIKE 'Aggregating%') >= 2 AS has_merge_and_partial,
    minIf(rn, explain LIKE '%JoinLogical%')
        < maxIf(rn, trimLeft(explain) LIKE 'Aggregating%') AS partial_below_join,
    minIf(rn, trimLeft(explain) LIKE 'Aggregating%')
        < minIf(rn, explain LIKE '%JoinLogical%') AS merge_above_join
FROM
(
    SELECT explain, rowNumberInAllBlocks() AS rn
    FROM
    (
        EXPLAIN SELECT t1.k AS k, count() AS c, sum(t1.v) AS s FROM t_corr_left AS t1 INNER JOIN t_corr_right_multi AS t2 ON t1.k = t2.k GROUP BY t1.k ORDER BY k
        SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1, explain_query_plan_default = 'legacy'
    )
) SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

SELECT '-- canary: variant B (full pushdown) fires for case 13''s query (single Aggregating, below the join)';
SELECT
    countIf(explain LIKE '%JoinLogical%') > 0 AS has_join,
    countIf(trimLeft(explain) LIKE 'Aggregating%') = 1 AS single_aggregation,
    minIf(rn, explain LIKE '%JoinLogical%')
        < minIf(rn, trimLeft(explain) LIKE 'Aggregating%') AS join_above_aggregation
FROM
(
    SELECT explain, rowNumberInAllBlocks() AS rn
    FROM
    (
        EXPLAIN SELECT t1.k AS k, count() AS c, sum(t1.v) AS s FROM t_corr_left AS t1 LEFT ANY JOIN t_corr_right_multi AS t2 ON t1.k = t2.k GROUP BY t1.k ORDER BY k
        SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1, explain_query_plan_default = 'legacy'
    )
) SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

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
DROP TABLE t_corr_right_2k;
DROP TABLE t_corr_right_expr;
DROP TABLE t_corr_empty;
DROP TABLE t_corr_empty_right;
