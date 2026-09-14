-- Tags: no-old-analyzer
-- no-old-analyzer: distributed planning requires the analyzer.

-- The `cascades_aggregation_pushdown` transformation offers a partial aggregation pushed below
-- a join as a cost-based alternative: a merge-only `Aggregating` above the join, a partial
-- `Aggregating` below on the pushed input. It wins when the pushed side is huge but has few
-- distinct keys.

DROP TABLE IF EXISTS t_push_facts;
DROP TABLE IF EXISTS t_push_dims;
DROP TABLE IF EXISTS t_push_dims_multi;

CREATE TABLE t_push_facts (key UInt32, value Int64) ENGINE = MergeTree ORDER BY key
  SETTINGS auto_statistics_types = '';
CREATE TABLE t_push_dims (key UInt32, name String) ENGINE = MergeTree ORDER BY key
  SETTINGS auto_statistics_types = '';
CREATE TABLE t_push_dims_multi (key UInt32, threshold Int64) ENGINE = MergeTree ORDER BY key
  SETTINGS auto_statistics_types = '';
-- a merge between planning and the worker read would invalidate the planned part names
SYSTEM STOP MERGES t_push_facts;
SYSTEM STOP MERGES t_push_dims;
SYSTEM STOP MERGES t_push_dims_multi;

INSERT INTO t_push_facts SELECT number % 10, number FROM numbers(1000);
INSERT INTO t_push_dims SELECT number, concat('name_', toString(number)) FROM numbers(8);
-- keys 0, 1, 2 appear 3 times (thresholds 100, 200, 300), keys 3, 4, 5 once (threshold 500),
-- keys 6-9 are absent
INSERT INTO t_push_dims_multi SELECT number % 3, toInt64(100 * (1 + intDiv(number, 3))) FROM numbers(9);
INSERT INTO t_push_dims_multi SELECT number, toInt64(500) FROM numbers(3, 3);

SET explain_query_plan_default = 'legacy';
SET make_distributed_plan = 1;
SET enable_cascades_optimizer = 1;
SET distributed_plan_execute_locally = 1;
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET max_rows_to_group_by = 0;
SET query_plan_optimize_join_order_randomize = 0;
-- the runtime-filter steps are part of the expected INNER JOIN plan shape, and whether the
-- filter moves into PREWHERE decides between a `Filter` and an `Expression` step in it
SET enable_join_runtime_filters = 1;
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1;
-- the physical build-side choice, the runtime-filter row threshold and the pre-cascades
-- join-order pass (which attaches the row estimates) decide the pinned push-right shapes
-- (all three settings are randomized by the test harness)
SET query_plan_join_swap_table = 'auto';
SET join_runtime_filter_min_probe_rows = 1000;
SET query_plan_optimize_join_order_limit = 10;
SET param__internal_cascades_cluster_node_count = 4;
SET param__internal_join_table_stat_hints = '{"t_push_facts": {"cardinality": 100000000, "avg_row_bytes": 12, "distinct_keys": {"key": 100}}, "t_push_dims": {"cardinality": 1000, "avg_row_bytes": 20, "distinct_keys": {"key": 1000}}}';

SELECT '-- 1. huge left side, few distinct keys: partial aggregation is pushed below the LEFT JOIN';
EXPLAIN SELECT count() FROM t_push_facts AS t1 LEFT JOIN t_push_dims AS t2 ON t1.key = t2.key GROUP BY t1.key;

-- asserts the behavioral compatibility contract end-to-end (settings-changes history ->
-- `QueryPlanOptimizationSettings` -> rule registration): the otherwise-pushable query above
-- reverts to the classic shape under `compatibility = '26.8'`.
SELECT '-- 1b. same query under compatibility = 26.8: classic shape (behavioral compatibility contract)';
EXPLAIN SELECT count() FROM t_push_facts AS t1 LEFT JOIN t_push_dims AS t2 ON t1.key = t2.key GROUP BY t1.key
SETTINGS compatibility = '26.8';

SELECT '-- 2. same with INNER JOIN';
EXPLAIN SELECT count() FROM t_push_facts AS t1 INNER JOIN t_push_dims AS t2 ON t1.key = t2.key GROUP BY t1.key;

SELECT '-- 2b. negative: non-deterministic join condition function (`rand`) blocks the pushdown, classic shape';
EXPLAIN SELECT count() FROM t_push_facts AS t1 LEFT JOIN t_push_dims AS t2 ON t1.key = t2.key AND rand() % 2 = 0 GROUP BY t1.key;

SELECT '-- 3. near-unique keys: pushdown does not pay off, classic shape';
SET param__internal_join_table_stat_hints = '{"t_push_facts": {"cardinality": 100000000, "avg_row_bytes": 12, "distinct_keys": {"key": 99000000}}, "t_push_dims": {"cardinality": 1000, "avg_row_bytes": 20, "distinct_keys": {"key": 1000}}}';
EXPLAIN SELECT count() FROM t_push_facts AS t1 LEFT JOIN t_push_dims AS t2 ON t1.key = t2.key GROUP BY t1.key;
SET param__internal_join_table_stat_hints = '{"t_push_facts": {"cardinality": 100000000, "avg_row_bytes": 12, "distinct_keys": {"key": 100}}, "t_push_dims": {"cardinality": 1000, "avg_row_bytes": 20, "distinct_keys": {"key": 1000}}}';

-- cardinality gate negatives (see `AggregationPushdown::buildPushdownAlternative`): the rule
-- itself refuses to build the alternative, unlike case 3's cost-based rejection above.
SELECT '-- 3a. negative: missing NDV for the pushed join key (cardinality gate), classic shape';
SET param__internal_join_table_stat_hints = '{"t_push_facts": {"cardinality": 100000000, "avg_row_bytes": 12}, "t_push_dims": {"cardinality": 1000, "avg_row_bytes": 20, "distinct_keys": {"key": 1000}}}';
EXPLAIN SELECT count() FROM t_push_facts AS t1 LEFT JOIN t_push_dims AS t2 ON t1.key = t2.key GROUP BY t1.key;
SET param__internal_join_table_stat_hints = '{"t_push_facts": {"cardinality": 100000000, "avg_row_bytes": 12, "distinct_keys": {"key": 100}}, "t_push_dims": {"cardinality": 1000, "avg_row_bytes": 20, "distinct_keys": {"key": 1000}}}';

-- two pushed keys (`key`, `value`, the latter from the residual condition) whose NDV product
-- equals the pushed side's cardinality although each is individually small - the exact shape the
-- pre-gate max-of-NDVs estimate mispriced as profitable (confirmed pushed on the pre-fix binary).
SELECT '-- 3b. negative: no guaranteed reduction from the composite key NDV (cardinality gate), classic shape';
SET param__internal_join_table_stat_hints = '{"t_push_facts": {"cardinality": 100000000, "avg_row_bytes": 12, "distinct_keys": {"key": 10000, "value": 10000}}, "t_push_dims_multi": {"cardinality": 1000, "avg_row_bytes": 20, "distinct_keys": {"key": 100}}}';
EXPLAIN SELECT t1.key AS k, count() FROM t_push_facts AS t1 INNER JOIN t_push_dims_multi AS t2 ON t1.key = t2.key AND t1.value > t2.threshold GROUP BY t1.key ORDER BY k;
SET param__internal_join_table_stat_hints = '{"t_push_facts": {"cardinality": 100000000, "avg_row_bytes": 12, "distinct_keys": {"key": 100}}, "t_push_dims": {"cardinality": 1000, "avg_row_bytes": 20, "distinct_keys": {"key": 1000}}}';

SELECT '-- 4. disabled by the setting: classic shape';
EXPLAIN SELECT count() FROM t_push_facts AS t1 LEFT JOIN t_push_dims AS t2 ON t1.key = t2.key GROUP BY t1.key
SETTINGS cascades_aggregation_pushdown = 0;

SELECT '-- 5a. aggregate with an argument: sum is pushed';
EXPLAIN SELECT sum(t1.value) FROM t_push_facts AS t1 LEFT JOIN t_push_dims AS t2 ON t1.key = t2.key GROUP BY t1.key;

SELECT '-- 5b. additional GROUP BY key from the right side: still pushed';
EXPLAIN SELECT count() FROM t_push_facts AS t1 LEFT JOIN t_push_dims AS t2 ON t1.key = t2.key GROUP BY t1.key, t2.name;

SELECT '-- 6. execution: count() per key over LEFT JOIN (keys 0-7 match, 8 and 9 do not)';
SELECT t1.key AS k, count() AS c FROM t_push_facts AS t1 LEFT JOIN t_push_dims AS t2 ON t1.key = t2.key GROUP BY t1.key ORDER BY k;

SELECT '-- 6b. execution: count() per key over INNER JOIN (keys 8 and 9 drop out)';
SELECT t1.key AS k, count() AS c FROM t_push_facts AS t1 INNER JOIN t_push_dims AS t2 ON t1.key = t2.key GROUP BY t1.key ORDER BY k;

SELECT '-- 7. execution: sum(t1.value) per (t1.key, t2.name) over LEFT JOIN';
SELECT t1.key AS k, t2.name AS n, sum(t1.value) AS s FROM t_push_facts AS t1 LEFT JOIN t_push_dims AS t2 ON t1.key = t2.key GROUP BY t1.key, t2.name ORDER BY k, n;

SELECT '-- 8. the same executions without the distributed planner must match';
SELECT t1.key AS k, count() AS c FROM t_push_facts AS t1 LEFT JOIN t_push_dims AS t2 ON t1.key = t2.key GROUP BY t1.key ORDER BY k
SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;
SELECT t1.key AS k, count() AS c FROM t_push_facts AS t1 INNER JOIN t_push_dims AS t2 ON t1.key = t2.key GROUP BY t1.key ORDER BY k
SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;
SELECT t1.key AS k, t2.name AS n, sum(t1.value) AS s FROM t_push_facts AS t1 LEFT JOIN t_push_dims AS t2 ON t1.key = t2.key GROUP BY t1.key, t2.name ORDER BY k, n
SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

SET param__internal_join_table_stat_hints = '{"t_push_facts": {"cardinality": 100000000, "avg_row_bytes": 12, "distinct_keys": {"key": 100, "value": 1000}}, "t_push_dims_multi": {"cardinality": 1000, "avg_row_bytes": 20, "distinct_keys": {"key": 100}}}';

SELECT '-- 9. duplicate right-side keys: each pushed group is duplicated by the join and merged m times';
EXPLAIN SELECT t1.key AS k, count() AS c, sum(t1.value) AS s FROM t_push_facts AS t1 LEFT JOIN t_push_dims_multi AS t2 ON t1.key = t2.key GROUP BY t1.key ORDER BY k;
SELECT t1.key AS k, count() AS c, sum(t1.value) AS s FROM t_push_facts AS t1 LEFT JOIN t_push_dims_multi AS t2 ON t1.key = t2.key GROUP BY t1.key ORDER BY k;

SELECT '-- 10. mixed condition (equi + non-equi): the pushed side groups by (key, value)';
EXPLAIN SELECT t1.key AS k, count() AS c FROM t_push_facts AS t1 INNER JOIN t_push_dims_multi AS t2 ON t1.key = t2.key AND t1.value > t2.threshold GROUP BY t1.key ORDER BY k;
SELECT t1.key AS k, count() AS c FROM t_push_facts AS t1 INNER JOIN t_push_dims_multi AS t2 ON t1.key = t2.key AND t1.value > t2.threshold GROUP BY t1.key ORDER BY k;

SELECT '-- 11. the same executions without the distributed planner must match';
SELECT t1.key AS k, count() AS c, sum(t1.value) AS s FROM t_push_facts AS t1 LEFT JOIN t_push_dims_multi AS t2 ON t1.key = t2.key GROUP BY t1.key ORDER BY k
SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;
SELECT t1.key AS k, count() AS c FROM t_push_facts AS t1 INNER JOIN t_push_dims_multi AS t2 ON t1.key = t2.key AND t1.value > t2.threshold GROUP BY t1.key ORDER BY k
SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

-- restores the preamble's stat hints after case 9-11 overrode them with `t_push_dims_multi`-only
-- hints (no `t_push_dims` entry); dropping it changes the active hints for cases 12+ and flips
-- their pinned plan shapes. Adds a `value` NDV on top of the preamble value: case 19 groups by
-- `t1.value` and extends the pushed keys with the join's `key` condition column, and the
-- cardinality gate in `AggregationPushdown::buildPushdownAlternative` needs a real NDV for both.
SET param__internal_join_table_stat_hints = '{"t_push_facts": {"cardinality": 100000000, "avg_row_bytes": 12, "distinct_keys": {"key": 100, "value": 1000}}, "t_push_dims": {"cardinality": 1000, "avg_row_bytes": 20, "distinct_keys": {"key": 1000}}}';

SELECT '-- 12. push-right: RIGHT JOIN with a huge right side';
EXPLAIN SELECT count() FROM t_push_dims AS t1 RIGHT JOIN t_push_facts AS t2 ON t1.key = t2.key GROUP BY t2.key;

SELECT '-- 13. push-right: INNER JOIN, cost pushes the huge right side';
EXPLAIN SELECT count() FROM t_push_dims AS t1 INNER JOIN t_push_facts AS t2 ON t1.key = t2.key GROUP BY t2.key;

SELECT '-- 14. variant B: LEFT SEMI with join keys subset of GROUP BY keys, no merge step above the join';
EXPLAIN SELECT count() FROM t_push_facts AS t1 LEFT SEMI JOIN t_push_dims AS t2 ON t1.key = t2.key GROUP BY t1.key;

SELECT '-- 15. variant B: LEFT ANTI';
EXPLAIN SELECT count() FROM t_push_facts AS t1 LEFT ANTI JOIN t_push_dims AS t2 ON t1.key = t2.key GROUP BY t1.key;

SELECT '-- 16. variant B: LEFT ANY';
EXPLAIN SELECT count() FROM t_push_facts AS t1 LEFT ANY JOIN t_push_dims AS t2 ON t1.key = t2.key GROUP BY t1.key;

SELECT '-- 17. variant B: RIGHT ANY, push-right';
EXPLAIN SELECT count() FROM t_push_dims AS t1 RIGHT ANY JOIN t_push_facts AS t2 ON t1.key = t2.key GROUP BY t2.key;

SELECT '-- 18. B-dominance negative: a GROUP BY key from the other side falls back to variant A';
EXPLAIN SELECT count() FROM t_push_facts AS t1 LEFT ANY JOIN t_push_dims AS t2 ON t1.key = t2.key GROUP BY t1.key, t2.name;

SELECT '-- 19. variant A for LEFT SEMI: the join key is not a GROUP BY key';
EXPLAIN SELECT count() FROM t_push_facts AS t1 LEFT SEMI JOIN t_push_dims AS t2 ON t1.key = t2.key GROUP BY t1.value;

SELECT '-- 20. variant A for LEFT ANTI: a GROUP BY key from the other side falls back to variant A';
EXPLAIN SELECT count() FROM t_push_facts AS t1 LEFT ANTI JOIN t_push_dims AS t2 ON t1.key = t2.key GROUP BY t1.key, t2.name;

SELECT '-- 21. variant A for RIGHT SEMI, push-right: a GROUP BY key from the other side falls back to variant A';
EXPLAIN SELECT count() FROM t_push_dims AS t1 RIGHT SEMI JOIN t_push_facts AS t2 ON t1.key = t2.key GROUP BY t2.key, t1.name;

SELECT '-- 22. variant A for RIGHT ANTI, push-right: a GROUP BY key from the other side falls back to variant A';
EXPLAIN SELECT count() FROM t_push_dims AS t1 RIGHT ANTI JOIN t_push_facts AS t2 ON t1.key = t2.key GROUP BY t2.key, t1.name;

SELECT '-- 23. negative: INNER ANY emits at most one row per key, never pushed';
EXPLAIN SELECT count() FROM t_push_facts AS t1 INNER ANY JOIN t_push_dims AS t2 ON t1.key = t2.key GROUP BY t1.key;

SELECT '-- 24. negative: FULL JOIN never pushes';
EXPLAIN SELECT count() FROM t_push_facts AS t1 FULL JOIN t_push_dims AS t2 ON t1.key = t2.key GROUP BY t1.key;

SELECT '-- 25. negative: ASOF JOIN never pushes';
EXPLAIN SELECT count() FROM t_push_facts AS t1 ASOF JOIN t_push_dims_multi AS t2 ON t1.key = t2.key AND t1.value >= t2.threshold GROUP BY t1.key;

SELECT '-- 26. task-budget sanity: 3 joins under an aggregation must not exhaust the task limit';
-- asserts that the cascades planner produces a distributed plan without a budget exception; the
-- shape is deterministic (the preamble pins the join-order, join-swap and runtime-filter
-- settings session-wide). The classic shape wins here: `t_push_dims_multi` has no stat-hint
-- entry at this point, so the pushed join subtree lacks the estimates the cardinality gate
-- needs and no pushdown alternative is built. `use_hash_table_stats_for_join_reordering` is
-- pinned to its default because this is the only canary with three joins, so the join-order
-- search has freedom: the msan flaky check (2026-09-02) flipped the `t_push_dims` /
-- `t_push_dims_multi` sibling order under the randomized value 0.
EXPLAIN SELECT count() FROM t_push_facts AS t1
  LEFT JOIN t_push_dims AS t2 ON t1.key = t2.key
  LEFT JOIN t_push_dims_multi AS t3 ON t1.key = t3.key
  LEFT JOIN t_push_dims AS t4 ON t1.key = t4.key
GROUP BY t1.key
SETTINGS use_hash_table_stats_for_join_reordering = 1;

-- Nested aggregation, pinned as a stable no-cascade shape (not exercising the new cascade
-- capability): the outer `Aggregating` sits above the inner query's variant-B pushdown group
-- through one identity `Expression` step (the subquery-boundary "Change column names to column
-- identifiers" translation, folded into that step). Structurally the outer rule COULD match the
-- inner's pushed `JoinLogical` alternative there, but `checkPattern` for the outer `Aggregating`
-- runs via `scheduleApplicableRules` synchronously from its own `ExploreExpressionTask` (which
-- runs once per group expression) - before its child's `ExploreGroupTask` (and hence the inner
-- aggregation's own `AggregationPushdown` application, several groups further down) ever runs.
-- `checkPattern` sees no join candidate yet and returns false, so `applyImpl` (which does run
-- after full child exploration, once scheduled) never gets scheduled for this expression, and
-- `ExploreExpressionTask` is never re-run later to give `checkPattern` another look. This is the
-- "once per source expression" engine bound from the rule's own doc comment, orthogonal to the
-- enumerate-all change: the classic reused-alternative shape below is what wins instead.
SELECT '-- 27. nested aggregation over a variant-B pushdown: outer aggregation reuses the pushed join as-is (no further cascade, see comment above)';
EXPLAIN SELECT k, max(c) AS total FROM (SELECT t1.key AS k, count() AS c FROM t_push_facts AS t1 LEFT ANY JOIN t_push_dims AS t2 ON t1.key = t2.key GROUP BY t1.key) GROUP BY k;

-- `distributed_plan_force_shuffle_aggregation` forbids only the partial + merge split (variant A);
-- variant B keeps a single final aggregation and must stay available under it. The expensive
-- build side is what makes the pushed plan win here: the aggregation shuffles the facts either
-- way under the setting, but probing the 50M-row hash table with ~100 aggregated rows beats
-- probing it with 100M raw ones. The pinned shape (the single `Aggregating` BELOW the join) is
-- itself the proof that the rule fired.
SELECT '-- 28. force-shuffle: variant B still fires under distributed_plan_force_shuffle_aggregation';
SET param__internal_join_table_stat_hints = '{"t_push_facts": {"cardinality": 100000000, "avg_row_bytes": 12, "distinct_keys": {"key": 100}}, "t_push_dims": {"cardinality": 50000000, "avg_row_bytes": 20, "distinct_keys": {"key": 50000000}}}';
EXPLAIN SELECT count() FROM t_push_facts AS t1 LEFT SEMI JOIN t_push_dims AS t2 ON t1.key = t2.key GROUP BY t1.key
SETTINGS distributed_plan_force_shuffle_aggregation = 1;
SET param__internal_join_table_stat_hints = '{"t_push_facts": {"cardinality": 100000000, "avg_row_bytes": 12, "distinct_keys": {"key": 100}}, "t_push_dims": {"cardinality": 1000, "avg_row_bytes": 20, "distinct_keys": {"key": 1000}}}';

SELECT '-- 29. force-shuffle: an A-shaped query (case 1''s) is not pushed, classic shuffle shape';
EXPLAIN SELECT count() FROM t_push_facts AS t1 LEFT JOIN t_push_dims AS t2 ON t1.key = t2.key GROUP BY t1.key
SETTINGS distributed_plan_force_shuffle_aggregation = 1;

DROP TABLE t_push_facts;
DROP TABLE t_push_dims;
DROP TABLE t_push_dims_multi;
