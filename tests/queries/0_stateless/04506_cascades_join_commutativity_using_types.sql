-- Tags: no-darwin, no-old-analyzer, no-flaky-check
-- no-flaky-check: every distributed-plan statement pays for a full optimizer run and multi-stage
-- execution, which is ~50x slower in debug builds; the flaky check's repeated runs exceed its budget.
-- no-darwin: distributed execution uses the streaming exchange, which is implemented only on Linux.
-- no-old-analyzer: distributed planning requires the analyzer.

-- `JoinCommutativity` swaps joins whose `USING` clause casts a mismatched key type to the
-- supertype: `swapInputs` remaps the cast and the `join_use_nulls` wrappers to the new sides.
-- Every join type the rule can swap is checked: the swap must win on cost (statistics hints
-- make the right side look huge, so the swapped broadcast build is much cheaper) and the
-- results must match the plain plan. The physical tables stay tiny to keep the test fast.
-- Keys are unique on each side, so ANY joins are deterministic.

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET max_rows_to_group_by = 0;
SET param__internal_cascades_cluster_node_count = 4;
SET query_plan_optimize_join_order_randomize = 0;
SET param__internal_join_table_stat_hints = '{"t_uc_small": {"cardinality": 100, "avg_row_bytes": 20, "distinct_keys": {"k": 100}}, "t_uc_big": {"cardinality": 100000000, "avg_row_bytes": 20, "distinct_keys": {"k": 1000000}}}';

DROP TABLE IF EXISTS t_uc_small;
DROP TABLE IF EXISTS t_uc_big;

-- UInt32 vs Int64 keys: USING (k) compares both sides as the Int64 supertype
CREATE TABLE t_uc_small (k UInt32, v String) ENGINE = MergeTree() ORDER BY k
  SETTINGS auto_statistics_types = '';
CREATE TABLE t_uc_big (k Int64, w String) ENGINE = MergeTree() ORDER BY k
  SETTINGS auto_statistics_types = '';
SYSTEM STOP MERGES t_uc_small;
SYSTEM STOP MERGES t_uc_big;

-- keys 0..19 match the big side; 4000000000 and 4000000001 do not
INSERT INTO t_uc_small SELECT number, 'v' || toString(number) FROM numbers(20);
INSERT INTO t_uc_small VALUES (4000000000, 'v_a'), (4000000001, 'v_b');
-- keys 0..99 and two negative keys that never match the UInt32 side
INSERT INTO t_uc_big SELECT number, 'w' || toString(number) FROM numbers(100);
INSERT INTO t_uc_big VALUES (-1, 'w_neg1'), (-2, 'w_neg2');

SELECT '-- 1. the swapped variant wins for every join type the rule can swap';
SELECT 'INNER', sum(explain LIKE '%swapped%') FROM (
    EXPLAIN PLAN keep_logical_steps = 1
    SELECT count() FROM t_uc_small INNER JOIN t_uc_big USING (k)
    SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1
);
SELECT 'LEFT ANY', sum(explain LIKE '%swapped%') FROM (
    EXPLAIN PLAN keep_logical_steps = 1
    SELECT count() FROM t_uc_small LEFT ANY JOIN t_uc_big USING (k)
    SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1
);
SELECT 'RIGHT ANY', sum(explain LIKE '%swapped%') FROM (
    EXPLAIN PLAN keep_logical_steps = 1
    SELECT count() FROM t_uc_small RIGHT ANY JOIN t_uc_big USING (k)
    SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1
);
SELECT 'LEFT SEMI', sum(explain LIKE '%swapped%') FROM (
    EXPLAIN PLAN keep_logical_steps = 1
    SELECT count() FROM t_uc_small LEFT SEMI JOIN t_uc_big USING (k)
    SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1
);
SELECT 'RIGHT SEMI', sum(explain LIKE '%swapped%') FROM (
    EXPLAIN PLAN keep_logical_steps = 1
    SELECT count() FROM t_uc_small RIGHT SEMI JOIN t_uc_big USING (k)
    SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1
);
SELECT 'LEFT ANTI', sum(explain LIKE '%swapped%') FROM (
    EXPLAIN PLAN keep_logical_steps = 1
    SELECT count() FROM t_uc_small LEFT ANTI JOIN t_uc_big USING (k)
    SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1
);
SELECT 'RIGHT ANTI', sum(explain LIKE '%swapped%') FROM (
    EXPLAIN PLAN keep_logical_steps = 1
    SELECT count() FROM t_uc_small RIGHT ANTI JOIN t_uc_big USING (k)
    SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1
);

-- Each pair below must print identical numbers: first from the swapped distributed plan,
-- then from the plain single-node plan.

SELECT '-- 2. results match the plain plan, join_use_nulls = 0';
SET join_use_nulls = 0;
SELECT 'INNER swapped', count(), sum(k), sum(length(v)), sum(length(w)) FROM t_uc_small INNER JOIN t_uc_big USING (k)
  SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1;
SELECT 'INNER plain', count(), sum(k), sum(length(v)), sum(length(w)) FROM t_uc_small INNER JOIN t_uc_big USING (k)
  SETTINGS enable_cascades_optimizer = 0, make_distributed_plan = 0;
SELECT 'LEFT ANY swapped', count(), sum(k), sum(length(v)), sum(length(w)) FROM t_uc_small LEFT ANY JOIN t_uc_big USING (k)
  SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1;
SELECT 'LEFT ANY plain', count(), sum(k), sum(length(v)), sum(length(w)) FROM t_uc_small LEFT ANY JOIN t_uc_big USING (k)
  SETTINGS enable_cascades_optimizer = 0, make_distributed_plan = 0;
SELECT 'RIGHT ANY swapped', count(), sum(k), sum(length(v)), sum(length(w)) FROM t_uc_small RIGHT ANY JOIN t_uc_big USING (k)
  SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1;
SELECT 'RIGHT ANY plain', count(), sum(k), sum(length(v)), sum(length(w)) FROM t_uc_small RIGHT ANY JOIN t_uc_big USING (k)
  SETTINGS enable_cascades_optimizer = 0, make_distributed_plan = 0;
SELECT 'LEFT SEMI swapped', count(), sum(k), sum(length(v)) FROM t_uc_small LEFT SEMI JOIN t_uc_big USING (k)
  SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1;
SELECT 'LEFT SEMI plain', count(), sum(k), sum(length(v)) FROM t_uc_small LEFT SEMI JOIN t_uc_big USING (k)
  SETTINGS enable_cascades_optimizer = 0, make_distributed_plan = 0;
SELECT 'RIGHT SEMI swapped', count(), sum(k), sum(length(w)) FROM t_uc_small RIGHT SEMI JOIN t_uc_big USING (k)
  SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1;
SELECT 'RIGHT SEMI plain', count(), sum(k), sum(length(w)) FROM t_uc_small RIGHT SEMI JOIN t_uc_big USING (k)
  SETTINGS enable_cascades_optimizer = 0, make_distributed_plan = 0;
SELECT 'LEFT ANTI swapped', count(), sum(k), sum(length(v)) FROM t_uc_small LEFT ANTI JOIN t_uc_big USING (k)
  SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1;
SELECT 'LEFT ANTI plain', count(), sum(k), sum(length(v)) FROM t_uc_small LEFT ANTI JOIN t_uc_big USING (k)
  SETTINGS enable_cascades_optimizer = 0, make_distributed_plan = 0;
SELECT 'RIGHT ANTI swapped', count(), sum(k), sum(length(w)) FROM t_uc_small RIGHT ANTI JOIN t_uc_big USING (k)
  SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1;
SELECT 'RIGHT ANTI plain', count(), sum(k), sum(length(w)) FROM t_uc_small RIGHT ANTI JOIN t_uc_big USING (k)
  SETTINGS enable_cascades_optimizer = 0, make_distributed_plan = 0;

SELECT '-- 3. results match the plain plan, join_use_nulls = 1';
SET join_use_nulls = 1;
SELECT 'INNER swapped', count(), sum(k), sum(length(v)), sum(length(w)) FROM t_uc_small INNER JOIN t_uc_big USING (k)
  SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1;
SELECT 'INNER plain', count(), sum(k), sum(length(v)), sum(length(w)) FROM t_uc_small INNER JOIN t_uc_big USING (k)
  SETTINGS enable_cascades_optimizer = 0, make_distributed_plan = 0;
SELECT 'LEFT ANY swapped', count(), sum(k), sum(length(v)), sum(length(w)) FROM t_uc_small LEFT ANY JOIN t_uc_big USING (k)
  SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1;
SELECT 'LEFT ANY plain', count(), sum(k), sum(length(v)), sum(length(w)) FROM t_uc_small LEFT ANY JOIN t_uc_big USING (k)
  SETTINGS enable_cascades_optimizer = 0, make_distributed_plan = 0;
SELECT 'RIGHT ANY swapped', count(), sum(k), sum(length(v)), sum(length(w)) FROM t_uc_small RIGHT ANY JOIN t_uc_big USING (k)
  SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1;
SELECT 'RIGHT ANY plain', count(), sum(k), sum(length(v)), sum(length(w)) FROM t_uc_small RIGHT ANY JOIN t_uc_big USING (k)
  SETTINGS enable_cascades_optimizer = 0, make_distributed_plan = 0;
SELECT 'LEFT SEMI swapped', count(), sum(k), sum(length(v)) FROM t_uc_small LEFT SEMI JOIN t_uc_big USING (k)
  SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1;
SELECT 'LEFT SEMI plain', count(), sum(k), sum(length(v)) FROM t_uc_small LEFT SEMI JOIN t_uc_big USING (k)
  SETTINGS enable_cascades_optimizer = 0, make_distributed_plan = 0;
SELECT 'RIGHT SEMI swapped', count(), sum(k), sum(length(w)) FROM t_uc_small RIGHT SEMI JOIN t_uc_big USING (k)
  SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1;
SELECT 'RIGHT SEMI plain', count(), sum(k), sum(length(w)) FROM t_uc_small RIGHT SEMI JOIN t_uc_big USING (k)
  SETTINGS enable_cascades_optimizer = 0, make_distributed_plan = 0;
SELECT 'LEFT ANTI swapped', count(), sum(k), sum(length(v)) FROM t_uc_small LEFT ANTI JOIN t_uc_big USING (k)
  SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1;
SELECT 'LEFT ANTI plain', count(), sum(k), sum(length(v)) FROM t_uc_small LEFT ANTI JOIN t_uc_big USING (k)
  SETTINGS enable_cascades_optimizer = 0, make_distributed_plan = 0;
SELECT 'RIGHT ANTI swapped', count(), sum(k), sum(length(w)) FROM t_uc_small RIGHT ANTI JOIN t_uc_big USING (k)
  SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1;
SELECT 'RIGHT ANTI plain', count(), sum(k), sum(length(w)) FROM t_uc_small RIGHT ANTI JOIN t_uc_big USING (k)
  SETTINGS enable_cascades_optimizer = 0, make_distributed_plan = 0;

SELECT '-- 4. the coerced key type; right-side columns become Nullable only with join_use_nulls';
SET join_use_nulls = 1;
SELECT toTypeName(k), toTypeName(w), k, v, w FROM t_uc_small LEFT ANY JOIN t_uc_big USING (k) ORDER BY k LIMIT 3
  SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1;
SELECT toTypeName(k), toTypeName(w), k, v, w FROM t_uc_small LEFT ANY JOIN t_uc_big USING (k) ORDER BY k LIMIT 3
  SETTINGS enable_cascades_optimizer = 0, make_distributed_plan = 0;
SET join_use_nulls = 0;
SELECT toTypeName(k), toTypeName(w), k, v, w FROM t_uc_small LEFT ANY JOIN t_uc_big USING (k) ORDER BY k LIMIT 3
  SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1;
SELECT toTypeName(k), toTypeName(w), k, v, w FROM t_uc_small LEFT ANY JOIN t_uc_big USING (k) ORDER BY k LIMIT 3
  SETTINGS enable_cascades_optimizer = 0, make_distributed_plan = 0;

DROP TABLE t_uc_small;
DROP TABLE t_uc_big;
