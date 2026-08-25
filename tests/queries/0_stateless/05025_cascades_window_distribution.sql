-- Tags: no-darwin, no-old-analyzer
-- no-darwin: distributed execution uses the streaming exchange, which is implemented only on Linux.
-- no-old-analyzer: distributed Cascades planning requires the analyzer.

-- A window with PARTITION BY runs on each node: the input is shuffled so that rows with
-- equal partition keys are on one node, then each node sorts its part and computes its
-- windows on its own. A window without PARTITION BY stays on a single node.

SET enable_analyzer = 1;
SET explain_query_plan_default = 'legacy';
SET make_distributed_plan = 1;
SET enable_cascades_optimizer = 1;
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET enable_join_runtime_filters = 0;
SET max_rows_to_group_by = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET param__internal_cascades_cluster_node_count = 4;
-- A high work weight makes the sort and window work decide the plan: splitting that work
-- across nodes saves far more than the extra exchange costs.
SET param__internal_cascades_cost_config = '{"work_weight":1000,"network_weight":0.01,"sequential_weight":1}';

DROP TABLE IF EXISTS t_win;

CREATE TABLE t_win (k UInt64, v UInt64) ENGINE = MergeTree() ORDER BY k
  SETTINGS auto_statistics_types = '';
-- a merge between planning and the worker read would invalidate the planned part names
SYSTEM STOP MERGES t_win;
INSERT INTO t_win SELECT number % 1000, number FROM numbers(100000);

SELECT '-- 1. PARTITION BY window runs per node: shuffle by the partition key, sort and window on each node';
EXPLAIN SELECT k, v, sum(v) OVER (PARTITION BY k ORDER BY v) FROM t_win;

SELECT '-- 2. results match the single-node plan';
SELECT count(), sum(v), sum(s) FROM (
    SELECT k, v, sum(v) OVER (PARTITION BY k ORDER BY v) AS s FROM t_win
);
SELECT count(), sum(v), sum(s) FROM (
    SELECT k, v, sum(v) OVER (PARTITION BY k ORDER BY v) AS s FROM t_win
) SETTINGS enable_cascades_optimizer = 0, make_distributed_plan = 0;

SELECT '-- 3. a window without PARTITION BY stays on a single node';
EXPLAIN SELECT k, v, sum(v) OVER () FROM t_win;

-- A float partition key hashes `-0.` and `0.` differently while they compare as one
-- partition, so a per-node window would split that partition across nodes.
DROP TABLE IF EXISTS t_wfloat;
CREATE TABLE t_wfloat (k Float64, v UInt64) ENGINE = MergeTree() ORDER BY v
  SETTINGS auto_statistics_types = '';
SYSTEM STOP MERGES t_wfloat;
INSERT INTO t_wfloat SELECT if(number % 2 = 0, 0., -0.), number FROM numbers(100000);

SELECT '-- 4. a float partition key keeps the window on a single node';
EXPLAIN SELECT k, sum(v) OVER (PARTITION BY k) FROM t_wfloat;

SELECT '-- 5. negative and positive zero form one partition, as in the single-node plan';
SELECT DISTINCT c FROM (SELECT count() OVER (PARTITION BY k) AS c FROM t_wfloat) ORDER BY c;
SELECT DISTINCT c FROM (SELECT count() OVER (PARTITION BY k) AS c FROM t_wfloat) ORDER BY c
SETTINGS enable_cascades_optimizer = 0, make_distributed_plan = 0, max_threads = 1;

-- The per-node window runs without the stream fan-out, so its output keeps the sort order
-- and a matching ORDER BY above needs no new sort, only the order-keeping gather.
SELECT '-- 6. the window output order is reused: no new sort for a matching ORDER BY';
EXPLAIN SELECT k, v, sum(v) OVER (PARTITION BY k ORDER BY v) AS s FROM t_win ORDER BY k, v;

-- The sort below the window carries the partition keys, so each node's streams stay
-- disjoint on `k`; the aggregation on `k` above the window then aggregates each stream
-- on its own, without the merge phase.
-- The outer query reads the plan text through `viewExplain`, which distributed Cascades
-- planning rejects, so the outer level turns it off.
SELECT '-- 7. the window sort keeps per-partition streams: the aggregation above skips merging';
SELECT countIf(explain LIKE '%Skip merging: 1%') > 0 FROM (
    EXPLAIN actions = 1 SELECT k, sum(s) FROM (SELECT k, sum(v) OVER (PARTITION BY k) AS s FROM t_win) GROUP BY k
    SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1, distributed_plan_force_shuffle_aggregation = 1
) SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

DROP TABLE t_win;
DROP TABLE t_wfloat;
