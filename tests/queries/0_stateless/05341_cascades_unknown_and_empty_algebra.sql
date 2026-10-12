-- Every operator of the Cascades derivation decides for itself what an unknown or a proven-empty
-- input means: a `UNION ALL` with an unknown input is unknown whatever its other inputs, an inner
-- join with a proven-empty side is empty even when the other side is unknown, a semi join over an
-- empty side is empty, a keyless aggregation of an unknown input is its one row while a keyed one
-- is unknown, and an any join emits each left row once. The cost token is stripped from the plan
-- lines: this test is about rows, and every change of a cost constant would move it otherwise.
SET enable_analyzer = 1;
-- A filter and the equi-join filter propagated onto it print as one step; the runner randomizes the merge.
SET query_plan_merge_filters = 1;
-- Each filter stays a step of its own with its own estimate; moved into the read it would vanish
-- from the plan lines, and the move depends on the storage and is randomized by the runner.
SET optimize_move_to_prewhere = 0;
SET enable_parallel_replicas = 0;
SET explain_query_plan_default = 'legacy';
SET enable_join_runtime_filters = 0;
SET query_plan_optimize_join_order_limit = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 0;
SET use_statistics = 0;
SET max_rows_to_group_by = 0;
SET param__internal_cascades_cluster_node_count = 3;
SET make_distributed_plan = 1;
SET enable_cascades_optimizer = 1;
SET param__internal_join_table_stat_hints = '{"t_ua_known": {"cardinality": 1000, "distinct_keys": {"k": 1000}}}';

DROP TABLE IF EXISTS t_ua_known;
DROP TABLE IF EXISTS t_ua_unk;
DROP TABLE IF EXISTS t_ua_empty;
CREATE TABLE t_ua_known (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
CREATE TABLE t_ua_unk (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
CREATE TABLE t_ua_empty (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
INSERT INTO t_ua_known SELECT number, number FROM numbers(1000);
INSERT INTO t_ua_unk SELECT number, number FROM numbers(100);
INSERT INTO t_ua_empty SELECT number, number FROM numbers(10);

SELECT '-- UNION ALL: a proven-empty first input does not make an unknown second input known';
SELECT replaceRegexpAll(explain, ', cost: [0-9.]+', '') FROM (
EXPLAIN estimates = 1
SELECT count() FROM (SELECT k FROM t_ua_empty WHERE k > 1000000 UNION ALL SELECT k FROM t_ua_unk WHERE v < 5)
);

SELECT '-- inner join: a proven-empty side decides, the unknown other side does not matter';
SELECT replaceRegexpAll(explain, ', cost: [0-9.]+', '') FROM (
EXPLAIN estimates = 1
SELECT count() FROM (SELECT k FROM t_ua_empty WHERE k > 1000000) AS e JOIN (SELECT k FROM t_ua_unk WHERE v < 5) AS u ON e.k = u.k
);

SELECT '-- semi join over an empty other side: empty';
SELECT replaceRegexpAll(explain, ', cost: [0-9.]+', '') FROM (
EXPLAIN estimates = 1
SELECT count() FROM t_ua_known AS a LEFT SEMI JOIN (SELECT k FROM t_ua_empty WHERE k > 1000000) AS e ON a.k = e.k
);

SELECT '-- aggregation of an unknown input: keyless is one row, keyed is unknown';
SELECT replaceRegexpAll(explain, ', cost: [0-9.]+', '') FROM (
EXPLAIN estimates = 1
SELECT count() FROM t_ua_unk WHERE v < 5
);
SELECT replaceRegexpAll(explain, ', cost: [0-9.]+', '') FROM (
EXPLAIN estimates = 1
SELECT k, count() FROM t_ua_unk WHERE v < 5 GROUP BY k
);

SELECT '-- left any join: the left side';
SELECT replaceRegexpAll(explain, ', cost: [0-9.]+', '') FROM (
EXPLAIN estimates = 1
SELECT count() FROM t_ua_known AS a LEFT ANY JOIN t_ua_unk AS u ON a.k = u.k
);

DROP TABLE t_ua_known;
DROP TABLE t_ua_unk;
DROP TABLE t_ua_empty;
