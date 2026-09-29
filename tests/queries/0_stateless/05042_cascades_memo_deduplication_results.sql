-- Memo-wide group deduplication (`cascades_memo_deduplication`, experimental and off by default)
-- lets a plan subtree that computes an already-known relation join that relation's memo group
-- instead of creating a new one. It is a search-space optimization, so it may never change rows:
-- every query below is run with the setting off and on, and the two must return the same result.
-- Repeated-subtree shapes are the ones deduplication acts on: a self-join, the same aggregated
-- subquery twice, equal UNION branches, and a top-N over a repeated aggregation.
-- Only results are asserted; the plan shape is free to differ (group numbering, costed tie-breaks).

SET enable_analyzer = 1;
SET make_distributed_plan = 1;
SET enable_cascades_optimizer = 1;
SET distributed_plan_execute_locally = 1;
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET enable_join_runtime_filters = 0;
-- The Fast test profile sets a non-zero max_rows_to_group_by, which keeps aggregations local.
SET max_rows_to_group_by = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET param__internal_cascades_cluster_node_count = 4;

DROP TABLE IF EXISTS memo_dedup_t;
CREATE TABLE memo_dedup_t (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
INSERT INTO memo_dedup_t SELECT number % 1000, number FROM numbers(10000);

SELECT '-- self-join';
SELECT count(), sum(l.v + r.v) FROM memo_dedup_t AS l JOIN memo_dedup_t AS r ON l.k = r.k WHERE l.k < 5
SETTINGS cascades_memo_deduplication = 0;
SELECT count(), sum(l.v + r.v) FROM memo_dedup_t AS l JOIN memo_dedup_t AS r ON l.k = r.k WHERE l.k < 5
SETTINGS cascades_memo_deduplication = 1;

SELECT '-- the same aggregated subquery on both sides of a join';
SELECT count(), sum(a.s + b.s) FROM (SELECT k, sum(v) AS s FROM memo_dedup_t GROUP BY k) AS a
JOIN (SELECT k, sum(v) AS s FROM memo_dedup_t GROUP BY k) AS b ON a.k = b.k
SETTINGS cascades_memo_deduplication = 0;
SELECT count(), sum(a.s + b.s) FROM (SELECT k, sum(v) AS s FROM memo_dedup_t GROUP BY k) AS a
JOIN (SELECT k, sum(v) AS s FROM memo_dedup_t GROUP BY k) AS b ON a.k = b.k
SETTINGS cascades_memo_deduplication = 1;

SELECT '-- equal UNION ALL branches';
SELECT count(), sum(s) FROM
(SELECT k, sum(v) AS s FROM memo_dedup_t GROUP BY k UNION ALL SELECT k, sum(v) AS s FROM memo_dedup_t GROUP BY k)
SETTINGS cascades_memo_deduplication = 0;
SELECT count(), sum(s) FROM
(SELECT k, sum(v) AS s FROM memo_dedup_t GROUP BY k UNION ALL SELECT k, sum(v) AS s FROM memo_dedup_t GROUP BY k)
SETTINGS cascades_memo_deduplication = 1;

SELECT '-- top-N over a repeated aggregation';
SELECT k, s FROM (SELECT k, sum(v) AS s FROM memo_dedup_t GROUP BY k) ORDER BY s DESC, k LIMIT 3
SETTINGS cascades_memo_deduplication = 0;
SELECT k, s FROM (SELECT k, sum(v) AS s FROM memo_dedup_t GROUP BY k) ORDER BY s DESC, k LIMIT 3
SETTINGS cascades_memo_deduplication = 1;

DROP TABLE memo_dedup_t;
