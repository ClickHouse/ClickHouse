-- Tags: no-parallel-replicas
-- Tag no-parallel-replicas: the EXPLAIN checks assert on the local read step.

-- A condition with observable side effects (`sleepEachRow`) must run on every candidate row, so the top-K
-- threshold filter is not added below it. This includes the `ALIAS` column shape, where the `ALIAS`
-- expression is lifted above the `Sort`.

SET explain_query_plan_default = 'legacy';
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET use_top_k_dynamic_filtering = 1;
SET use_skip_indexes_for_top_k = 0;
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1;
SET optimize_read_in_order = 1;

DROP TABLE IF EXISTS t_topk_side_effect;
CREATE TABLE t_topk_side_effect (time UInt32, body String, body_alias String ALIAS concat(body, '!'))
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;
INSERT INTO t_topk_side_effect SELECT number, toString(number) FROM numbers(1000);

-- Without side effects the threshold filter is added, also for the `ALIAS` shape.
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT time FROM t_topk_side_effect WHERE body != '' ORDER BY time DESC LIMIT 10)
WHERE explain ILIKE '%FUNCTION \_\_topKFilter%';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT body_alias FROM t_topk_side_effect WHERE body != '' ORDER BY time DESC LIMIT 10)
WHERE explain ILIKE '%FUNCTION \_\_topKFilter%';

-- With `sleepEachRow` in the condition it is not.
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT time FROM t_topk_side_effect WHERE sleepEachRow(0) = 0 ORDER BY time DESC LIMIT 10)
WHERE explain ILIKE '%FUNCTION \_\_topKFilter%';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT body_alias FROM t_topk_side_effect WHERE sleepEachRow(0) = 0 ORDER BY time DESC LIMIT 10)
WHERE explain ILIKE '%FUNCTION \_\_topKFilter%';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT body_alias FROM t_topk_side_effect PREWHERE sleepEachRow(0) = 0 ORDER BY time DESC LIMIT 10)
WHERE explain ILIKE '%FUNCTION \_\_topKFilter%';

SELECT groupArray(body_alias) FROM (SELECT body_alias FROM t_topk_side_effect WHERE sleepEachRow(0) = 0 ORDER BY time DESC LIMIT 5);

DROP TABLE t_topk_side_effect;
