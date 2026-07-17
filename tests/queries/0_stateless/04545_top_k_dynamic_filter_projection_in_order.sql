-- Regression test for issue #110862: TopK dynamic filtering must also be disabled
-- when the read is made in-order by a selected sorting projection, not only by the
-- base table's sorting-key prefix. Otherwise a redundant/counterproductive
-- __topKFilter prewhere is installed on top of the projection read, re-reading the
-- sort column that InOrder reading already provides.

DROP TABLE IF EXISTS t_topk_proj_rio;

CREATE TABLE t_topk_proj_rio (id UInt64, k UInt64, score UInt64, payload String CODEC(NONE))
ENGINE = MergeTree ORDER BY (k, id)
SETTINGS index_granularity = 256, min_bytes_for_wide_part = 0;

INSERT INTO t_topk_proj_rio
SELECT number, number % 128, sipHash64(number), toString(number) FROM numbers(32768);

OPTIMIZE TABLE t_topk_proj_rio FINAL;

-- Sorting projection ordered by (score, id): serves `ORDER BY score, id` in-order.
ALTER TABLE t_topk_proj_rio ADD PROJECTION p_score (SELECT id, k, score, payload ORDER BY (score, id));
ALTER TABLE t_topk_proj_rio MATERIALIZE PROJECTION p_score SETTINGS mutations_sync = 2;

-- Correctness: results are identical with and without dynamic filtering.
SELECT id FROM t_topk_proj_rio ORDER BY score, id LIMIT 5
SETTINGS optimize_read_in_order = 1, optimize_use_projections = 1, use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 100;

-- The read is served in-order by the p_score projection, so NO __topKFilter must be
-- installed (expected 0). Before the fix this reported 1.
SELECT count() > 0 AS has_topk_filter
FROM (
    EXPLAIN projections = 1, actions = 1
    SELECT id, cityHash64(payload) FROM t_topk_proj_rio ORDER BY score, id LIMIT 10
    SETTINGS optimize_read_in_order = 1, optimize_use_projections = 1, use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 100
)
WHERE explain ILIKE '%__topKFilter%';

-- The projection is still selected and the read is InOrder (the projection does the work).
SELECT count() > 0 AS uses_projection_in_order
FROM (
    EXPLAIN projections = 1
    SELECT id, cityHash64(payload) FROM t_topk_proj_rio ORDER BY score, id LIMIT 10
    SETTINGS optimize_read_in_order = 1, optimize_use_projections = 1, use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 100
)
WHERE explain ILIKE '%InOrder%';

-- Sanity: with no projection able to serve the order (ORDER BY score without a matching
-- projection covering all read columns), dynamic filtering must STILL be applied so the
-- fix does not over-disable the optimization (expected 1).
DROP TABLE IF EXISTS t_topk_noproj;
CREATE TABLE t_topk_noproj (id UInt64, k UInt64, score UInt64, payload String CODEC(NONE))
ENGINE = MergeTree ORDER BY (k, id)
SETTINGS index_granularity = 256, min_bytes_for_wide_part = 0;
INSERT INTO t_topk_noproj SELECT number, number % 128, sipHash64(number), toString(number) FROM numbers(32768);
OPTIMIZE TABLE t_topk_noproj FINAL;

SELECT count() > 0 AS has_topk_filter
FROM (
    EXPLAIN actions = 1
    SELECT id, cityHash64(payload) FROM t_topk_noproj ORDER BY score, id LIMIT 10
    SETTINGS optimize_read_in_order = 1, optimize_use_projections = 1, use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 100
)
WHERE explain ILIKE '%__topKFilter%';

-- The predictor must not be stronger than the second-pass projection chooser. When the query
-- has a filter, the chooser may keep the cheaper filtered base-table read and reject the
-- sorting projection on cost, so dynamic filtering must STILL be applied to the base read
-- (expected 1). Before the guard was tightened this reported 0.
SELECT count() > 0 AS has_topk_filter
FROM (
    EXPLAIN projections = 1, actions = 1
    SELECT id, cityHash64(payload) FROM t_topk_proj_rio WHERE k = 7 ORDER BY score, id LIMIT 10
    SETTINGS optimize_read_in_order = 1, optimize_use_projections = 1, use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 100
)
WHERE explain ILIKE '%__topKFilter%';

-- When a matching sorting projection exists but `preferred_optimize_projection_name` pins a
-- different (non-matching) projection, the chooser only ever considers the pinned one, so the
-- read is not made in-order by the matching projection and dynamic filtering must STILL apply
-- (expected 1). Before the guard was tightened this reported 0.
ALTER TABLE t_topk_proj_rio ADD PROJECTION p_other (SELECT id, k, score, payload ORDER BY (k, score));
ALTER TABLE t_topk_proj_rio MATERIALIZE PROJECTION p_other SETTINGS mutations_sync = 2;

SELECT count() > 0 AS has_topk_filter
FROM (
    EXPLAIN projections = 1, actions = 1
    SELECT id, cityHash64(payload) FROM t_topk_proj_rio ORDER BY score, id LIMIT 10
    SETTINGS optimize_read_in_order = 1, optimize_use_projections = 1, use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 100, preferred_optimize_projection_name = 'p_other'
)
WHERE explain ILIKE '%__topKFilter%';

DROP TABLE t_topk_proj_rio;
DROP TABLE t_topk_noproj;
