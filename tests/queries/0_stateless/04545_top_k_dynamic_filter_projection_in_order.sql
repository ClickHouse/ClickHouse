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

-- A filtered read is not made in-order by the projection, so dynamic filtering must STILL be
-- applied (expected 1) and the read must not be in order (expected 0). The read-order assertion
-- is what makes the first one meaningful: without it the arm passes whenever the filter is
-- present, whatever the plan does.
SELECT count() > 0 AS has_topk_filter
FROM (
    EXPLAIN projections = 1, actions = 1
    SELECT id, cityHash64(payload) FROM t_topk_proj_rio WHERE k = 7 ORDER BY score, id LIMIT 10
    SETTINGS optimize_read_in_order = 1, optimize_use_projections = 1, use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 100
)
WHERE explain ILIKE '%__topKFilter%';

SELECT count() > 0 AS filtered_in_order
FROM (
    EXPLAIN projections = 1
    SELECT id, cityHash64(payload) FROM t_topk_proj_rio WHERE k = 7 ORDER BY score, id LIMIT 10
    SETTINGS optimize_read_in_order = 1, optimize_use_projections = 1, use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 100
)
WHERE explain ILIKE '%InOrder%';

-- A pin naming an existing projection narrows the candidate set to it, so the matching sorting
-- projection is not selected, the read is not in order (expected 0) and dynamic filtering must
-- STILL apply (expected 1).
ALTER TABLE t_topk_proj_rio ADD PROJECTION p_other (SELECT id, k, score, payload ORDER BY (k, score));
ALTER TABLE t_topk_proj_rio MATERIALIZE PROJECTION p_other SETTINGS mutations_sync = 2;

SELECT count() > 0 AS has_topk_filter
FROM (
    EXPLAIN projections = 1, actions = 1
    SELECT id, cityHash64(payload) FROM t_topk_proj_rio ORDER BY score, id LIMIT 10
    SETTINGS optimize_read_in_order = 1, optimize_use_projections = 1, use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 100, preferred_optimize_projection_name = 'p_other'
)
WHERE explain ILIKE '%__topKFilter%';

SELECT count() > 0 AS pinned_in_order
FROM (
    EXPLAIN projections = 1
    SELECT id, cityHash64(payload) FROM t_topk_proj_rio ORDER BY score, id LIMIT 10
    SETTINGS optimize_read_in_order = 1, optimize_use_projections = 1, use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 100, preferred_optimize_projection_name = 'p_other'
)
WHERE explain ILIKE '%InOrder%';

-- A pin naming no existing projection does not narrow the candidate set, so the matching
-- projection is still selected and serves the read in-order: dynamic filtering must be disabled
-- (expected 0), and the projection must really be the one serving the read (expected 1, 1).
SELECT count() > 0 AS has_topk_filter
FROM (
    EXPLAIN projections = 1, actions = 1
    SELECT id, cityHash64(payload) FROM t_topk_proj_rio ORDER BY score, id LIMIT 10
    SETTINGS optimize_read_in_order = 1, optimize_use_projections = 1, use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 100, preferred_optimize_projection_name = 'does_not_exist'
)
WHERE explain ILIKE '%__topKFilter%';

SELECT count() > 0 AS unpinned_uses_p_score
FROM (
    EXPLAIN projections = 1
    SELECT id, cityHash64(payload) FROM t_topk_proj_rio ORDER BY score, id LIMIT 10
    SETTINGS optimize_read_in_order = 1, optimize_use_projections = 1, use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 100, preferred_optimize_projection_name = 'does_not_exist'
)
WHERE explain ILIKE '%p_score%';

SELECT count() > 0 AS unpinned_in_order
FROM (
    EXPLAIN projections = 1
    SELECT id, cityHash64(payload) FROM t_topk_proj_rio ORDER BY score, id LIMIT 10
    SETTINGS optimize_read_in_order = 1, optimize_use_projections = 1, use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 100, preferred_optimize_projection_name = 'does_not_exist'
)
WHERE explain ILIKE '%InOrder%';

-- A declared but never materialized projection has no parts, so the chooser drops it and the read
-- stays on the base table: dynamic filtering must still apply (expected 1) and the read must not
-- be in order (expected 0).
DROP TABLE IF EXISTS t_topk_unmat;
CREATE TABLE t_topk_unmat (id UInt64, k UInt64, score UInt64, payload String CODEC(NONE))
ENGINE = MergeTree ORDER BY (k, id)
SETTINGS index_granularity = 256, min_bytes_for_wide_part = 0;
INSERT INTO t_topk_unmat SELECT number, number % 128, sipHash64(number), toString(number) FROM numbers(32768);
OPTIMIZE TABLE t_topk_unmat FINAL;
ALTER TABLE t_topk_unmat ADD PROJECTION p_score (SELECT id, k, score, payload ORDER BY (score, id));

SELECT count() > 0 AS has_topk_filter
FROM (
    EXPLAIN projections = 1, actions = 1
    SELECT id, cityHash64(payload) FROM t_topk_unmat ORDER BY score, id LIMIT 10
    SETTINGS optimize_read_in_order = 1, optimize_use_projections = 1, use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 100
)
WHERE explain ILIKE '%__topKFilter%';

SELECT count() > 0 AS unmaterialized_in_order
FROM (
    EXPLAIN projections = 1
    SELECT id, cityHash64(payload) FROM t_topk_unmat ORDER BY score, id LIMIT 10
    SETTINGS optimize_read_in_order = 1, optimize_use_projections = 1, use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 100
)
WHERE explain ILIKE '%InOrder%';

-- With the projection materialized for only some parts, the chooser reads the rest from the base
-- table under a union, and that branch is not in order: dynamic filtering must still apply
-- (expected 1). The projection assertion (expected 1) is what keeps the first one honest: without
-- it the arm also passes when no projection is selected at all, which is a different plan.
DROP TABLE IF EXISTS t_topk_mixed;
CREATE TABLE t_topk_mixed (part UInt8, id UInt64, k UInt64, score UInt64, payload String CODEC(NONE))
ENGINE = MergeTree PARTITION BY part ORDER BY (k, id)
SETTINGS index_granularity = 256, min_bytes_for_wide_part = 0;
INSERT INTO t_topk_mixed SELECT 0, number, number % 128, sipHash64(number), toString(number) FROM numbers(16384);
INSERT INTO t_topk_mixed SELECT 1, number, number % 128, sipHash64(number + 99), toString(number) FROM numbers(16384);
OPTIMIZE TABLE t_topk_mixed FINAL;
ALTER TABLE t_topk_mixed ADD PROJECTION p_score (SELECT id, k, score, payload ORDER BY (score, id));
ALTER TABLE t_topk_mixed MATERIALIZE PROJECTION p_score IN PARTITION 0 SETTINGS mutations_sync = 2;

SELECT count() > 0 AS has_topk_filter
FROM (
    EXPLAIN projections = 1, actions = 1
    SELECT id, cityHash64(payload) FROM t_topk_mixed ORDER BY score, id LIMIT 10
    SETTINGS optimize_read_in_order = 1, optimize_use_projections = 1, use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 100
)
WHERE explain ILIKE '%__topKFilter%';

SELECT count() > 0 AS mixed_reads_base_table_branch
FROM (
    EXPLAIN projections = 1
    SELECT id, cityHash64(payload) FROM t_topk_mixed ORDER BY score, id LIMIT 10
    SETTINGS optimize_read_in_order = 1, optimize_use_projections = 1, use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 100
)
WHERE explain ILIKE '%Union%';

-- A sampled read never reaches a projection, so dynamic filtering must still apply (expected 1)
-- and the read must not be in order (expected 0).
DROP TABLE IF EXISTS t_topk_sample;
CREATE TABLE t_topk_sample (id UInt64, k UInt64, score UInt64, payload String CODEC(NONE))
ENGINE = MergeTree ORDER BY (k, id) SAMPLE BY id
SETTINGS index_granularity = 256, min_bytes_for_wide_part = 0;
INSERT INTO t_topk_sample SELECT number, number % 128, sipHash64(number), toString(number) FROM numbers(32768);
OPTIMIZE TABLE t_topk_sample FINAL;
ALTER TABLE t_topk_sample ADD PROJECTION p_score (SELECT id, k, score, payload ORDER BY (score, id));
ALTER TABLE t_topk_sample MATERIALIZE PROJECTION p_score SETTINGS mutations_sync = 2;

SELECT count() > 0 AS has_topk_filter
FROM (
    EXPLAIN projections = 1, actions = 1
    SELECT id, cityHash64(payload) FROM t_topk_sample SAMPLE 1/2 ORDER BY score, id LIMIT 10
    SETTINGS optimize_read_in_order = 1, optimize_use_projections = 1, use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 100
)
WHERE explain ILIKE '%__topKFilter%';

SELECT count() > 0 AS sampled_in_order
FROM (
    EXPLAIN projections = 1
    SELECT id, cityHash64(payload) FROM t_topk_sample SAMPLE 1/2 ORDER BY score, id LIMIT 10
    SETTINGS optimize_read_in_order = 1, optimize_use_projections = 1, use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 100
)
WHERE explain ILIKE '%InOrder%';

DROP TABLE t_topk_proj_rio;
DROP TABLE t_topk_noproj;
DROP TABLE t_topk_unmat;
DROP TABLE t_topk_mixed;
DROP TABLE t_topk_sample;
