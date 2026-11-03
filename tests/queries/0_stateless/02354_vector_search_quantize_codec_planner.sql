-- Tags: no-parallel-replicas
-- (the two-stage codes rewrite is deliberately disabled under parallel replicas, so the plan-shape assertions below
--  cannot hold there; the query still returns exact results in that case.)
-- A vector column carrying a `Quantized(...)` codec makes the query planner automatically rewrite
-- ORDER BY distance LIMIT into a two-stage shortlist (over the small quantized codes subcolumn) + rescore
-- (against the full-precision vector), reading the heavy vector column lazily for the shortlisted rows only.
-- The codec is gated behind `allow_experimental_codecs`.

SET allow_experimental_codecs = 1;
SET vector_search_use_quantized_codes = 1;
-- Lazy materialization (LazilyReadFromMergeTree) is an analyzer-only plan optimization, so the plan-shape assertion
-- below needs the analyzer (the old-analyzer CI config does not produce the lazy read).
SET enable_analyzer = 1;
-- Pin the lazy-materialization settings the test harness randomizes: the shortlist size is clamped to
-- query_plan_max_limit_for_lazy_materialization (otherwise the full-coverage exact checks become approximate), and the
-- plan's lazy read of the vector column needs lazy materialization enabled.
SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 1000000;

DROP TABLE IF EXISTS quantize_auto;
CREATE TABLE quantize_auto
(
    id UInt32,
    vec Array(Float32) CODEC(Quantized('rabitq', 64))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO quantize_auto (id, vec)
SELECT number, arrayMap(j -> toFloat32(sipHash64(number, j) % 2000 / 1000.0 - 1.0), range(64))
FROM numbers(4000);

-- The plan contains the inner quantized shortlist and a lazy read of the vector column.
SELECT
    'plan',
    countIf(explain ILIKE '%quantized shortlist%') > 0 AS has_shortlist,
    countIf(explain ILIKE '%LazilyReadFromMergeTree%') > 0 AS has_lazy_read
FROM
(
    EXPLAIN PLAN
    SELECT id FROM quantize_auto
    ORDER BY cosineDistance(vec, (SELECT vec FROM quantize_auto WHERE id = 123)) ASC
    LIMIT 5 SETTINGS vector_search_index_fetch_multiplier = 50
);

-- rabitq is a cosine-only estimator (it drops the vector norm), so an L2Distance query must NOT use the codes shortlist
-- (which would rank by angle and could drop the true L2-nearest rows); the rewrite bails and the query stays exact.
SELECT 'l2_on_cosine_only_no_shortlist',
    countIf(explain ILIKE '%quantized shortlist%') = 0
FROM
(
    EXPLAIN PLAN
    SELECT id FROM quantize_auto
    ORDER BY L2Distance(vec, (SELECT vec FROM quantize_auto WHERE id = 123)) ASC
    LIMIT 5 SETTINGS vector_search_index_fetch_multiplier = 50
);

-- With a shortlist covering all rows, the codes path reproduces the exact brute-force top-k.
WITH (SELECT vec FROM quantize_auto WHERE id = 123) AS ref
SELECT 'unfiltered_exact',
    (SELECT groupArray(id) FROM (SELECT id, cosineDistance(vec, ref) AS d FROM quantize_auto ORDER BY d, id LIMIT 10))
    = (SELECT groupArray(id) FROM (SELECT id FROM quantize_auto ORDER BY cosineDistance(vec, ref) ASC LIMIT 10 SETTINGS vector_search_index_fetch_multiplier = 1000));

-- Same with a post-filter (the original motivation): the WHERE is prefiltered before the shortlist.
WITH (SELECT vec FROM quantize_auto WHERE id = 123) AS ref
SELECT 'filtered_exact',
    (SELECT groupArray(id) FROM (SELECT id, cosineDistance(vec, ref) AS d FROM quantize_auto WHERE id % 7 = 0 ORDER BY d, id LIMIT 8))
    = (SELECT groupArray(id) FROM (SELECT id FROM quantize_auto WHERE id % 7 = 0 ORDER BY cosineDistance(vec, ref) ASC LIMIT 8 SETTINGS vector_search_index_fetch_multiplier = 1000));

-- The exact-match query vector is returned first (its rescore distance is 0).
WITH (SELECT vec FROM quantize_auto WHERE id = 123) AS ref
SELECT 'nearest_is_self', (SELECT id FROM quantize_auto ORDER BY cosineDistance(vec, ref) ASC LIMIT 1 SETTINGS vector_search_index_fetch_multiplier = 100) = 123;

-- The codes path validates vector_search_index_fetch_multiplier identically to the vector-similarity-index path:
-- non-positive and oversized (> 1000) values are rejected instead of silently collapsing or inflating the shortlist.
SELECT id FROM quantize_auto ORDER BY cosineDistance(vec, (SELECT vec FROM quantize_auto WHERE id = 1)) ASC LIMIT 5 SETTINGS vector_search_index_fetch_multiplier = -1; -- { serverError INVALID_SETTING_VALUE }
SELECT id FROM quantize_auto ORDER BY cosineDistance(vec, (SELECT vec FROM quantize_auto WHERE id = 1)) ASC LIMIT 5 SETTINGS vector_search_index_fetch_multiplier = 0; -- { serverError INVALID_SETTING_VALUE }
SELECT id FROM quantize_auto ORDER BY cosineDistance(vec, (SELECT vec FROM quantize_auto WHERE id = 1)) ASC LIMIT 5 SETTINGS vector_search_index_fetch_multiplier = 2000; -- { serverError INVALID_SETTING_VALUE }

DROP TABLE quantize_auto;
