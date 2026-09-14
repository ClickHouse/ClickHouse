-- Tags: no-parallel-replicas
-- (the two-stage codes rewrite is deliberately disabled under parallel replicas, so the plan-shape assertions below
--  cannot hold there.)
-- Edge cases of the two-stage `Quantized(...)` planner rewrite: it must not corrupt or fail queries that select the
-- companion subcolumn, or that already contain a column named like the rewrite's internal sort key. In those cases the
-- rewrite either preserves the needed column or bails out to the exact path (the exact full-precision result is always
-- correct, just unaccelerated).

SET allow_experimental_codecs = 1;
SET vector_search_use_quantized_codes = 1;
SET enable_analyzer = 1;
SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 1000000;

DROP TABLE IF EXISTS quantize_edge;
CREATE TABLE quantize_edge
(
    id UInt32,
    vec Array(Float32) CODEC(Quantized('rabitq', 64))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO quantize_edge (id, vec)
SELECT number, arrayMap(j -> toFloat32(sipHash64(number, j) % 2000 / 1000.0 - 1.0), range(64))
FROM numbers(2000);

-- Selecting the companion subcolumn alongside the distance ORDER BY must still work: the rewrite keeps `vec.quantized`
-- in the shortlist output (it is consumed by the rescore expression) instead of dropping it, so the query returns the
-- subcolumn for the top-k rather than failing with a missing column. rabitq codes are 64/8 + 4 = 12 bytes.
WITH (SELECT vec FROM quantize_edge WHERE id = 123) AS ref
SELECT 'selects_subcolumn', count(), countDistinct(length(q)) FROM
(
    SELECT id, vec.quantized AS q FROM quantize_edge
    ORDER BY cosineDistance(vec, ref) ASC LIMIT 5 SETTINGS vector_search_index_fetch_multiplier = 50
);

-- A column named like the internal sort key forces the rewrite to bail to the exact path (no shortlist), and the query
-- still returns the correct exact top-k.
DROP TABLE IF EXISTS quantize_collide;
CREATE TABLE quantize_collide
(
    id UInt32,
    vec Array(Float32) CODEC(Quantized('rabitq', 64)),
    `__quantize_approx_distance` Float32
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO quantize_collide (id, vec, `__quantize_approx_distance`)
SELECT number, arrayMap(j -> toFloat32(sipHash64(number, j) % 2000 / 1000.0 - 1.0), range(64)), 0
FROM numbers(2000);

SELECT 'collision_bails',
    countIf(explain ILIKE '%quantized shortlist%') = 0 AS no_shortlist
FROM
(
    EXPLAIN PLAN
    SELECT id, `__quantize_approx_distance` FROM quantize_collide
    ORDER BY cosineDistance(vec, (SELECT vec FROM quantize_collide WHERE id = 123)) ASC LIMIT 5 SETTINGS vector_search_index_fetch_multiplier = 50
);

-- Even though it bails, the result is the exact brute-force top-k.
WITH (SELECT vec FROM quantize_collide WHERE id = 123) AS ref
SELECT 'collision_exact',
    (SELECT groupArray(id) FROM (SELECT id FROM quantize_collide ORDER BY cosineDistance(vec, ref) ASC, id LIMIT 10 SETTINGS vector_search_index_fetch_multiplier = 50))
    = (SELECT groupArray(id) FROM (SELECT id, cosineDistance(vec, ref) AS d FROM quantize_collide ORDER BY d, id LIMIT 10));

DROP TABLE quantize_collide;
DROP TABLE quantize_edge;
