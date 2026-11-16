-- Tags: no-parallel-replicas
-- (the two-stage codes rewrite is deliberately disabled under parallel replicas, so the plan-shape assertion below
--  cannot hold there; the query still returns exact results in that case.)
-- The `int8` method of the `Quantized(...)` column codec stores one Lloyd-Max Int8 code per coordinate (of the rotated,
-- unit-variance vector) plus the per-vector L2 norm, exposed as the readable subcolumn `<column>.quantized`. The
-- full-precision data is stored verbatim, so reading the vector itself is unaffected. The codec is gated behind
-- `allow_experimental_codecs`.

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

DROP TABLE IF EXISTS quantize_int8;
CREATE TABLE quantize_int8
(
    id UInt32,
    vec Array(Float32) CODEC(Quantized('int8', 64))
)
ENGINE = MergeTree ORDER BY id;

-- The codec round-trips through SHOW CREATE.
SELECT 'show_create_has_codec', position(create_table_query, 'Quantized(\'int8\', 64') > 0
FROM system.tables WHERE database = currentDatabase() AND name = 'quantize_int8';

INSERT INTO quantize_int8 (id, vec)
SELECT number, arrayMap(j -> toFloat32(sipHash64(number, j) % 2000 / 1000.0 - 1.0), range(64))
FROM numbers(4000);

-- The quantized subcolumn is readable; its code size is dimensions + 4 (one Int8 per coordinate, plus the norm).
SELECT 'subcolumn_type', toTypeName(vec.quantized) FROM quantize_int8 LIMIT 1;
SELECT 'code_length', length(vec.quantized) FROM quantize_int8 GROUP BY length(vec.quantized);

-- The full-precision vectors round-trip unchanged.
SELECT 'full_precision_rows', count() FROM quantize_int8 WHERE length(vec) = 64;

-- The planner rewrites ORDER BY distance LIMIT into a two-stage shortlist (over the codes) + lazy rescore.
SELECT
    'plan',
    countIf(explain ILIKE '%quantized shortlist%') > 0 AS has_shortlist,
    countIf(explain ILIKE '%LazilyReadFromMergeTree%') > 0 AS has_lazy_read
FROM
(
    EXPLAIN PLAN
    SELECT id FROM quantize_int8
    ORDER BY L2Distance(vec, (SELECT vec FROM quantize_int8 WHERE id = 123)) ASC
    LIMIT 5 SETTINGS vector_search_index_fetch_multiplier = 50
);

-- With a shortlist covering all rows, the codes path reproduces the exact brute-force top-k.
WITH (SELECT vec FROM quantize_int8 WHERE id = 123) AS ref
SELECT 'unfiltered_exact',
    (SELECT groupArray(id) FROM (SELECT id, L2Distance(vec, ref) AS d FROM quantize_int8 ORDER BY d, id LIMIT 10))
    = (SELECT groupArray(id) FROM (SELECT id FROM quantize_int8 ORDER BY L2Distance(vec, ref) ASC LIMIT 10 SETTINGS vector_search_index_fetch_multiplier = 1000));

-- Same with a post-filter: the WHERE is prefiltered before the shortlist.
WITH (SELECT vec FROM quantize_int8 WHERE id = 123) AS ref
SELECT 'filtered_exact',
    (SELECT groupArray(id) FROM (SELECT id, L2Distance(vec, ref) AS d FROM quantize_int8 WHERE id % 7 = 0 ORDER BY d, id LIMIT 8))
    = (SELECT groupArray(id) FROM (SELECT id FROM quantize_int8 WHERE id % 7 = 0 ORDER BY L2Distance(vec, ref) ASC LIMIT 8 SETTINGS vector_search_index_fetch_multiplier = 1000));

-- The exact-match query vector is returned first (its rescore distance is 0).
WITH (SELECT vec FROM quantize_int8 WHERE id = 123) AS ref
SELECT 'nearest_is_self', (SELECT id FROM quantize_int8 ORDER BY L2Distance(vec, ref) ASC LIMIT 1 SETTINGS vector_search_index_fetch_multiplier = 100) = 123;

DROP TABLE quantize_int8;
