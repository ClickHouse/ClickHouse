-- Tags: no-parallel-replicas
-- (the two-stage codes rewrite is deliberately disabled under parallel replicas, so the plan-shape assertion below
--  cannot hold there; the query still returns exact results in that case.)
-- The `prefix` (Matryoshka) method of the `Quantized(...)` codec keeps only the leading `leading_dimensions` of the vector
-- as the readable subcolumn `<column>.quantized`, stored either as int8 (with a per-vector scale, +4 bytes) or as
-- bfloat16. For MRL-trained embeddings the prefix carries most of the signal, so it is a cheap shortlist. The
-- full-precision data is stored verbatim, so the exact rescore is unaffected. Syntax:
-- `Quantized('prefix', dimensions, leading_dimensions, 'int8'|'bf16')`.

SET allow_experimental_codecs = 1;
SET vector_search_use_quantized_codes = 1;
-- Lazy materialization (LazilyReadFromMergeTree) is an analyzer-only plan optimization, so the plan-shape assertion
-- below needs the analyzer (the old-analyzer CI config does not produce the lazy read).
SET enable_analyzer = 1;
SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 1000000;

DROP TABLE IF EXISTS quantize_mrl;
CREATE TABLE quantize_mrl
(
    id UInt32,
    vfull Array(Float32) CODEC(Quantized('prefix', 64, 64, 'bf16')), -- whole vector as bfloat16
    vtrunc Array(Float32) CODEC(Quantized('prefix', 64, 16, 'int8'))  -- leading 16 dims as int8
)
ENGINE = MergeTree ORDER BY id;

-- The methods round-trip through SHOW CREATE in their canonical form.
SELECT 'show_create',
    position(create_table_query, 'Quantized(\'prefix_bf16\', 64, 64') > 0,
    position(create_table_query, 'Quantized(\'prefix_int8\', 64, 16') > 0
FROM system.tables WHERE database = currentDatabase() AND name = 'quantize_mrl';

INSERT INTO quantize_mrl (id, vfull, vtrunc)
SELECT number,
       arrayMap(j -> toFloat32(sipHash64(number, j) % 2000 / 1000.0 - 1.0), range(64)),
       arrayMap(j -> toFloat32(sipHash64(number, j) % 2000 / 1000.0 - 1.0), range(64))
FROM numbers(4000);

-- Code sizes: bf16 full = 64 * 2 = 128 bytes; int8 prefix = 16 + 4 (scale) = 20 bytes.
SELECT 'codes_bf16', length(vfull.quantized) FROM quantize_mrl GROUP BY length(vfull.quantized);
SELECT 'codes_int8', length(vtrunc.quantized) FROM quantize_mrl GROUP BY length(vtrunc.quantized);

-- The full-precision vectors round-trip unchanged.
SELECT 'full_precision_rows', count() FROM quantize_mrl WHERE length(vfull) = 64;

-- The planner rewrites ORDER BY distance LIMIT into the two-stage shortlist + lazy rescore for an mrl column.
SELECT
    'plan',
    countIf(explain ILIKE '%quantized shortlist%') > 0 AS has_shortlist,
    countIf(explain ILIKE '%LazilyReadFromMergeTree%') > 0 AS has_lazy_read
FROM
(
    EXPLAIN PLAN
    SELECT id FROM quantize_mrl
    ORDER BY L2Distance(vfull, (SELECT vfull FROM quantize_mrl WHERE id = 123)) ASC
    LIMIT 5 SETTINGS vector_search_index_fetch_multiplier = 50
);

-- With a shortlist covering all rows, the codes path reproduces the exact brute-force top-k (the rescore is exact).
WITH (SELECT vfull FROM quantize_mrl WHERE id = 123) AS ref
SELECT 'bf16_unfiltered_exact',
    (SELECT groupArray(id) FROM (SELECT id, L2Distance(vfull, ref) AS d FROM quantize_mrl ORDER BY d, id LIMIT 10))
    = (SELECT groupArray(id) FROM (SELECT id FROM quantize_mrl ORDER BY L2Distance(vfull, ref) ASC LIMIT 10 SETTINGS vector_search_index_fetch_multiplier = 1000));

WITH (SELECT vtrunc FROM quantize_mrl WHERE id = 123) AS ref
SELECT 'int8_unfiltered_exact',
    (SELECT groupArray(id) FROM (SELECT id, L2Distance(vtrunc, ref) AS d FROM quantize_mrl ORDER BY d, id LIMIT 10))
    = (SELECT groupArray(id) FROM (SELECT id FROM quantize_mrl ORDER BY L2Distance(vtrunc, ref) ASC LIMIT 10 SETTINGS vector_search_index_fetch_multiplier = 1000));

-- The full-vector bf16 shortlist is near-lossless, so the exact-match query vector is returned first.
WITH (SELECT vfull FROM quantize_mrl WHERE id = 123) AS ref
SELECT 'bf16_nearest_is_self', (SELECT id FROM quantize_mrl ORDER BY L2Distance(vfull, ref) ASC LIMIT 1 SETTINGS vector_search_index_fetch_multiplier = 100) = 123;

-- Non-finite prefix values (NaN, +/-Inf) must not trip std::lround(NaN) in the int8 encoder: they are encoded
-- deterministically (the scale ignores them and they become 0), and since the full-precision vector is stored verbatim
-- it round-trips unchanged. The insert must succeed and the codes keep their fixed length.
INSERT INTO quantize_mrl (id, vfull, vtrunc)
SELECT 9999,
    arrayMap(j -> if(j = 0, toFloat32(nan), if(j = 1, toFloat32(inf), if(j = 2, toFloat32(-inf), toFloat32(j)))), range(64)),
    arrayMap(j -> if(j = 0, toFloat32(nan), if(j = 1, toFloat32(inf), if(j = 2, toFloat32(-inf), toFloat32(j)))), range(64));
SELECT 'nonfinite', isNaN(vtrunc[1]), isInfinite(vtrunc[2]), length(vtrunc.quantized) FROM quantize_mrl WHERE id = 9999;

DROP TABLE quantize_mrl;
