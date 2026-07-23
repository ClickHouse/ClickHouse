-- Tags: no-parallel-replicas
-- (the two-stage codes rewrite is deliberately disabled under parallel replicas, so the plan-shape assertion below
--  cannot hold there; the query still returns exact results in that case.)
-- The `product` (trained Product Quantization) method of the `Quantized(...)` column codec learns a per-part codebook with
-- k-means and stores one m-byte code per vector, exposed as the readable subcolumn `<column>.quantized`, plus the
-- per-part codebook as the subcolumn `<column>.product_quantization_codebook`. The full-precision data is stored verbatim, so reading the
-- vector itself (and the exact rescore) is unaffected. The codec is gated behind `allow_experimental_codecs`.
-- Syntax: `Quantized('product', dimensions, nbits, m)`.

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

DROP TABLE IF EXISTS quantize_pq;
CREATE TABLE quantize_pq
(
    id UInt32,
    vec Array(Float32) CODEC(Quantized('product', 64, 8, 8))
)
ENGINE = MergeTree ORDER BY id;

-- The codec round-trips through SHOW CREATE.
SELECT 'show_create_has_codec', position(create_table_query, 'Quantized(\'product\', 64, 8, 8') > 0
FROM system.tables WHERE database = currentDatabase() AND name = 'quantize_pq';

INSERT INTO quantize_pq (id, vec)
SELECT number, arrayMap(j -> toFloat32(sipHash64(number, j) % 2000 / 1000.0 - 1.0), range(64))
FROM numbers(4000);

-- The codes subcolumn is readable; with nbits = 8 each subspace contributes one byte, so the code is m = 8 bytes.
SELECT 'codes_type', toTypeName(vec.quantized) FROM quantize_pq LIMIT 1;
SELECT 'codes_length', length(vec.quantized) FROM quantize_pq GROUP BY length(vec.quantized);

-- The codebook subcolumn is readable; it holds 2^nbits centroids of `dimensions` floats: 256 * 64 * 4 = 65536 bytes.
SELECT 'codebook_type', toTypeName(vec.product_quantization_codebook) FROM quantize_pq LIMIT 1;
SELECT 'codebook_length', length(vec.product_quantization_codebook) FROM quantize_pq GROUP BY length(vec.product_quantization_codebook);

-- The full-precision vectors round-trip unchanged.
SELECT 'full_precision_rows', count() FROM quantize_pq WHERE length(vec) = 64;

-- The planner rewrites ORDER BY distance LIMIT into a two-stage shortlist (ranked with `productQuantizationDistance` over the codes and
-- the per-part codebook) + lazy rescore against the full-precision vector.
SELECT
    'plan',
    countIf(explain ILIKE '%quantized shortlist%') > 0 AS has_shortlist,
    countIf(explain ILIKE '%productQuantizationDistance%') > 0 AS has_pq_distance,
    countIf(explain ILIKE '%LazilyReadFromMergeTree%') > 0 AS has_lazy_read
FROM
(
    EXPLAIN actions = 1
    SELECT id FROM quantize_pq
    ORDER BY L2Distance(vec, (SELECT vec FROM quantize_pq WHERE id = 123)) ASC
    LIMIT 5 SETTINGS vector_search_index_fetch_multiplier = 50
);

-- With a shortlist covering all rows, the codes path reproduces the exact brute-force top-k (the rescore is exact L2).
WITH (SELECT vec FROM quantize_pq WHERE id = 123) AS ref
SELECT 'unfiltered_exact',
    (SELECT groupArray(id) FROM (SELECT id, L2Distance(vec, ref) AS d FROM quantize_pq ORDER BY d, id LIMIT 10))
    = (SELECT groupArray(id) FROM (SELECT id FROM quantize_pq ORDER BY L2Distance(vec, ref) ASC LIMIT 10 SETTINGS vector_search_index_fetch_multiplier = 1000));

-- Same with a post-filter: the WHERE is prefiltered before the shortlist.
WITH (SELECT vec FROM quantize_pq WHERE id = 123) AS ref
SELECT 'filtered_exact',
    (SELECT groupArray(id) FROM (SELECT id, L2Distance(vec, ref) AS d FROM quantize_pq WHERE id % 7 = 0 ORDER BY d, id LIMIT 8))
    = (SELECT groupArray(id) FROM (SELECT id FROM quantize_pq WHERE id % 7 = 0 ORDER BY L2Distance(vec, ref) ASC LIMIT 8 SETTINGS vector_search_index_fetch_multiplier = 1000));

-- The exact-match query vector is returned first (its rescore distance is 0).
WITH (SELECT vec FROM quantize_pq WHERE id = 123) AS ref
SELECT 'nearest_is_self', (SELECT id FROM quantize_pq ORDER BY L2Distance(vec, ref) ASC LIMIT 1 SETTINGS vector_search_index_fetch_multiplier = 100) = 123;

DROP TABLE quantize_pq;
