-- The `product` method of the `Quantized(...)` codec trains a codebook per part. A merge re-runs the serialization write path
-- on the merged data, so the merged part gets a freshly retrained codebook: two source parts (two codebooks) collapse
-- to one part with one codebook, and the two-stage search stays correct against it.

SET allow_experimental_codecs = 1;
SET vector_search_use_quantized_codes = 1;
SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 1000000;

DROP TABLE IF EXISTS quantize_pq_merge;
CREATE TABLE quantize_pq_merge
(
    id UInt32,
    vec Array(Float32) CODEC(Quantized('product', 64, 8, 8))
)
ENGINE = MergeTree ORDER BY id;

-- Two separate inserts land in two parts, each training its own codebook from its own data.
INSERT INTO quantize_pq_merge (id, vec)
SELECT number, arrayMap(j -> toFloat32(sipHash64(number, j) % 2000 / 1000.0 - 1.0), range(64))
FROM numbers(2000);
-- A disjoint seed space (no overlap with the first insert's seeds) keeps the second part's vectors distinct, so the two
-- parts train genuinely different codebooks and the merged top-k is tie-free.
INSERT INTO quantize_pq_merge (id, vec)
SELECT number + 1000000, arrayMap(j -> toFloat32(sipHash64(number + 5000000, j) % 2000 / 1000.0 - 1.0), range(64))
FROM numbers(2000);

SELECT 'parts_before', count() FROM system.parts WHERE database = currentDatabase() AND table = 'quantize_pq_merge' AND active;
-- One distinct codebook per part.
SELECT 'codebooks_before', uniqExact(vec.product_quantization_codebook) FROM quantize_pq_merge;

OPTIMIZE TABLE quantize_pq_merge FINAL;

SELECT 'parts_after', count() FROM system.parts WHERE database = currentDatabase() AND table = 'quantize_pq_merge' AND active;
-- The merged part carries a single retrained codebook.
SELECT 'codebooks_after', uniqExact(vec.product_quantization_codebook) FROM quantize_pq_merge;

-- The two-stage search reproduces the exact brute-force top-k against the retrained codebook (shortlist covers all rows).
WITH (SELECT vec FROM quantize_pq_merge WHERE id = 123) AS ref
SELECT 'exact_after_merge',
    (SELECT groupArray(id) FROM (SELECT id, L2Distance(vec, ref) AS d FROM quantize_pq_merge ORDER BY d, id LIMIT 10))
    = (SELECT groupArray(id) FROM (SELECT id FROM quantize_pq_merge ORDER BY L2Distance(vec, ref) ASC LIMIT 10 SETTINGS vector_search_index_fetch_multiplier = 1000));

DROP TABLE quantize_pq_merge;
