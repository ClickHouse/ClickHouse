-- Tags: no-parallel-replicas
-- Exercises the rabitq bit-sliced popcount scan at a dimension large enough (1024 -> 128-byte codes) to run the
-- vectorized 64-byte main loop of the AVX-512 (VPOPCNTDQ) kernel where available, not just its byte-wise tail. The
-- AVX-512 and scalar kernels accumulate the same integer popcounts, so results are identical; this test checks the
-- end-to-end search stays correct (exact top-k under a full-coverage shortlist, a high-recall shortlist, and self-match).
-- The codec is gated behind `enable_quantized_codec`.

SET enable_quantized_codec = 1;
SET vector_search_use_quantized_codes = 1;
-- The shortlist size is k * vector_search_index_fetch_multiplier clamped to query_plan_max_limit_for_lazy_materialization;
-- the test harness randomizes the latter, which would shrink the shortlist and make the exact/recall checks flaky. Pin it.
SET query_plan_max_limit_for_lazy_materialization = 1000000;

DROP TABLE IF EXISTS quantize_rabitq_simd;
CREATE TABLE quantize_rabitq_simd
(
    id UInt32,
    vec Array(Float32) CODEC(Quantized('rabitq', 1024))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO quantize_rabitq_simd (id, vec)
SELECT number, arrayMap(j -> toFloat32(sipHash64(number, j) % 2000 / 1000.0 - 1.0), range(1024))
FROM numbers(5000);

-- rabitq code size at 1024 dims: 1024/8 sign bytes + 4-byte factor = 132 bytes (so the 64-byte SIMD loop runs twice).
SELECT 'code_length', length(vec.quantized) FROM quantize_rabitq_simd GROUP BY length(vec.quantized);

-- A shortlist covering all rows reproduces the exact brute-force top-k.
WITH (SELECT vec FROM quantize_rabitq_simd WHERE id = 42) AS ref
SELECT 'unfiltered_exact',
    (SELECT groupArray(id) FROM (SELECT id, cosineDistance(vec, ref) AS d FROM quantize_rabitq_simd ORDER BY d, id LIMIT 10))
    = (SELECT groupArray(id) FROM (SELECT id FROM quantize_rabitq_simd ORDER BY cosineDistance(vec, ref) ASC LIMIT 10 SETTINGS vector_search_index_fetch_multiplier = 1000));

-- A moderate shortlist still recovers most true neighbours (a broken popcount scan would collapse recall).
WITH (SELECT vec FROM quantize_rabitq_simd WHERE id = 42) AS ref,
     (SELECT groupArray(id) FROM (SELECT id FROM quantize_rabitq_simd ORDER BY cosineDistance(vec, ref), id LIMIT 10)) AS truth
SELECT 'recall_at_10_ge_8',
    length(arrayIntersect(truth,
        (SELECT groupArray(id) FROM (SELECT id FROM quantize_rabitq_simd ORDER BY cosineDistance(vec, ref) ASC LIMIT 10 SETTINGS vector_search_index_fetch_multiplier = 20)))) >= 8;

-- The exact-match query vector ranks first.
WITH (SELECT vec FROM quantize_rabitq_simd WHERE id = 42) AS ref
SELECT 'nearest_is_self', (SELECT id FROM quantize_rabitq_simd ORDER BY cosineDistance(vec, ref) ASC LIMIT 1 SETTINGS vector_search_index_fetch_multiplier = 50) = 42;

DROP TABLE quantize_rabitq_simd;

-- End-to-end tests for Multi-bit RaBitQ (2-8 bits).
-- Covers parameter validation, code sizes, encoding, and vector search.

SET enable_quantized_codec = 1;
SET vector_search_use_quantized_codes = 1;
SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 1000000;

DROP TABLE IF EXISTS quantize_multi_bit_rabitq;
DROP TABLE IF EXISTS quantize_multi_bit_rabitq_bad;
-- Parameter validation.
CREATE TABLE quantize_multi_bit_rabitq_bad (id UInt32, vec Array(Float32) CODEC(Quantized('rabitq', 256, 9))) ENGINE = MergeTree ORDER BY id; -- { serverError ILLEGAL_CODEC_PARAMETER }
CREATE TABLE quantize_multi_bit_rabitq_bad (id UInt32, vec Array(Float32) CODEC(Quantized('rabitq', 255, 4))) ENGINE = MergeTree ORDER BY id; -- { serverError ILLEGAL_CODEC_PARAMETER }
-- Test every supported bit width.
CREATE TABLE quantize_multi_bit_rabitq
(
    id UInt32,
    v2 Array(Float32) CODEC(Quantized('rabitq', 256, 2)),
    v3 Array(Float32) CODEC(Quantized('rabitq', 256, 3)),
    v4 Array(Float32) CODEC(Quantized('rabitq', 256, 4)),
    v5 Array(Float32) CODEC(Quantized('rabitq', 256, 5)),
    v6 Array(Float32) CODEC(Quantized('rabitq', 256, 6)),
    v7 Array(Float32) CODEC(Quantized('rabitq', 256, 7)),
    v8 Array(Float32) CODEC(Quantized('rabitq', 256, 8))
)
ENGINE = MergeTree ORDER BY id;
-- Verify codec configuration.
SELECT 'show_create',
    position(create_table_query, 'Quantized(\'rabitq\', 256, 2') > 0,
    position(create_table_query, 'Quantized(\'rabitq\', 256, 8') > 0
FROM system.tables
WHERE database = currentDatabase() AND name = 'quantize_multi_bit_rabitq';
-- Insert identical vectors for all bit widths.
INSERT INTO quantize_multi_bit_rabitq
SELECT number, v, v, v, v, v, v, v
FROM
(
    SELECT number,
        arrayMap(j -> toFloat32(sipHash64(number, j) % 2000 / 1000.0 - 1.0), range(256)) AS v
    FROM numbers(2000)
);
-- Code size = dimensions * bits / 8 + sizeof(float).
SELECT 'code_lengths',
    length(v2.quantized),
    length(v3.quantized),
    length(v4.quantized),
    length(v5.quantized),
    length(v6.quantized),
    length(v7.quantized),
    length(v8.quantized)
FROM quantize_multi_bit_rabitq
GROUP BY
    length(v2.quantized),
    length(v3.quantized),
    length(v4.quantized),
    length(v5.quantized),
    length(v6.quantized),
    length(v7.quantized),
    length(v8.quantized);
-- Full-precision vectors must remain unchanged.
SELECT 'full_precision_rows', count()
FROM quantize_multi_bit_rabitq
WHERE length(v2) = 256
    AND v2 = v3
    AND v2 = v4
    AND v2 = v5
    AND v2 = v6
    AND v2 = v7
    AND v2 = v8;
-- Verify the two-stage quantized search plan.
SELECT
    'plan',
    countIf(explain ILIKE '%quantized shortlist%') > 0,
    countIf(explain ILIKE '%LazilyReadFromMergeTree%') > 0
FROM
(
    EXPLAIN PLAN
    SELECT id FROM quantize_multi_bit_rabitq
    ORDER BY cosineDistance(v2, (SELECT v2 FROM quantize_multi_bit_rabitq WHERE id = 42)) ASC
    LIMIT 10 SETTINGS vector_search_index_fetch_multiplier = 50
);

-- Full-coverage shortlist must reproduce exact Top-K results.
WITH (SELECT v2 FROM quantize_multi_bit_rabitq WHERE id = 42) AS ref
SELECT 'exact_2bit',
    (SELECT groupArray(id) FROM (SELECT id, cosineDistance(v2, ref) AS d FROM quantize_multi_bit_rabitq ORDER BY d, id LIMIT 10))
    = (SELECT groupArray(id) FROM (SELECT id FROM quantize_multi_bit_rabitq ORDER BY cosineDistance(v2, ref) ASC LIMIT 10 SETTINGS vector_search_index_fetch_multiplier = 1000));
WITH (SELECT v3 FROM quantize_multi_bit_rabitq WHERE id = 42) AS ref
SELECT 'exact_3bit',
    (SELECT groupArray(id) FROM (SELECT id, cosineDistance(v3, ref) AS d FROM quantize_multi_bit_rabitq ORDER BY d, id LIMIT 10))
    = (SELECT groupArray(id) FROM (SELECT id FROM quantize_multi_bit_rabitq ORDER BY cosineDistance(v3, ref) ASC LIMIT 10 SETTINGS vector_search_index_fetch_multiplier = 1000));
WITH (SELECT v4 FROM quantize_multi_bit_rabitq WHERE id = 42) AS ref
SELECT 'exact_4bit',
    (SELECT groupArray(id) FROM (SELECT id, cosineDistance(v4, ref) AS d FROM quantize_multi_bit_rabitq ORDER BY d, id LIMIT 10))
    = (SELECT groupArray(id) FROM (SELECT id FROM quantize_multi_bit_rabitq ORDER BY cosineDistance(v4, ref) ASC LIMIT 10 SETTINGS vector_search_index_fetch_multiplier = 1000));
WITH (SELECT v5 FROM quantize_multi_bit_rabitq WHERE id = 42) AS ref
SELECT 'exact_5bit',
    (SELECT groupArray(id) FROM (SELECT id, cosineDistance(v5, ref) AS d FROM quantize_multi_bit_rabitq ORDER BY d, id LIMIT 10))
    = (SELECT groupArray(id) FROM (SELECT id FROM quantize_multi_bit_rabitq ORDER BY cosineDistance(v5, ref) ASC LIMIT 10 SETTINGS vector_search_index_fetch_multiplier = 1000));
WITH (SELECT v6 FROM quantize_multi_bit_rabitq WHERE id = 42) AS ref
SELECT 'exact_6bit',
    (SELECT groupArray(id) FROM (SELECT id, cosineDistance(v6, ref) AS d FROM quantize_multi_bit_rabitq ORDER BY d, id LIMIT 10))
    = (SELECT groupArray(id) FROM (SELECT id FROM quantize_multi_bit_rabitq ORDER BY cosineDistance(v6, ref) ASC LIMIT 10 SETTINGS vector_search_index_fetch_multiplier = 1000));
WITH (SELECT v7 FROM quantize_multi_bit_rabitq WHERE id = 42) AS ref
SELECT 'exact_7bit',
    (SELECT groupArray(id) FROM (SELECT id, cosineDistance(v7, ref) AS d FROM quantize_multi_bit_rabitq ORDER BY d, id LIMIT 10))
    = (SELECT groupArray(id) FROM (SELECT id FROM quantize_multi_bit_rabitq ORDER BY cosineDistance(v7, ref) ASC LIMIT 10 SETTINGS vector_search_index_fetch_multiplier = 1000));
WITH (SELECT v8 FROM quantize_multi_bit_rabitq WHERE id = 42) AS ref
SELECT 'exact_8bit',
    (SELECT groupArray(id) FROM (SELECT id, cosineDistance(v8, ref) AS d FROM quantize_multi_bit_rabitq ORDER BY d, id LIMIT 10))
    = (SELECT groupArray(id) FROM (SELECT id FROM quantize_multi_bit_rabitq ORDER BY cosineDistance(v8, ref) ASC LIMIT 10 SETTINGS vector_search_index_fetch_multiplier = 1000));
-- Verify Recall with a limited shortlist (10 * 20 = 200 candidates out of 2000 rows).
-- The estimator must recover at least 8 of the exact Top-10 neighbors.
WITH (SELECT v2 FROM quantize_multi_bit_rabitq WHERE id = 42) AS ref,
     (SELECT groupArray(id) FROM
         (SELECT id FROM quantize_multi_bit_rabitq ORDER BY cosineDistance(v2, ref), id LIMIT 10)) AS truth
SELECT 'recall_2bit_ge_8',
    length(arrayIntersect(truth,
        (SELECT groupArray(id) FROM
            (SELECT id FROM quantize_multi_bit_rabitq ORDER BY cosineDistance(v2, ref) ASC
             LIMIT 10 SETTINGS vector_search_index_fetch_multiplier = 20)))) >= 8;
WITH (SELECT v3 FROM quantize_multi_bit_rabitq WHERE id = 42) AS ref,
     (SELECT groupArray(id) FROM
         (SELECT id FROM quantize_multi_bit_rabitq ORDER BY cosineDistance(v3, ref), id LIMIT 10)) AS truth
SELECT 'recall_3bit_ge_8',
    length(arrayIntersect(truth,
        (SELECT groupArray(id) FROM
            (SELECT id FROM quantize_multi_bit_rabitq ORDER BY cosineDistance(v3, ref) ASC
             LIMIT 10 SETTINGS vector_search_index_fetch_multiplier = 20)))) >= 8;
WITH (SELECT v4 FROM quantize_multi_bit_rabitq WHERE id = 42) AS ref,
     (SELECT groupArray(id) FROM
         (SELECT id FROM quantize_multi_bit_rabitq ORDER BY cosineDistance(v4, ref), id LIMIT 10)) AS truth
SELECT 'recall_4bit_ge_8',
    length(arrayIntersect(truth,
        (SELECT groupArray(id) FROM
            (SELECT id FROM quantize_multi_bit_rabitq ORDER BY cosineDistance(v4, ref) ASC
             LIMIT 10 SETTINGS vector_search_index_fetch_multiplier = 20)))) >= 8;
WITH (SELECT v5 FROM quantize_multi_bit_rabitq WHERE id = 42) AS ref,
     (SELECT groupArray(id) FROM
         (SELECT id FROM quantize_multi_bit_rabitq ORDER BY cosineDistance(v5, ref), id LIMIT 10)) AS truth
SELECT 'recall_5bit_ge_8',
    length(arrayIntersect(truth,
        (SELECT groupArray(id) FROM
            (SELECT id FROM quantize_multi_bit_rabitq ORDER BY cosineDistance(v5, ref) ASC
             LIMIT 10 SETTINGS vector_search_index_fetch_multiplier = 20)))) >= 8;
WITH (SELECT v6 FROM quantize_multi_bit_rabitq WHERE id = 42) AS ref,
     (SELECT groupArray(id) FROM
         (SELECT id FROM quantize_multi_bit_rabitq ORDER BY cosineDistance(v6, ref), id LIMIT 10)) AS truth
SELECT 'recall_6bit_ge_8',
    length(arrayIntersect(truth,
        (SELECT groupArray(id) FROM
            (SELECT id FROM quantize_multi_bit_rabitq ORDER BY cosineDistance(v6, ref) ASC
             LIMIT 10 SETTINGS vector_search_index_fetch_multiplier = 20)))) >= 8;
WITH (SELECT v7 FROM quantize_multi_bit_rabitq WHERE id = 42) AS ref,
     (SELECT groupArray(id) FROM
         (SELECT id FROM quantize_multi_bit_rabitq ORDER BY cosineDistance(v7, ref), id LIMIT 10)) AS truth
SELECT 'recall_7bit_ge_8',
    length(arrayIntersect(truth,
        (SELECT groupArray(id) FROM
            (SELECT id FROM quantize_multi_bit_rabitq ORDER BY cosineDistance(v7, ref) ASC
             LIMIT 10 SETTINGS vector_search_index_fetch_multiplier = 20)))) >= 8;
WITH (SELECT v8 FROM quantize_multi_bit_rabitq WHERE id = 42) AS ref,
     (SELECT groupArray(id) FROM
         (SELECT id FROM quantize_multi_bit_rabitq ORDER BY cosineDistance(v8, ref), id LIMIT 10)) AS truth
SELECT 'recall_8bit_ge_8',
    length(arrayIntersect(truth,
        (SELECT groupArray(id) FROM
            (SELECT id FROM quantize_multi_bit_rabitq ORDER BY cosineDistance(v8, ref) ASC
             LIMIT 10 SETTINGS vector_search_index_fetch_multiplier = 20)))) >= 8;
-- Self-match must be the nearest neighbour.

WITH (SELECT v2 FROM quantize_multi_bit_rabitq WHERE id = 42) AS ref
SELECT 'self_2bit', (SELECT id FROM quantize_multi_bit_rabitq ORDER BY cosineDistance(v2, ref) ASC LIMIT 1 SETTINGS vector_search_index_fetch_multiplier = 100) = 42;
WITH (SELECT v3 FROM quantize_multi_bit_rabitq WHERE id = 42) AS ref
SELECT 'self_3bit', (SELECT id FROM quantize_multi_bit_rabitq ORDER BY cosineDistance(v3, ref) ASC LIMIT 1 SETTINGS vector_search_index_fetch_multiplier = 100) = 42;

WITH (SELECT v4 FROM quantize_multi_bit_rabitq WHERE id = 42) AS ref
SELECT 'self_4bit', (SELECT id FROM quantize_multi_bit_rabitq ORDER BY cosineDistance(v4, ref) ASC LIMIT 1 SETTINGS vector_search_index_fetch_multiplier = 100) = 42;
WITH (SELECT v5 FROM quantize_multi_bit_rabitq WHERE id = 42) AS ref
SELECT 'self_5bit', (SELECT id FROM quantize_multi_bit_rabitq ORDER BY cosineDistance(v5, ref) ASC LIMIT 1 SETTINGS vector_search_index_fetch_multiplier = 100) = 42;

WITH (SELECT v6 FROM quantize_multi_bit_rabitq WHERE id = 42) AS ref
SELECT 'self_6bit', (SELECT id FROM quantize_multi_bit_rabitq ORDER BY cosineDistance(v6, ref) ASC LIMIT 1 SETTINGS vector_search_index_fetch_multiplier = 100) = 42;
WITH (SELECT v7 FROM quantize_multi_bit_rabitq WHERE id = 42) AS ref
SELECT 'self_7bit', (SELECT id FROM quantize_multi_bit_rabitq ORDER BY cosineDistance(v7, ref) ASC LIMIT 1 SETTINGS vector_search_index_fetch_multiplier = 100) = 42;

WITH (SELECT v8 FROM quantize_multi_bit_rabitq WHERE id = 42) AS ref
SELECT 'self_8bit', (SELECT id FROM quantize_multi_bit_rabitq ORDER BY cosineDistance(v8, ref) ASC LIMIT 1 SETTINGS vector_search_index_fetch_multiplier = 100) = 42;

DROP TABLE quantize_multi_bit_rabitq;
