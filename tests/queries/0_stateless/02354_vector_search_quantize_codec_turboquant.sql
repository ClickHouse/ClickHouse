-- End-to-end coverage for the `turboquant` method of the `Quantized(...)` vector codec: a 2-bit QJL (Quantized
-- Johnson-Lindenstrauss) cosine estimator that, like `rabitq`, drops the vector norm and is therefore cosine-only.
-- This checks the whole path - encode, the per-part codes subcolumn, and the two-stage shortlist rewrite ranked with
-- `turboquant` codes - stays correct (exact top-k under a full-coverage shortlist, a high-recall shortlist, and
-- self-match). The codec is gated behind `enable_quantized_codec`.

SET enable_quantized_codec = 1;
SET vector_search_use_quantized_codes = 1;
-- Lazy materialization is an analyzer-only plan optimization and the rewrite requires it, so under the old-analyzer CI
-- config every check below would be satisfied by a plain exact scan.
SET enable_analyzer = 1;
-- The shortlist size is k * vector_search_index_fetch_multiplier clamped to query_plan_max_limit_for_lazy_materialization,
-- and the rewrite runs at all only when lazy materialization is on. The test harness randomizes both: a smaller clamp
-- shrinks the shortlist and makes the exact/recall checks flaky, and the boolean leaves them on a plain exact scan, which
-- satisfies them while exercising nothing. Pin both.
SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 1000000;

DROP TABLE IF EXISTS quantize_turboquant;
CREATE TABLE quantize_turboquant
(
    id UInt32,
    vec Array(Float32) CODEC(Quantized('turboquant', 1024))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO quantize_turboquant (id, vec)
SELECT number, arrayMap(j -> toFloat32(sipHash64(number, j) % 2000 / 1000.0 - 1.0), range(1024))
FROM numbers(5000);

-- turboquant code size at 1024 dims: 1024/4 = 256 packed bytes (2 bits per coordinate) + a 4-byte correction factor.
SELECT 'code_length', length(vec.quantized) FROM quantize_turboquant GROUP BY length(vec.quantized);

-- In-range control: the rewrite engages for this shape, so a decline reddens here instead of leaving the checks below
-- satisfied by an exact scan. `enable_parallel_replicas = 0` inline because the runner can inject parallel replicas,
-- which disables the rewrite, and this file carries no `no-parallel-replicas` tag.
SELECT 'rewrite_engages',
    countIf(explain ILIKE '%quantized shortlist%') > 0
FROM
(
    EXPLAIN actions = 1
    SELECT id FROM quantize_turboquant
    ORDER BY cosineDistance(vec, (SELECT vec FROM quantize_turboquant WHERE id = 42)) ASC
    LIMIT 10 SETTINGS vector_search_index_fetch_multiplier = 1000, enable_parallel_replicas = 0
);

-- A shortlist covering all rows reproduces the exact brute-force top-k.
WITH (SELECT vec FROM quantize_turboquant WHERE id = 42) AS ref
SELECT 'unfiltered_exact',
    (SELECT groupArray(id) FROM (SELECT id, cosineDistance(vec, ref) AS d FROM quantize_turboquant ORDER BY d, id LIMIT 10))
    = (SELECT groupArray(id) FROM (SELECT id FROM quantize_turboquant ORDER BY cosineDistance(vec, ref) ASC LIMIT 10 SETTINGS vector_search_index_fetch_multiplier = 1000));

-- A moderate shortlist still recovers most true neighbours (a broken estimator would collapse recall).
WITH (SELECT vec FROM quantize_turboquant WHERE id = 42) AS ref,
     (SELECT groupArray(id) FROM (SELECT id FROM quantize_turboquant ORDER BY cosineDistance(vec, ref), id LIMIT 10)) AS truth
SELECT 'recall_at_10_ge_8',
    length(arrayIntersect(truth,
        (SELECT groupArray(id) FROM (SELECT id FROM quantize_turboquant ORDER BY cosineDistance(vec, ref) ASC LIMIT 10 SETTINGS vector_search_index_fetch_multiplier = 20)))) >= 8;

-- The exact-match query vector ranks first.
WITH (SELECT vec FROM quantize_turboquant WHERE id = 42) AS ref
SELECT 'nearest_is_self', (SELECT id FROM quantize_turboquant ORDER BY cosineDistance(vec, ref) ASC LIMIT 1 SETTINGS vector_search_index_fetch_multiplier = 50) = 42;

DROP TABLE quantize_turboquant;
