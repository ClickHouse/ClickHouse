-- The structured random projection behind the `Quantized(...)` codec uses an exact Kronecker Hadamard transform
-- H_{2^k} (x) H_m for dimensions of the form 2^k * m (m in {12, 20}), instead of zero-padding to the next power of two.
-- This exercises that path with dimension 96 = 8 * 12 (which would otherwise pad to 128): encode and query must use the
-- same rotation, so the codes path still reproduces the exact brute-force top-k and ranks the query vector first.
-- The codec is gated behind `allow_experimental_codecs`.

SET allow_experimental_codecs = 1;
SET vector_search_use_quantized_codes = 1;
-- The shortlist size is k * vector_search_index_fetch_multiplier clamped to query_plan_max_limit_for_lazy_materialization;
-- the test harness randomizes the latter, which would shrink the full-coverage shortlist and make the exact check flaky. Pin it.
SET query_plan_max_limit_for_lazy_materialization = 1000000;

DROP TABLE IF EXISTS quantize_kron;
CREATE TABLE quantize_kron
(
    id UInt32,
    vec Array(Float32) CODEC(Quantized('rabitq', 96))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO quantize_kron (id, vec)
SELECT number, arrayMap(j -> toFloat32(sipHash64(number, j) % 2000 / 1000.0 - 1.0), range(96))
FROM numbers(2000);

-- The quantized subcolumn has the expected code size for rabitq at 96 dims (96/8 sign bytes + 4-byte factor).
SELECT 'code_length', length(vec.quantized) FROM quantize_kron GROUP BY length(vec.quantized);

-- With a shortlist covering all rows, the codes path reproduces the exact brute-force top-k.
WITH (SELECT vec FROM quantize_kron WHERE id = 123) AS ref
SELECT 'unfiltered_exact',
    (SELECT groupArray(id) FROM (SELECT id, cosineDistance(vec, ref) AS d FROM quantize_kron ORDER BY d, id LIMIT 10))
    = (SELECT groupArray(id) FROM (SELECT id FROM quantize_kron ORDER BY cosineDistance(vec, ref) ASC LIMIT 10 SETTINGS vector_search_index_fetch_multiplier = 1000));

-- The exact-match query vector is returned first (its rescore distance is 0), confirming encode/query use the same rotation.
WITH (SELECT vec FROM quantize_kron WHERE id = 123) AS ref
SELECT 'nearest_is_self', (SELECT id FROM quantize_kron ORDER BY cosineDistance(vec, ref) ASC LIMIT 1 SETTINGS vector_search_index_fetch_multiplier = 100) = 123;

DROP TABLE quantize_kron;
