-- A vector column carrying a `Quantize(...)` codec makes the query planner automatically rewrite
-- ORDER BY distance LIMIT into a two-stage shortlist (over the small quantized codes subcolumn) + rescore
-- (against the full-precision vector), reading the heavy vector column lazily for the shortlisted rows only.

DROP TABLE IF EXISTS quantize_auto;
CREATE TABLE quantize_auto
(
    id UInt32,
    vec Array(Float32) CODEC(Quantize('rabitq', 64))
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
    ORDER BY L2Distance(vec, (SELECT vec FROM quantize_auto WHERE id = 123)) ASC
    LIMIT 5 SETTINGS vector_search_index_fetch_multiplier = 50
);

-- With a shortlist covering all rows, the codes path reproduces the exact brute-force top-k.
WITH (SELECT vec FROM quantize_auto WHERE id = 123) AS ref
SELECT 'unfiltered_exact',
    (SELECT groupArray(id) FROM (SELECT id, L2Distance(vec, ref) AS d FROM quantize_auto ORDER BY d, id LIMIT 10))
    = (SELECT groupArray(id) FROM (SELECT id FROM quantize_auto ORDER BY L2Distance(vec, ref) ASC LIMIT 10 SETTINGS vector_search_index_fetch_multiplier = 4000));

-- Same with a post-filter (the original motivation): the WHERE is prefiltered before the shortlist.
WITH (SELECT vec FROM quantize_auto WHERE id = 123) AS ref
SELECT 'filtered_exact',
    (SELECT groupArray(id) FROM (SELECT id, L2Distance(vec, ref) AS d FROM quantize_auto WHERE id % 7 = 0 ORDER BY d, id LIMIT 8))
    = (SELECT groupArray(id) FROM (SELECT id FROM quantize_auto WHERE id % 7 = 0 ORDER BY L2Distance(vec, ref) ASC LIMIT 8 SETTINGS vector_search_index_fetch_multiplier = 4000));

-- The exact-match query vector is returned first (its rescore distance is 0).
WITH (SELECT vec FROM quantize_auto WHERE id = 123) AS ref
SELECT 'nearest_is_self', (SELECT id FROM quantize_auto ORDER BY L2Distance(vec, ref) ASC LIMIT 1 SETTINGS vector_search_index_fetch_multiplier = 100) = 123;

DROP TABLE quantize_auto;
