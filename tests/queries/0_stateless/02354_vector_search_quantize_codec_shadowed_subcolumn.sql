-- Tags: no-parallel-replicas
-- (the two-stage codes rewrite is deliberately disabled under parallel replicas; the plan-shape assertion cannot hold
--  there. The query still returns exact results in that case.)
-- Regression test: ClickHouse lets a physical dotted column shadow a subcolumn (`tryGetColumn` returns the physical
-- column first). A table that declares both a `Quantized(...)` column `vec` and a physical column named `vec.quantized`
-- would otherwise make the quantized-vector-search rewrite resolve the physical column instead of the companion codes
-- subcolumn, ranking the shortlist on unrelated bytes (silently, when the widths match). The rewrite must detect the
-- shadow and fall back to exact KNN. Exercises `useVectorSearchWithQuantizedCodes`.

SET allow_experimental_codecs = 1;
SET vector_search_use_quantized_codes = 1;
SET enable_analyzer = 1;

DROP TABLE IF EXISTS quantize_shadow;
-- The physical `vec.quantized` is a FixedString(12) - the same width as the `int8` code for 8 dims (8 + 4-byte norm),
-- so a misbind would NOT throw; it would silently rank on these constant bytes.
CREATE TABLE quantize_shadow
(
    id UInt32,
    vec Array(Float32) CODEC(Quantized('int8', 8)),
    `vec.quantized` FixedString(12)
)
ENGINE = MergeTree ORDER BY id;

-- Random-ish float vectors so the L2 distances to the reference are distinct (integer ramps would tie ids 5±d and make
-- the top-k boundary order-dependent).
INSERT INTO quantize_shadow
SELECT number, arrayMap(j -> toFloat32(sipHash64(number, j) % 2000) / 1000.0 - 1.0, range(8)), toFixedString('abcdefghijkl', 12)
FROM numbers(200);

-- With the shadow present the rewrite must bail, so the plan carries no quantized shortlist.
SELECT 'no_shortlist_with_shadow',
    countIf(explain ILIKE '%quantized shortlist%') = 0
FROM
(
    EXPLAIN actions = 1
    SELECT id FROM quantize_shadow
    ORDER BY L2Distance(vec, (SELECT vec FROM quantize_shadow WHERE id = 5)) ASC
    LIMIT 5 SETTINGS vector_search_index_fetch_multiplier = 10
);

-- And results are the exact nearest neighbours, not garbage ranked on the shadow column's bytes.
WITH (SELECT vec FROM quantize_shadow WHERE id = 5) AS ref
SELECT 'exact_despite_shadow',
    (SELECT groupArray(id) FROM (SELECT id FROM quantize_shadow WHERE id != 5 ORDER BY L2Distance(vec, ref) ASC LIMIT 5 SETTINGS vector_search_index_fetch_multiplier = 10))
    = (SELECT groupArray(id) FROM (SELECT id, L2Distance(vec, ref) AS d FROM quantize_shadow WHERE id != 5 ORDER BY d, id LIMIT 5));

DROP TABLE quantize_shadow;
