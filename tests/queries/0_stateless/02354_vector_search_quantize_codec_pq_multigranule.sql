-- Regression test: the `pq` codec stores one codebook value per part, but a scan reads it once per granule. The read
-- must broadcast that single value to every granule (a `ColumnConst`) without re-consuming the one-value stream -
-- otherwise multi-granule parts fail with "Incorrect size of nested column in constructor of ColumnConst". A small
-- `max_block_size` forces many granule reads from the single-value codebook stream.

SET allow_experimental_codecs = 1;
SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 1000000;

DROP TABLE IF EXISTS quantize_pq_mg;
CREATE TABLE quantize_pq_mg
(
    id UInt32,
    vec Array(Float32) CODEC(Quantize('pq', 64, 8, 8))
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192;

-- More than three granules, in a single part.
INSERT INTO quantize_pq_mg (id, vec)
SELECT number, arrayMap(j -> toFloat32(sipHash64(number, j) % 2000 / 1000.0 - 1.0), range(64))
FROM numbers(30000);

SELECT 'one_part', count() FROM system.parts WHERE database = currentDatabase() AND table = 'quantize_pq_mg' AND active;

-- Read the codebook subcolumn across many small blocks (each well below one granule): every row sees the same 65536-byte
-- codebook, and the read does not fail. Comparing the length avoids materializing the broadcast blob.
SELECT 'codebook_read_all_granules', countIf(length(vec.pq_codebook) = 65536), count() FROM quantize_pq_mg SETTINGS max_block_size = 1024;

-- The two-stage vector search reads codes and codebook across all granules and rescores exactly (full coverage).
WITH (SELECT vec FROM quantize_pq_mg WHERE id = 12345) AS ref
SELECT 'exact_multigranule',
    (SELECT groupArray(id) FROM (SELECT id, L2Distance(vec, ref) AS d FROM quantize_pq_mg ORDER BY d, id LIMIT 10))
    = (SELECT groupArray(id) FROM (SELECT id FROM quantize_pq_mg ORDER BY L2Distance(vec, ref) ASC LIMIT 10 SETTINGS vector_search_index_fetch_multiplier = 30000, max_block_size = 1024));

-- The nearest neighbour of a row's own vector is itself, with the shortlist read across granules.
WITH (SELECT vec FROM quantize_pq_mg WHERE id = 12345) AS ref
SELECT 'nearest_is_self', (SELECT id FROM quantize_pq_mg ORDER BY L2Distance(vec, ref) ASC LIMIT 1 SETTINGS vector_search_index_fetch_multiplier = 100, max_block_size = 1024) = 12345;

DROP TABLE quantize_pq_mg;
