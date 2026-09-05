-- Tags: no-parallel-replicas
-- Point-read rescore for `Quantized`-codec vector search. A per-column `max_compress_block_size` equal to exactly one
-- vector's byte size (`dimensions * sizeof(element)`) stores the full-precision `Array` elements one vector per
-- compressed block, so the two-phase quantized-codes search rescores each shortlisted candidate with a single-block
-- point read instead of decompressing a whole granule. The setting is applied to the element substream only, so the
-- companion codes and (for `product`) the single-value per-part codebook keep their default block size.
--
-- There are no easily assertable I/O metrics here, so this test checks correctness: the block-aligned (point-read) path
-- must return exactly the same results as the unaligned (granule-read) path - same codes give the same shortlist, and
-- both read the same full-precision vectors, just addressed differently. Covered for a flat `int8` codec and the
-- trained `product` codec (whose codebook must not be re-blocked by the per-column setting).

SET allow_experimental_codecs = 1;
SET vector_search_use_quantized_codes = 1;
SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 1000000;

DROP TABLE IF EXISTS quantize_pr_int8_aligned;
DROP TABLE IF EXISTS quantize_pr_int8_unaligned;
DROP TABLE IF EXISTS quantize_pr_bf16_aligned;
DROP TABLE IF EXISTS quantize_pr_bf16_unaligned;
DROP TABLE IF EXISTS quantize_pr_pq_aligned;
DROP TABLE IF EXISTS quantize_pr_pq_unaligned;

-- --- Flat int8 codec: aligned (one Float32 vector = 64 * 4 = 256 bytes per block) vs unaligned (default block size). ---

CREATE TABLE quantize_pr_int8_aligned
(
    id UInt32,
    vec Array(Float32) CODEC(Quantized('int8', 64)) SETTINGS (max_compress_block_size = 256)
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 512, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

CREATE TABLE quantize_pr_int8_unaligned
(
    id UInt32,
    vec Array(Float32) CODEC(Quantized('int8', 64))
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 512, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO quantize_pr_int8_aligned
SELECT number, arrayMap(j -> toFloat32(sipHash64(number, j) % 2000 / 1000.0 - 1.0), range(64))
FROM numbers(5000);
INSERT INTO quantize_pr_int8_unaligned SELECT * FROM quantize_pr_int8_aligned;

-- Both parts must be Wide (point read only applies to Wide parts; the aligned table would otherwise silently fall back).
SELECT 'int8_wide_parts',
    (SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 'quantize_pr_int8_aligned' AND active) = 'Wide';

-- Point-read (aligned) vs granule-read (unaligned) two-stage search return the identical top-k.
WITH (SELECT vec FROM quantize_pr_int8_aligned WHERE id = 2500) AS ref
SELECT 'int8_aligned_eq_unaligned',
    (SELECT arraySort(groupArray(id)) FROM (SELECT id FROM quantize_pr_int8_aligned   ORDER BY L2Distance(vec, ref) ASC LIMIT 20 SETTINGS vector_search_index_fetch_multiplier = 20))
    = (SELECT arraySort(groupArray(id)) FROM (SELECT id FROM quantize_pr_int8_unaligned ORDER BY L2Distance(vec, ref) ASC LIMIT 20 SETTINGS vector_search_index_fetch_multiplier = 20));

-- A row's own vector rescores to itself as the nearest neighbour via the point read.
WITH (SELECT vec FROM quantize_pr_int8_aligned WHERE id = 2500) AS ref
SELECT 'int8_nearest_is_self',
    (SELECT id FROM quantize_pr_int8_aligned ORDER BY L2Distance(vec, ref) ASC LIMIT 1 SETTINGS vector_search_index_fetch_multiplier = 100) = 2500;

-- --- BFloat16 base: element size is 2 bytes, so one vector = 64 * 2 = 128 bytes per block (checks the
-- `sizeof(element)` path of the alignment). ---

CREATE TABLE quantize_pr_bf16_aligned
(
    id UInt32,
    vec Array(BFloat16) CODEC(Quantized('int8', 64)) SETTINGS (max_compress_block_size = 128)
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 512, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

CREATE TABLE quantize_pr_bf16_unaligned
(
    id UInt32,
    vec Array(BFloat16) CODEC(Quantized('int8', 64))
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 512, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO quantize_pr_bf16_aligned
SELECT number, arrayMap(j -> toFloat32(sipHash64(number, j) % 2000 / 1000.0 - 1.0), range(64))
FROM numbers(5000);
INSERT INTO quantize_pr_bf16_unaligned SELECT * FROM quantize_pr_bf16_aligned;

SELECT 'bf16_wide_parts',
    (SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 'quantize_pr_bf16_aligned' AND active) = 'Wide';

WITH (SELECT vec FROM quantize_pr_bf16_aligned WHERE id = 2500) AS ref
SELECT 'bf16_aligned_eq_unaligned',
    (SELECT arraySort(groupArray(id)) FROM (SELECT id FROM quantize_pr_bf16_aligned   ORDER BY L2Distance(vec, ref) ASC LIMIT 20 SETTINGS vector_search_index_fetch_multiplier = 20))
    = (SELECT arraySort(groupArray(id)) FROM (SELECT id FROM quantize_pr_bf16_unaligned ORDER BY L2Distance(vec, ref) ASC LIMIT 20 SETTINGS vector_search_index_fetch_multiplier = 20));

WITH (SELECT vec FROM quantize_pr_bf16_aligned WHERE id = 2500) AS ref
SELECT 'bf16_nearest_is_self',
    (SELECT id FROM quantize_pr_bf16_aligned ORDER BY L2Distance(vec, ref) ASC LIMIT 1 SETTINGS vector_search_index_fetch_multiplier = 100) = 2500;

-- --- Trained product codec: the per-column setting must NOT re-block the single-value 65536-byte codebook. ---

CREATE TABLE quantize_pr_pq_aligned
(
    id UInt32,
    vec Array(Float32) CODEC(Quantized('product', 64, 8, 8)) SETTINGS (max_compress_block_size = 256)
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 512, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

CREATE TABLE quantize_pr_pq_unaligned
(
    id UInt32,
    vec Array(Float32) CODEC(Quantized('product', 64, 8, 8))
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 512, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO quantize_pr_pq_aligned
SELECT number, arrayMap(j -> toFloat32(sipHash64(number, j) % 2000 / 1000.0 - 1.0), range(64))
FROM numbers(5000);
INSERT INTO quantize_pr_pq_unaligned SELECT * FROM quantize_pr_pq_aligned;

-- The codebook is a single per-part value: aligning the element stream to 256-byte blocks must leave it in one block,
-- so every granule still reads the full 65536-byte codebook (regression guard for the elements-only alignment).
SELECT 'pq_aligned_codebook_intact',
    countIf(length(vec.product_quantization_codebook) = 65536), count()
FROM quantize_pr_pq_aligned SETTINGS max_block_size = 512;

-- Point-read (aligned) vs granule-read (unaligned) product-codec search return the identical top-k.
WITH (SELECT vec FROM quantize_pr_pq_aligned WHERE id = 2500) AS ref
SELECT 'pq_aligned_eq_unaligned',
    (SELECT arraySort(groupArray(id)) FROM (SELECT id FROM quantize_pr_pq_aligned   ORDER BY L2Distance(vec, ref) ASC LIMIT 20 SETTINGS vector_search_index_fetch_multiplier = 20))
    = (SELECT arraySort(groupArray(id)) FROM (SELECT id FROM quantize_pr_pq_unaligned ORDER BY L2Distance(vec, ref) ASC LIMIT 20 SETTINGS vector_search_index_fetch_multiplier = 20));

WITH (SELECT vec FROM quantize_pr_pq_aligned WHERE id = 2500) AS ref
SELECT 'pq_nearest_is_self',
    (SELECT id FROM quantize_pr_pq_aligned ORDER BY L2Distance(vec, ref) ASC LIMIT 1 SETTINGS vector_search_index_fetch_multiplier = 100) = 2500;

DROP TABLE quantize_pr_int8_aligned;
DROP TABLE quantize_pr_int8_unaligned;
DROP TABLE quantize_pr_bf16_aligned;
DROP TABLE quantize_pr_bf16_unaligned;
DROP TABLE quantize_pr_pq_aligned;
DROP TABLE quantize_pr_pq_unaligned;
