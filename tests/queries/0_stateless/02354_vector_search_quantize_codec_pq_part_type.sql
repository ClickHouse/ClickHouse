-- The trained `product` method of the `Quantized(...)` codec stores a per-part codebook (one artifact for the whole column).
-- A compact part serializes each granule with a fresh state, which would train and write a separate codebook per
-- granule, so a `product` column must force its parts to be Wide even when the size/row thresholds would otherwise pick
-- Compact. The data-independent methods are stateless per row and are not forced.

SET allow_experimental_codecs = 1;

DROP TABLE IF EXISTS quantize_pq_parttype;
CREATE TABLE quantize_pq_parttype (id UInt32, vec Array(Float32) CODEC(Quantized('product', 64, 8, 8)))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;

-- These thresholds would make a 2000-row part Compact; the pq codebook forces Wide instead.
INSERT INTO quantize_pq_parttype SELECT number, arrayMap(j -> toFloat32(sipHash64(number, j) % 1000), range(64)) FROM numbers(2000);
SELECT 'pq_part_type', part_type FROM system.parts
WHERE database = currentDatabase() AND table = 'quantize_pq_parttype' AND active;
DROP TABLE quantize_pq_parttype;
