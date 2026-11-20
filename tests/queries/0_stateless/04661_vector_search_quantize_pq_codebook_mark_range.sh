#!/usr/bin/env bash
# Tags: no-distributed-cache

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The codebook of the `product` quantization method is a single value for the whole part, written after the data of
# all granules, so every granule's mark points at its start and no mark delimits its end. With an adaptive write
# buffer the 65536-byte codebook spans several compressed blocks and the part's final mark points into the middle of
# it, so a read whose mark range ends before the final mark used to be bounded by that mark and read the codebook
# short: `Cannot read all data of type FixedString. Bytes read:49152. String size:65536`.
#
# The bound is only enforced by read buffers that implement `setReadUntilPosition` (object storage, encrypted or
# cached disks) - a plain local disk ignores it - hence the `local_blob_storage` disk. The compression settings are
# pinned because the codebook must span more than one compressed block, and the granularity settings because the
# read must stop before the part's final mark.

$CLICKHOUSE_CLIENT --enable_quantized_codec 1 -m -q "
DROP TABLE IF EXISTS quantize_pq_codebook_mark_range;

CREATE TABLE quantize_pq_codebook_mark_range
(
    id UInt32,
    vec Array(Float32) CODEC(Quantized('product', 64, 8, 8))
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1024, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0,
    min_compress_block_size = 65536, max_compress_block_size = 1048576,
    min_columns_to_activate_adaptive_write_buffer = 1, adaptive_write_buffer_initial_size = 16384,
    disk = disk(type = 'local_blob_storage', path = '${CLICKHOUSE_TEST_UNIQUE_NAME}/');

-- ~10 granules in a single wide part.
INSERT INTO quantize_pq_codebook_mark_range
SELECT number, arrayMap(j -> toFloat32(sipHash64(number, j) % 2000 / 1000.0 - 1.0), range(64))
FROM numbers(10000);

SELECT 'part_type', part_type FROM system.parts
WHERE database = currentDatabase() AND table = 'quantize_pq_codebook_mark_range' AND active;

-- A read of the first three granules only: its mark range ends long before the part's final mark.
SELECT 'partial_range', countIf(length(vec.product_quantization_codebook) = 65536), count()
FROM quantize_pq_codebook_mark_range WHERE id < 3000 SETTINGS max_block_size = 1024;

SELECT 'full_scan', countIf(length(vec.product_quantization_codebook) = 65536), count()
FROM quantize_pq_codebook_mark_range SETTINGS max_block_size = 1024;

-- Every granule must see the very same codebook, whatever the mark range of the read is. Hashing it keeps the
-- broadcast blob from being materialized: the subcolumn is a \`ColumnConst\`, so the hash is computed once per block.
SELECT 'same_codebook',
    (SELECT uniqExact(cityHash64(vec.product_quantization_codebook)) FROM quantize_pq_codebook_mark_range WHERE id < 3000),
    (SELECT uniqExact(cityHash64(vec.product_quantization_codebook)) FROM quantize_pq_codebook_mark_range),
    (SELECT any(cityHash64(vec.product_quantization_codebook)) FROM quantize_pq_codebook_mark_range WHERE id < 3000)
        = (SELECT any(cityHash64(vec.product_quantization_codebook)) FROM quantize_pq_codebook_mark_range);

DROP TABLE quantize_pq_codebook_mark_range SYNC;
"
