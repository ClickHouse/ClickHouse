-- Tags: no-random-merge-tree-settings
-- no-random-merge-tree-settings: random settings would change block sizes and break the alignment between MergeTree's writer and the aggregate's chunking.

-- Check that `estimateCompressionRatio('T64', N)` (which uses tryGetCompressedSize internally) agrees with the on-disk size.

SELECT 'T64 default';

DROP TABLE IF EXISTS t64_default;

CREATE TABLE t64_default (x UInt32 CODEC(T64))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_compress_block_size = 0, max_compress_block_size = 65536;

INSERT INTO t64_default SELECT number FROM numbers(100000);

SELECT
    column_data_compressed_bytes AS on_disk_bytes,
    toUInt64(round(column_data_uncompressed_bytes /
                   (SELECT estimateCompressionRatio('T64', 65536)(x) FROM t64_default))) AS aggregate_predicted_bytes,
    on_disk_bytes = aggregate_predicted_bytes AS matches
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't64_default' AND active AND column = 'x';

DROP TABLE t64_default;


SELECT 'T64 byte';

DROP TABLE IF EXISTS t64_byte;

CREATE TABLE t64_byte (x UInt32 CODEC(T64('byte')))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_compress_block_size = 0, max_compress_block_size = 65536;

INSERT INTO t64_byte SELECT number FROM numbers(100000);

SELECT
    column_data_compressed_bytes AS on_disk_bytes,
    toUInt64(round(column_data_uncompressed_bytes /
                   (SELECT estimateCompressionRatio('T64(\'byte\')', 65536)(x) FROM t64_byte))) AS aggregate_predicted_bytes,
    on_disk_bytes = aggregate_predicted_bytes AS matches
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't64_byte' AND active AND column = 'x';

DROP TABLE t64_byte;


SELECT 'T64 bit';

DROP TABLE IF EXISTS t64_bit;

CREATE TABLE t64_bit (x UInt32 CODEC(T64('bit')))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_compress_block_size = 0, max_compress_block_size = 65536;

INSERT INTO t64_bit SELECT number FROM numbers(100000);

SELECT
    column_data_compressed_bytes AS on_disk_bytes,
    toUInt64(round(column_data_uncompressed_bytes /
                   (SELECT estimateCompressionRatio('T64(\'bit\')', 65536)(x) FROM t64_bit))) AS aggregate_predicted_bytes,
    on_disk_bytes = aggregate_predicted_bytes AS matches
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't64_bit' AND active AND column = 'x';

DROP TABLE t64_bit;


SELECT 'T64 Int64 cross-zero';

DROP TABLE IF EXISTS t64_int64_cross_zero;

CREATE TABLE t64_int64_cross_zero (x Int64 CODEC(T64))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_compress_block_size = 0, max_compress_block_size = 65536;

-- Values span both sides of zero, exercising the signed cross-zero branch of `getValuableBitsNumber`.
INSERT INTO t64_int64_cross_zero SELECT toInt64(number) - 50000 FROM numbers(100000);

SELECT
    column_data_compressed_bytes AS on_disk_bytes,
    toUInt64(round(column_data_uncompressed_bytes /
                   (SELECT estimateCompressionRatio('T64', 65536)(x) FROM t64_int64_cross_zero))) AS aggregate_predicted_bytes,
    on_disk_bytes = aggregate_predicted_bytes AS matches
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't64_int64_cross_zero' AND active AND column = 'x';

DROP TABLE t64_int64_cross_zero;


SELECT 'T64 misaligned blocks';

DROP TABLE IF EXISTS t64_misaligned_blocks;

CREATE TABLE t64_misaligned_blocks (x UInt32 CODEC(T64))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_compress_block_size = 0, max_compress_block_size = 5;

-- 5 is not a multiple of sizeof(UInt32), so every block ends with unaligned bytes (`bytes_to_skip`) and the final 1-byte block holds no 
-- whole value (`bytes_to_compress == 0`). One granule of data keeps the writer's chunking aligned with the aggregate's.
INSERT INTO t64_misaligned_blocks SELECT number FROM numbers(4);

SELECT
    column_data_compressed_bytes AS on_disk_bytes,
    toUInt64(round(column_data_uncompressed_bytes /
                   (SELECT estimateCompressionRatio('T64', 5)(x) FROM t64_misaligned_blocks))) AS aggregate_predicted_bytes,
    on_disk_bytes = aggregate_predicted_bytes AS matches
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't64_misaligned_blocks' AND active AND column = 'x';

DROP TABLE t64_misaligned_blocks;
