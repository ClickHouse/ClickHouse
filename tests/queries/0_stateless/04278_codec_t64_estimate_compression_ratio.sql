-- Tags: no-random-merge-tree-settings
-- no-random-merge-tree-settings: random settings would change block sizes and break the alignment between MergeTree's writer and the aggregate's chunking.

-- Check that `estimateCompressionRatio('T64', N)` (which uses tryGetCompressedSize internally) agrees with the on-disk size.

SELECT 'T64 default';

DROP TABLE IF EXISTS t_t64;

CREATE TABLE t_t64 (x UInt32 CODEC(T64))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_compress_block_size = 0, max_compress_block_size = 65536;

INSERT INTO t_t64 SELECT number FROM numbers(100000);

SELECT
    column_data_compressed_bytes AS on_disk_bytes,
    toUInt64(round(column_data_uncompressed_bytes /
                   (SELECT estimateCompressionRatio('T64', 65536)(x) FROM t_t64))) AS aggregate_predicted_bytes,
    on_disk_bytes = aggregate_predicted_bytes AS matches
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_t64' AND active AND column = 'x';

DROP TABLE t_t64;


SELECT 'T64 byte';

DROP TABLE IF EXISTS t_t64;

CREATE TABLE t_t64 (x UInt32 CODEC(T64('byte')))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_compress_block_size = 0, max_compress_block_size = 65536;

INSERT INTO t_t64 SELECT number FROM numbers(100000);

SELECT
    column_data_compressed_bytes AS on_disk_bytes,
    toUInt64(round(column_data_uncompressed_bytes /
                   (SELECT estimateCompressionRatio('T64(\'byte\')', 65536)(x) FROM t_t64))) AS aggregate_predicted_bytes,
    on_disk_bytes = aggregate_predicted_bytes AS matches
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_t64' AND active AND column = 'x';

DROP TABLE t_t64;


SELECT 'T64 bit';

DROP TABLE IF EXISTS t_t64;

CREATE TABLE t_t64 (x UInt32 CODEC(T64('bit')))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_compress_block_size = 0, max_compress_block_size = 65536;

INSERT INTO t_t64 SELECT number FROM numbers(100000);

SELECT
    column_data_compressed_bytes AS on_disk_bytes,
    toUInt64(round(column_data_uncompressed_bytes /
                   (SELECT estimateCompressionRatio('T64(\'bit\')', 65536)(x) FROM t_t64))) AS aggregate_predicted_bytes,
    on_disk_bytes = aggregate_predicted_bytes AS matches
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_t64' AND active AND column = 'x';

DROP TABLE t_t64;
