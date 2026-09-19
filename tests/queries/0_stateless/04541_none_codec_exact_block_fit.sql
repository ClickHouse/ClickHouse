-- Regression test for the zero-copy NONE write path: the direct path reserves
-- COMPRESSED_BLOCK_PREFIX_SIZE (25) bytes for the checksum and header in front of the data inside
-- the output buffer, so a file buffer sized to exactly the configured block size leaves room for
-- only `block_size - 25` bytes of payload. A payload that exactly reaches the block size would then
-- be split into a second frame, adding one frame's worth of service bytes on disk. The buffers must
-- be sized to `block_size + COMPRESSED_BLOCK_PREFIX_SIZE` so a full block still fits in one frame.
--
-- Both checks compare an "exact-fit" table (block size == payload size) against a "huge block" table
-- that always keeps the whole payload in one frame. With the buffers sized correctly the exact-fit
-- table also uses a single frame, so the on-disk sizes are equal. If the buffer were one prefix
-- short, the exact-fit table would need an extra frame and be 25 bytes larger, and the comparisons
-- below would return 0.

DROP TABLE IF EXISTS data_none_exact;
DROP TABLE IF EXISTS data_none_huge;
DROP TABLE IF EXISTS pk_none_exact;
DROP TABLE IF EXISTS pk_none_huge;

-- Data stream (MergeTreeWriterStream): 8192 UInt64 values with CODEC(NONE) are exactly 65536 bytes,
-- so a single granule fills a 65536-byte block exactly. A non-adaptive write buffer keeps the block
-- size fixed (min_columns_to_activate_adaptive_write_buffer = 0 disables the adaptive path).
CREATE TABLE data_none_exact (n UInt64 CODEC(NONE)) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_compress_block_size = 65536, max_compress_block_size = 65536,
             index_granularity = 8192, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
             min_columns_to_activate_adaptive_write_buffer = 0;

CREATE TABLE data_none_huge (n UInt64 CODEC(NONE)) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_compress_block_size = 1048576, max_compress_block_size = 1048576,
             index_granularity = 8192, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
             min_columns_to_activate_adaptive_write_buffer = 0;

INSERT INTO data_none_exact SELECT number FROM numbers(8192);
INSERT INTO data_none_huge  SELECT number FROM numbers(8192);

-- Equal only if the 65536-byte block still fits in one frame (buffer sized to block + prefix).
SELECT (SELECT sum(data_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'data_none_exact' AND active)
     = (SELECT sum(data_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'data_none_huge' AND active);

-- The data must still be readable.
SELECT count(), sum(n) FROM data_none_exact;

-- Primary index (MergeTreeDataPartWriterOnDisk): a FixedString(256) key with index_granularity = 1
-- stores 256 bytes per index entry; 255 rows produce 256 index entries, so the primary index is
-- exactly 65536 bytes and fills a 65536-byte block exactly.
CREATE TABLE pk_none_exact (k FixedString(256)) ENGINE = MergeTree ORDER BY k
    SETTINGS compress_primary_key = 1, primary_key_compression_codec = 'NONE',
             primary_key_compress_block_size = 65536, index_granularity = 1,
             min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

CREATE TABLE pk_none_huge (k FixedString(256)) ENGINE = MergeTree ORDER BY k
    SETTINGS compress_primary_key = 1, primary_key_compression_codec = 'NONE',
             primary_key_compress_block_size = 8388608, index_granularity = 1,
             min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO pk_none_exact SELECT leftPad(toString(number), 256, '0')::FixedString(256) FROM numbers(255);
INSERT INTO pk_none_huge  SELECT leftPad(toString(number), 256, '0')::FixedString(256) FROM numbers(255);

-- Equal only if the 65536-byte primary index still fits in one frame (buffer sized to block + prefix).
SELECT (SELECT sum(primary_key_size) FROM system.parts WHERE database = currentDatabase() AND table = 'pk_none_exact' AND active)
     = (SELECT sum(primary_key_size) FROM system.parts WHERE database = currentDatabase() AND table = 'pk_none_huge' AND active);

-- The index must still be usable: a point lookup via the primary key and a full count.
SELECT count() FROM pk_none_exact WHERE k = leftPad('100', 256, '0')::FixedString(256);
SELECT count() FROM pk_none_exact;

DROP TABLE data_none_exact;
DROP TABLE data_none_huge;
DROP TABLE pk_none_exact;
DROP TABLE pk_none_huge;
