-- Tests ALTER TABLE ... RECOMPRESS COLUMN: re-compressing a column's data with its current codec.

SET mutations_sync = 2;
SET check_query_single_value_result = 1;

-- Wide parts: recompression happens without deserializing the values.
DROP TABLE IF EXISTS t_recompress_wide;

CREATE TABLE t_recompress_wide (id UInt64, s String CODEC(NONE))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_recompress_wide SELECT number, repeat('a', 100) FROM numbers(100000);

SELECT DISTINCT 'wide part', part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_recompress_wide' AND active;

SELECT 'wide before', count(), countIf(s = repeat('a', 100)), countIf(id < 100000) FROM t_recompress_wide;
SELECT 'wide none is large', sum(data_compressed_bytes) > 5000000 FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_recompress_wide' AND column = 's' AND active;

-- Changing the codec is metadata-only; RECOMPRESS applies it to existing data.
ALTER TABLE t_recompress_wide MODIFY COLUMN s CODEC(ZSTD);
ALTER TABLE t_recompress_wide RECOMPRESS COLUMN s;

SELECT 'wide after', count(), countIf(s = repeat('a', 100)), countIf(id < 100000) FROM t_recompress_wide;
SELECT 'wide zstd is small', sum(data_compressed_bytes) < 1000000 FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_recompress_wide' AND column = 's' AND active;

-- Exercise mark-based reads (point lookup and a scattered scan) to validate the rewritten marks.
SELECT 'wide point', s = repeat('a', 100) FROM t_recompress_wide WHERE id = 99999;
SELECT 'wide scan', count() FROM t_recompress_wide WHERE id % 7 = 0 AND s = repeat('a', 100);

CHECK TABLE t_recompress_wide;

DROP TABLE t_recompress_wide;

-- Compact parts: recompression falls back to a full re-serialization of the part.
DROP TABLE IF EXISTS t_recompress_compact;

CREATE TABLE t_recompress_compact (id UInt64, s String CODEC(NONE))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = '1G', min_rows_for_wide_part = 1000000000;

INSERT INTO t_recompress_compact SELECT number, repeat('b', 100) FROM numbers(1000);

SELECT DISTINCT 'compact part', part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_recompress_compact' AND active;

SELECT 'compact before', count(), countIf(s = repeat('b', 100)) FROM t_recompress_compact;
-- The stored bytes of a CODEC(NONE) column are as large as the uncompressed data.
SELECT 'compact none is large', sum(data_compressed_bytes) > 90000 FROM system.parts
WHERE database = currentDatabase() AND table = 't_recompress_compact' AND active;

ALTER TABLE t_recompress_compact MODIFY COLUMN s CODEC(ZSTD);
ALTER TABLE t_recompress_compact RECOMPRESS COLUMN s;

SELECT 'compact after', count(), countIf(s = repeat('b', 100)) FROM t_recompress_compact;
-- Prove the fallback actually rewrote the stored bytes with the new codec.
SELECT 'compact zstd is small', sum(data_compressed_bytes) < 20000 FROM system.parts
WHERE database = currentDatabase() AND table = 't_recompress_compact' AND active;

-- Compact parts are the explicit non-fast-path case: the part must be re-serialized as a whole,
-- so the raw-block recompression counter of the mutation must stay at zero.
SYSTEM FLUSH LOGS part_log;
SELECT 'compact fallback no raw blocks', count() > 0, sum(ProfileEvents['MutationRecompressedBlocks']) FROM system.part_log
WHERE database = currentDatabase() AND table = 't_recompress_compact' AND event_type = 'MutatePart';

CHECK TABLE t_recompress_compact;

DROP TABLE t_recompress_compact;

-- Inherited default codec on a Wide part: a column without an explicit CODEC is recompressed with
-- the table's *current* `default_compression_codec`, not the codec stored in the source part. The
-- wide fast path cannot represent a default-codec change (it would recompress with the part's stored
-- default and silently do nothing), so such columns are re-serialized as a whole part instead.
DROP TABLE IF EXISTS t_recompress_inherited;

CREATE TABLE t_recompress_inherited (id UInt64, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, default_compression_codec = 'NONE';

INSERT INTO t_recompress_inherited SELECT number, repeat('a', 100) FROM numbers(100000);

SELECT DISTINCT 'inherited part', part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_recompress_inherited' AND active;
SELECT 'inherited none is large', sum(data_compressed_bytes) > 5000000 FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_recompress_inherited' AND column = 's' AND active;

-- Change the table-wide default codec (metadata-only; `s` has no explicit CODEC).
ALTER TABLE t_recompress_inherited MODIFY SETTING default_compression_codec = 'ZSTD';
ALTER TABLE t_recompress_inherited RECOMPRESS COLUMN s;

SELECT 'inherited after', count(), countIf(s = repeat('a', 100)), countIf(id < 100000) FROM t_recompress_inherited;
SELECT 'inherited zstd is small', sum(data_compressed_bytes) < 1000000 FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_recompress_inherited' AND column = 's' AND active;

CHECK TABLE t_recompress_inherited;

DROP TABLE t_recompress_inherited;

-- Regression for a heap-buffer-overflow (caught under ASan): decompressing an LZ4-compressed source
-- block wide-copies in 8-byte chunks and can write a few bytes past the exact decompressed size, so
-- `readBlock` must reserve the codec's trailing slack. The other cases above compress from CODEC(NONE)
-- sources, which never exercise the LZ4 decompress path. Uses highly compressible data so the source
-- blocks reach the full ~1 MiB decompressed size, matching the failure seen in the stress test.
DROP TABLE IF EXISTS t_recompress_lz4_source;

-- Pin full part storage: the raw-block fast path only applies to wide parts with full (local)
-- storage, and packed storage (`min_bytes_for_full_part_storage` may be randomized in tests)
-- would silently route through the whole-part fallback instead of the path under test.
CREATE TABLE t_recompress_lz4_source (id UInt64, s String CODEC(LZ4))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

-- Insert a single part: a background merge of several insert parts between the size snapshot below
-- and the size check after the ALTER would rewrite the data with a different block structure and
-- confound the size comparison. (`SYSTEM STOP MERGES` is not an option -- it also blocks mutations.)
INSERT INTO t_recompress_lz4_source SELECT number, repeat('a', 100) FROM numbers(100000)
SETTINGS max_insert_threads = 1, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 268435456;

-- The source is genuinely LZ4-compressed: ~10 MB of repeated bytes compress to well under a
-- megabyte, far from the CODEC(NONE) size, so recompression really has LZ4 blocks to decompress.
SELECT 'lz4 source is compressed', sum(column_data_compressed_bytes) BETWEEN 20000 AND 5000000, sum(column_data_uncompressed_bytes) > 10000000 FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_recompress_lz4_source' AND column = 's' AND active;

DROP TABLE IF EXISTS t_recompress_lz4_sizes;
CREATE TABLE t_recompress_lz4_sizes ENGINE = Memory AS
SELECT sum(column_data_compressed_bytes) AS size_before FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_recompress_lz4_source' AND column = 's' AND active;

-- Recompress an LZ4 source to ZSTD: the source blocks are decompressed with LZ4 during recompression.
ALTER TABLE t_recompress_lz4_source MODIFY COLUMN s CODEC(ZSTD);
ALTER TABLE t_recompress_lz4_source RECOMPRESS COLUMN s;

SELECT 'lz4 source after', count(), countIf(s = repeat('a', 100)), countIf(id < 100000) FROM t_recompress_lz4_source;
SELECT 'lz4 source point', s = repeat('a', 100) FROM t_recompress_lz4_source WHERE id = 99999;

-- Prove the stored bytes were actually rewritten with the new codec: the raw-block fast path
-- re-emits the source blocks one-to-one and both codecs are deterministic, so a recompression
-- that silently no-ops (or re-applies the source codec) would leave the compressed size exactly
-- unchanged. A different size, still far below the uncompressed size, means the blocks really were
-- re-encoded by another codec. (An absolute LZ4-vs-ZSTD size comparison is deliberately avoided:
-- which of the two is smaller on a long repeated run flips with the compression block layout,
-- which test-level randomization controls.)
SELECT 'lz4 bytes rewritten', sum(column_data_compressed_bytes) != (SELECT any(size_before) FROM t_recompress_lz4_sizes), sum(column_data_compressed_bytes) < 5000000 FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_recompress_lz4_source' AND column = 's' AND active;

SYSTEM FLUSH LOGS part_log;
SELECT 'lz4 raw path used', count() > 0, sum(ProfileEvents['MutationRecompressedBlocks']) > 0 FROM system.part_log
WHERE database = currentDatabase() AND table = 't_recompress_lz4_source' AND event_type = 'MutatePart';

CHECK TABLE t_recompress_lz4_source;

DROP TABLE t_recompress_lz4_sizes;
DROP TABLE t_recompress_lz4_source;
