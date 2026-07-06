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

ALTER TABLE t_recompress_compact MODIFY COLUMN s CODEC(ZSTD);
ALTER TABLE t_recompress_compact RECOMPRESS COLUMN s;

SELECT 'compact after', count(), countIf(s = repeat('b', 100)) FROM t_recompress_compact;

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

CREATE TABLE t_recompress_lz4_source (id UInt64, s String CODEC(LZ4))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_recompress_lz4_source SELECT number, repeat('a', 100) FROM numbers(100000);

-- Recompress an LZ4 source to ZSTD: the source blocks are decompressed with LZ4 during recompression.
ALTER TABLE t_recompress_lz4_source MODIFY COLUMN s CODEC(ZSTD);
ALTER TABLE t_recompress_lz4_source RECOMPRESS COLUMN s;

SELECT 'lz4 source after', count(), countIf(s = repeat('a', 100)), countIf(id < 100000) FROM t_recompress_lz4_source;
SELECT 'lz4 source point', s = repeat('a', 100) FROM t_recompress_lz4_source WHERE id = 99999;

CHECK TABLE t_recompress_lz4_source;

DROP TABLE t_recompress_lz4_source;
