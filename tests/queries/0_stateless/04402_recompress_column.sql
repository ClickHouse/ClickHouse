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
