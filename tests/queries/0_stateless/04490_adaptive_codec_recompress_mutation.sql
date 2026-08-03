-- Tags: no-random-merge-tree-settings
-- no-random-merge-tree-settings: reads mergeTreeCodecBlockCounts, randomized block-number/offset columns add rows.

-- Mutating a part that is past its RECOMPRESS TTL rewrites it with that codec. Adaptive selection must not override it, the same as on merges.

-- Full-part mutation: DELETE rewrites every column. The new part is due for RECOMPRESS CODEC(NONE).
DROP TABLE IF EXISTS t_full;

CREATE TABLE t_full (dt DateTime, n UInt64) ENGINE = MergeTree ORDER BY n TTL dt + INTERVAL 1 SECOND RECOMPRESS CODEC(NONE)
SETTINGS min_bytes_for_wide_part = 0, allow_experimental_adaptive_codec_selection = 1;

INSERT INTO t_full SELECT now() - INTERVAL 1 DAY, number FROM numbers(100000);
ALTER TABLE t_full DELETE WHERE n = 0 SETTINGS mutations_sync = 2;

SELECT 'full-part', max(mapContains(codec_block_counts, 'T64')), max(mapContains(codec_block_counts, 'NONE'))
FROM mergeTreeCodecBlockCounts(currentDatabase(), t_full) WHERE column = 'n';

SELECT 'roundtrip', count(), sum(n) FROM t_full;

DROP TABLE t_full;

-- Column-only mutation. UPDATE rewrites only `v` (`n` is hardlinked). OPTIMIZE recompresses to NONE first. Rewritten `v` reuses that codec.
DROP TABLE IF EXISTS t_colonly;

CREATE TABLE t_colonly (dt DateTime, n UInt64, v UInt64) ENGINE = MergeTree ORDER BY n TTL dt + INTERVAL 1 SECOND RECOMPRESS CODEC(NONE)
SETTINGS min_bytes_for_wide_part = 0, allow_experimental_adaptive_codec_selection = 1;

INSERT INTO t_colonly SELECT now() - INTERVAL 1 DAY, number, number FROM numbers(100000);
OPTIMIZE TABLE t_colonly FINAL;
ALTER TABLE t_colonly UPDATE v = v + 1 WHERE 1 SETTINGS mutations_sync = 2;

SELECT 'column-only', max(mapContains(codec_block_counts, 'T64')), max(mapContains(codec_block_counts, 'NONE'))
FROM mergeTreeCodecBlockCounts(currentDatabase(), t_colonly) WHERE column = 'v';

SELECT 'roundtrip-col', count(), sum(v) FROM t_colonly;

DROP TABLE t_colonly;
