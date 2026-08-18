-- Tags: no-random-merge-tree-settings
-- no-random-merge-tree-settings: reads mergeTreeCodecBlockCounts, randomized block-number/offset columns add rows.

-- Adaptive codec selection must not override an explicit RECOMPRESS CODEC(X). RECOMPRESS CODEC(Default) stays adaptive.

DROP TABLE IF EXISTS t_h;
DROP TABLE IF EXISTS t_v;
DROP TABLE IF EXISTS t_def;

-- RECOMPRESS CODEC(NONE), horizontal merge on wide part (as there's no adaptivity on compact).
CREATE TABLE t_h (dt DateTime, n UInt64) ENGINE = MergeTree ORDER BY n
TTL dt + INTERVAL 1 SECOND RECOMPRESS CODEC(NONE)
SETTINGS min_bytes_for_wide_part = 0, allow_experimental_adaptive_codec_selection = 1, enable_vertical_merge_algorithm = 0;

INSERT INTO t_h SELECT now() - INTERVAL 1 DAY, number FROM numbers(100000);

OPTIMIZE TABLE t_h FINAL;

-- RECOMPRESS CODEC(NONE), vertical merge.
CREATE TABLE t_v (dt DateTime, n UInt64) ENGINE = MergeTree ORDER BY dt
TTL dt + INTERVAL 1 SECOND RECOMPRESS CODEC(NONE)
SETTINGS min_bytes_for_wide_part = 0, allow_experimental_adaptive_codec_selection = 1, 
         vertical_merge_algorithm_min_columns_to_activate = 1, vertical_merge_algorithm_min_rows_to_activate = 1;

INSERT INTO t_v SELECT now() - INTERVAL 1 DAY, number FROM numbers(100000);

OPTIMIZE TABLE t_v FINAL;

-- RECOMPRESS CODEC(Default): adaptive still applies, so monotonic `n` becomes T64.
CREATE TABLE t_def (dt DateTime, n UInt64) ENGINE = MergeTree ORDER BY n
TTL dt + INTERVAL 1 SECOND RECOMPRESS CODEC(Default)
SETTINGS min_bytes_for_wide_part = 0, allow_experimental_adaptive_codec_selection = 1;

INSERT INTO t_def SELECT now() - INTERVAL 1 DAY, number FROM numbers(100000);

OPTIMIZE TABLE t_def FINAL;

-- NONE cases: `n` is only NONE.
SELECT 'horizontal', max(mapContains(codec_block_counts, 'T64')), max(mapContains(codec_block_counts, 'NONE'))
FROM mergeTreeCodecBlockCounts(currentDatabase(), t_h) WHERE column = 'n';
SELECT 'vertical', max(mapContains(codec_block_counts, 'T64')), max(mapContains(codec_block_counts, 'NONE'))
FROM mergeTreeCodecBlockCounts(currentDatabase(), t_v) WHERE column = 'n';

-- Default case: adaptive still applies, so `n` is T64.
SELECT 'default', max(mapContains(codec_block_counts, 'T64')), max(mapContains(codec_block_counts, 'NONE'))
FROM mergeTreeCodecBlockCounts(currentDatabase(), t_def) WHERE column = 'n';

SELECT 'roundtrip', count(), sum(n) FROM t_def;

DROP TABLE t_h;
DROP TABLE t_v;
DROP TABLE t_def;
