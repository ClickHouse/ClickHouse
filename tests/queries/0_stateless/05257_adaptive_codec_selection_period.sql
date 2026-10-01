-- Tags: no-random-merge-tree-settings
-- no-random-merge-tree-settings: the counts need 64 KB blocks of 8192 UInt64 values.

DROP TABLE IF EXISTS t_period_1;
DROP TABLE IF EXISTS t_period_8;

CREATE TABLE t_period_1 (x UInt64) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, enable_adaptive_codec_selection = 1, default_compression_codec = 'LZ4', adaptive_codec_selection_period = 1;
CREATE TABLE t_period_8 AS t_period_1 SETTINGS adaptive_codec_selection_period = 8;

-- Four regimes of 8 blocks, each changing the winner. Period 8 keeps a winner until the window's sampled block, or until it does badly.
INSERT INTO t_period_1 SELECT multiIf(number < 65536, number % 4096, number < 131072, intDiv(number, 64), number < 196608, number % 4096, cityHash64(number)) FROM numbers(262144);
INSERT INTO t_period_8 SELECT multiIf(number < 65536, number % 4096, number < 131072, intDiv(number, 64), number < 196608, number % 4096, cityHash64(number)) FROM numbers(262144);

-- Inserts are not adaptive, merges are.
OPTIMIZE TABLE t_period_1 FINAL;
OPTIMIZE TABLE t_period_8 FINAL;

SELECT codec_block_counts FROM mergeTreeCodecBlockCounts(currentDatabase(), t_period_1);
SELECT codec_block_counts FROM mergeTreeCodecBlockCounts(currentDatabase(), t_period_8);

SELECT count(), sum(x) FROM t_period_1;
SELECT count(), sum(x) FROM t_period_8;
CHECK TABLE t_period_1 SETTINGS check_query_single_value_result = 1;
CHECK TABLE t_period_8 SETTINGS check_query_single_value_result = 1;

DROP TABLE t_period_1;
DROP TABLE t_period_8;
