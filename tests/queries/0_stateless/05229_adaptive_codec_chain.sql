-- Tags: no-random-merge-tree-settings
-- no-random-merge-tree-settings: randomized granularity and compress block sizes give tiny blocks where the framing decides the winner.

DROP TABLE IF EXISTS t_chain_lz4;
DROP TABLE IF EXISTS t_chain_two_stage_default;
DROP TABLE IF EXISTS t_chain_none_default;

CREATE TABLE t_chain_lz4
(
    x UInt64,  -- sorted: T64 keeps the varying bits and LZ4 squeezes the pattern they form
    y UInt64,  -- random 12-bit values: T64 alone, LZ4 cannot take more than the chain framing off its output
    z UInt64,  -- hashes: nothing shrinks them, stored raw
    f Float64, -- random two-decimal values: ALP alone
    g Float64  -- sorted two-decimal values: ALP packs them into a regular bit pattern that LZ4 squeezes
)
ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, enable_adaptive_codec_selection = 1, default_compression_codec = 'LZ4';

INSERT INTO t_chain_lz4 SELECT number, cityHash64(number) % 4096, cityHash64(number), (cityHash64(number) % 100000) / 100, round(number / 100, 2) FROM numbers(300000);
OPTIMIZE TABLE t_chain_lz4 FINAL; -- inserts aren't adaptive, merges are

SELECT column, arraySort(mapKeys(codec_block_counts)) FROM mergeTreeCodecBlockCounts(currentDatabase(), t_chain_lz4) ORDER BY column;
SELECT count(), sum(x), sum(y), sum(z), max(f), max(g) FROM t_chain_lz4;

-- A multi-stage default is not chained.
CREATE TABLE t_chain_two_stage_default (x UInt64)
ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, enable_adaptive_codec_selection = 1, default_compression_codec = 'LZ4, ZSTD(1)';

INSERT INTO t_chain_two_stage_default SELECT number FROM numbers(300000);
OPTIMIZE TABLE t_chain_two_stage_default FINAL;

SELECT column, arraySort(mapKeys(codec_block_counts)) FROM mergeTreeCodecBlockCounts(currentDatabase(), t_chain_two_stage_default);

-- NONE is not chained either.
CREATE TABLE t_chain_none_default (x UInt64)
ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, enable_adaptive_codec_selection = 1, default_compression_codec = 'NONE';

INSERT INTO t_chain_none_default SELECT number FROM numbers(300000);
OPTIMIZE TABLE t_chain_none_default FINAL;

SELECT column, arraySort(mapKeys(codec_block_counts)) FROM mergeTreeCodecBlockCounts(currentDatabase(), t_chain_none_default);

DROP TABLE t_chain_lz4;
DROP TABLE t_chain_two_stage_default;
DROP TABLE t_chain_none_default;
