-- Tags: no-random-merge-tree-settings
-- no-random-merge-tree-settings: block counts depend on the default codec, and randomized enable_block_number/offset_column add rows on merge.

DROP TABLE IF EXISTS t_adaptive_on;
DROP TABLE IF EXISTS t_adaptive_off;
DROP TABLE IF EXISTS t_adaptive_insert;
DROP TABLE IF EXISTS t_adaptive_mutation;
DROP TABLE IF EXISTS t_adaptive_compact;

CREATE TABLE t_adaptive_on
(
    a UInt64,              -- no codec, narrow range -> T64
    b UInt64 CODEC(LZ4),   -- explicit codec -> stays LZ4
    c String,              -- no specialized candidate -> NONE or default per block (its `.size` substream has one and goes adaptive)
    d Int128,              -- no specialized candidate -> NONE or default per block
    e Nullable(Int64),     -- candidate leaf -> T64
    f Array(Int32)         -- candidate leaf -> T64
)
ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, enable_adaptive_codec_selection = 1;

CREATE TABLE t_adaptive_off AS t_adaptive_on
ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, enable_adaptive_codec_selection = 0;

CREATE TABLE t_adaptive_insert AS t_adaptive_on
ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, enable_adaptive_codec_selection = 1;

CREATE TABLE t_adaptive_mutation AS t_adaptive_on
ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, enable_adaptive_codec_selection = 1;

CREATE TABLE t_adaptive_compact AS t_adaptive_on
ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 1000000000, enable_adaptive_codec_selection = 1;


-- Merge with the setting ON: default-coded columns with a specialized candidate (a, e, f) get T64,
-- c and d select between NONE and the default, explicit b stays LZ4.
SELECT 'Adaptive ON';
INSERT INTO t_adaptive_on SELECT number, number, toString(number), number, number, [toInt32(number)] FROM numbers(50000);
INSERT INTO t_adaptive_on SELECT number, number, toString(number), number, number, [toInt32(number)] FROM numbers(50000, 50000);
OPTIMIZE TABLE t_adaptive_on FINAL;
SELECT * FROM mergeTreeCodecBlockCounts(currentDatabase(), t_adaptive_on) ORDER BY part_name, column, substream;

-- Setting OFF: nothing becomes adaptive.
SELECT 'Adaptive OFF';
INSERT INTO t_adaptive_off SELECT number, number, toString(number), number, number, [toInt32(number)] FROM numbers(50000);
INSERT INTO t_adaptive_off SELECT number, number, toString(number), number, number, [toInt32(number)] FROM numbers(50000, 50000);
OPTIMIZE TABLE t_adaptive_off FINAL;
SELECT * FROM mergeTreeCodecBlockCounts(currentDatabase(), t_adaptive_off) ORDER BY part_name, column, substream;

-- Setting ON but insert only (no merge): adaptive is merge-time only, so still the default.
SELECT 'Adaptive INSERT';
INSERT INTO t_adaptive_insert SELECT number, number, toString(number), number, number, [toInt32(number)] FROM numbers(100000);
SELECT * FROM mergeTreeCodecBlockCounts(currentDatabase(), t_adaptive_insert) ORDER BY part_name, column, substream;

-- Column-only mutation with the setting ON: ALTER UPDATE rewrites only e (the column-only mutation path), so e becomes T64.
SELECT 'Adaptive MUTATION';
INSERT INTO t_adaptive_mutation SELECT number, number, toString(number), number, number, [toInt32(number)] FROM numbers(100000);
ALTER TABLE t_adaptive_mutation UPDATE e = e + 1 WHERE 1 SETTINGS mutations_sync = 2;
SELECT * FROM mergeTreeCodecBlockCounts(currentDatabase(), t_adaptive_mutation) ORDER BY part_name, column, substream;

-- Compact part with the setting ON: codec_block_counts is empty for Compact, so verify the data round-trips and the part checks out.
SELECT 'Adaptive COMPACT';
INSERT INTO t_adaptive_compact SELECT number, number, toString(number), number, number, [toInt32(number)] FROM numbers(1000);
INSERT INTO t_adaptive_compact SELECT number, number, toString(number), number, number, [toInt32(number)] FROM numbers(1000, 1000);
OPTIMIZE TABLE t_adaptive_compact FINAL;
SELECT count(), sum(a) FROM t_adaptive_compact;
SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_adaptive_compact' AND active;
CHECK TABLE t_adaptive_compact SETTINGS check_query_single_value_result = 1;

DROP TABLE t_adaptive_on;
DROP TABLE t_adaptive_off;
DROP TABLE t_adaptive_insert;
DROP TABLE t_adaptive_mutation;
DROP TABLE t_adaptive_compact;
