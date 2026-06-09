-- Tags: no-random-merge-tree-settings
-- no-random-merge-tree-settings: reads system.parts_columns, which randomized enable_block_number/offset_column add rows to.

DROP TABLE IF EXISTS t_adaptive_on;
DROP TABLE IF EXISTS t_adaptive_off;
DROP TABLE IF EXISTS t_adaptive_insert;
DROP TABLE IF EXISTS t_adaptive_mutation;
DROP TABLE IF EXISTS t_adaptive_compact;

CREATE TABLE t_adaptive_on
(
    a UInt64,              -- no codec, narrow range -> T64
    b UInt64 CODEC(LZ4),   -- explicit codec -> stays LZ4
    c String,              -- non-candidate type -> default
    d Int128,              -- non-candidate integer -> default
    e Nullable(Int64),     -- candidate leaf -> T64
    f Array(Int32)         -- candidate leaf -> T64
)
ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, allow_experimental_adaptive_codec_selection = 1;

CREATE TABLE t_adaptive_off AS t_adaptive_on
ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, allow_experimental_adaptive_codec_selection = 0;

CREATE TABLE t_adaptive_insert AS t_adaptive_on
ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, allow_experimental_adaptive_codec_selection = 1;

CREATE TABLE t_adaptive_mutation AS t_adaptive_on
ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, allow_experimental_adaptive_codec_selection = 1;

CREATE TABLE t_adaptive_compact AS t_adaptive_on
ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 1000000000, allow_experimental_adaptive_codec_selection = 1;


-- Merge with the setting ON: candidate default-coded columns (a, e, f) get T64; explicit b and non-candidate c, d keep the default.
SELECT 'Adaptive ON';
INSERT INTO t_adaptive_on SELECT number, number, toString(number), number, number, [toInt32(number)] FROM numbers(50000);
INSERT INTO t_adaptive_on SELECT number, number, toString(number), number, number, [toInt32(number)] FROM numbers(50000, 50000);
OPTIMIZE TABLE t_adaptive_on FINAL;
SELECT column, mapContains(codec_block_counts, 'T64') AS has_t64
FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_adaptive_on' AND active ORDER BY column;

-- Setting OFF: nothing becomes adaptive.
SELECT 'Adaptive OFF';
INSERT INTO t_adaptive_off SELECT number, number, toString(number), number, number, [toInt32(number)] FROM numbers(50000);
INSERT INTO t_adaptive_off SELECT number, number, toString(number), number, number, [toInt32(number)] FROM numbers(50000, 50000);
OPTIMIZE TABLE t_adaptive_off FINAL;
SELECT column, mapContains(codec_block_counts, 'T64') AS has_t64
FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_adaptive_off' AND active ORDER BY column;

-- Setting ON but insert only (no merge): adaptive is merge-time only, so still the default.
SELECT 'Adaptive INSERT';
INSERT INTO t_adaptive_insert SELECT number, number, toString(number), number, number, [toInt32(number)] FROM numbers(100000);
SELECT column, mapContains(codec_block_counts, 'T64') AS has_t64
FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_adaptive_insert' AND active ORDER BY column;

-- Column-only mutation with the setting ON: ALTER UPDATE rewrites only e (the column-only mutation path), so e becomes T64.
SELECT 'Adaptive MUTATION';
INSERT INTO t_adaptive_mutation SELECT number, number, toString(number), number, number, [toInt32(number)] FROM numbers(100000);
ALTER TABLE t_adaptive_mutation UPDATE e = e + 1 WHERE 1 SETTINGS mutations_sync = 2;
SELECT column, mapContains(codec_block_counts, 'T64') AS has_t64
FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_adaptive_mutation' AND active ORDER BY column;

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
