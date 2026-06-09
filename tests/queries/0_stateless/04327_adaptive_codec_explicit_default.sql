-- Tags: no-random-merge-tree-settings
-- no-random-merge-tree-settings: reads system.parts_columns, which randomised enable_block_number/offset_column add rows to.

DROP TABLE IF EXISTS t_explicit_default_on;
DROP TABLE IF EXISTS t_explicit_default_off;

CREATE TABLE t_explicit_default_on
(
    a UInt64 CODEC(Default),        -- lone Default -> adaptive (T64)
    b UInt64,                       -- no codec -> adaptive (T64), control
    c UInt64 CODEC(NONE, Default),  -- compound, Default second -> stays explicit, no adaptive T64
    d UInt64 CODEC(LZ4),            -- explicit -> no adaptive T64, control
    e UInt64 CODEC(Default, NONE)   -- compound, Default first -> stays explicit, no adaptive T64
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, allow_experimental_adaptive_codec_selection = 1, allow_suspicious_codecs = 1;

CREATE TABLE t_explicit_default_off AS t_explicit_default_on
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, allow_experimental_adaptive_codec_selection = 0, allow_suspicious_codecs = 1;

SELECT 'Adaptive ON';
INSERT INTO t_explicit_default_on SELECT number, number, number, number, number FROM numbers(50000);
INSERT INTO t_explicit_default_on SELECT number, number, number, number, number FROM numbers(50000, 50000);
OPTIMIZE TABLE t_explicit_default_on FINAL;
SELECT column, mapContains(codec_block_counts, 'T64') AS has_t64
FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_explicit_default_on' AND active ORDER BY column;

SELECT 'Adaptive OFF';
INSERT INTO t_explicit_default_off SELECT number, number, number, number, number FROM numbers(50000);
INSERT INTO t_explicit_default_off SELECT number, number, number, number, number FROM numbers(50000, 50000);
OPTIMIZE TABLE t_explicit_default_off FINAL;
SELECT column, mapContains(codec_block_counts, 'T64') AS has_t64
FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_explicit_default_off' AND active ORDER BY column;

-- Read back to exercise the part.
SELECT 'read', count(), sum(a) FROM t_explicit_default_on;

DROP TABLE t_explicit_default_on;
DROP TABLE t_explicit_default_off;
