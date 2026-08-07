-- Regression test: piecewise-constant data with a small decimal range must take the XOR path,
-- which collapses plateaus into runs, instead of being forced into a per-value decimal packing.
-- https://github.com/ClickHouse/ClickHouse/pull/113575

SET allow_experimental_codecs = 1;

DROP TABLE IF EXISTS plateau_base;
DROP TABLE IF EXISTS plateau_wallaby32;
DROP TABLE IF EXISTS plateau_wallaby64;

CREATE TABLE plateau_base (i UInt32 CODEC(NONE), f Float64 CODEC(NONE)) ENGINE = MergeTree ORDER BY i
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_compress_block_size = 65536, max_compress_block_size = 1048576;
CREATE TABLE plateau_wallaby32 (i UInt32 CODEC(NONE), f Float32 CODEC(Wallaby)) ENGINE = MergeTree ORDER BY i
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_compress_block_size = 65536, max_compress_block_size = 1048576;
CREATE TABLE plateau_wallaby64 (i UInt32 CODEC(NONE), f Float64 CODEC(Wallaby)) ENGINE = MergeTree ORDER BY i
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_compress_block_size = 65536, max_compress_block_size = 1048576;

SELECT '# Long plateaus of small decimals: half a vector of 1.0, half of 2.0, repeated';
INSERT INTO plateau_base SELECT number, if(intDiv(number, 512) % 2 = 0, 1.0, 2.0) FROM numbers(100000);
INSERT INTO plateau_wallaby64 SELECT i, f FROM plateau_base;
INSERT INTO plateau_wallaby32 SELECT i, toFloat32(f) FROM plateau_base;
OPTIMIZE TABLE plateau_wallaby32 FINAL;
OPTIMIZE TABLE plateau_wallaby64 FINAL;

SELECT '# Roundtrip is lossless';
SELECT count(), sum(bin(a.f) <> bin(b.f)) FROM plateau_base AS b INNER JOIN plateau_wallaby64 AS a USING i;
SELECT count(), sum(bin(toFloat32(b.f)) <> bin(a.f)) FROM plateau_base AS b INNER JOIN plateau_wallaby32 AS a USING i;

SELECT '# Plateaus collapse into runs: under 0.5 bits per value';
SELECT (SELECT sum(data_compressed_bytes) FROM system.columns WHERE database = currentDatabase() AND table = 'plateau_wallaby32' AND name = 'f') < 100000 / 16;
SELECT (SELECT sum(data_compressed_bytes) FROM system.columns WHERE database = currentDatabase() AND table = 'plateau_wallaby64' AND name = 'f') < 100000 / 16;

DROP TABLE plateau_base;
DROP TABLE plateau_wallaby32;
DROP TABLE plateau_wallaby64;
