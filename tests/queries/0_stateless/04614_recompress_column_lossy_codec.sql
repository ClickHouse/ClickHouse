-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings
-- no-fasttest: needs the SZ3 library
-- no-random-settings, no-random-merge-tree-settings: the test requires stable wide parts, and SZ3 is
-- lossy, so its output depends on how the data is split into compressed blocks.

-- `ALTER TABLE ... RECOMPRESS COLUMN` to a lossy codec (`SZ3`) must not take the in-place wide-part
-- fast path. That path re-emits the raw compressed blocks one-to-one and relies on the decompressed
-- bytes staying byte-identical (it carries over the source part's uncompressed checksums and marks),
-- and it never sees the column values, so it cannot run the writer-side vector-dimension setup that
-- normal `SZ3` writes perform. `splitAndModifyMutationCommands` routes a `RECOMPRESS COLUMN` whose
-- target codec resolves to a lossy codec on any data substream through the whole-part rewrite, which
-- re-serializes the column through the regular writer.
--
-- `CHECK TABLE` is not asserted for the `SZ3` tables: the stored uncompressed checksum can never match
-- what `CHECK TABLE` recomputes for a lossy codec, for normally inserted parts as well -- see
-- https://github.com/ClickHouse/ClickHouse/issues/111139.

SET allow_experimental_codecs = 1;
SET mutations_sync = 2;
SET check_query_single_value_result = 1;

-- Scalar Float64 column: lossless -> SZ3.
DROP TABLE IF EXISTS t_recompress_lossy;
CREATE TABLE t_recompress_lossy (key UInt64, orig Float64, val Float64 CODEC(ZSTD(1)))
ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_recompress_lossy SELECT number, sin(number / 1000.) * 100, sin(number / 1000.) * 100 FROM numbers(100000);

SELECT DISTINCT 'scalar wide part', part_type FROM system.parts
WHERE database = currentDatabase() AND table = 't_recompress_lossy' AND active;

ALTER TABLE t_recompress_lossy MODIFY COLUMN val Float64 CODEC(SZ3('ALGO_INTERP', 'ABS', 0.01));
ALTER TABLE t_recompress_lossy RECOMPRESS COLUMN val;

-- The data was actually recompressed with the lossy codec (the values changed -- a leftover of the
-- old lossless data would read back bit-exact) and reads back within the codec's absolute error bound.
SELECT 'scalar lossy applied, within error', sum(val != orig) > 0, max(abs(val - orig)) <= 0.05 FROM t_recompress_lossy;

DROP TABLE t_recompress_lossy;

-- Array(Float64) column: the lossy codec applies to the nested float substream, and normal `SZ3`
-- writes derive the vector dimension from the array sizes -- something the raw-block path could not do.
DROP TABLE IF EXISTS t_recompress_lossy_arr;
CREATE TABLE t_recompress_lossy_arr (key UInt64, orig Array(Float64), arr Array(Float64) CODEC(ZSTD(1)))
ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_recompress_lossy_arr
SELECT number, [sin(number / 1000.) * 100, cos(number / 1000.) * 100, sin(number / 500.) * 10], [sin(number / 1000.) * 100, cos(number / 1000.) * 100, sin(number / 500.) * 10]
FROM numbers(100000);

SELECT DISTINCT 'array wide part', part_type FROM system.parts
WHERE database = currentDatabase() AND table = 't_recompress_lossy_arr' AND active;

ALTER TABLE t_recompress_lossy_arr MODIFY COLUMN arr Array(Float64) CODEC(SZ3('ALGO_INTERP', 'ABS', 0.01));
ALTER TABLE t_recompress_lossy_arr RECOMPRESS COLUMN arr;

SELECT 'array lengths preserved', sum(length(arr) != 3) = 0 FROM t_recompress_lossy_arr;
SELECT 'array lossy applied, within error', sum(arr != orig) > 0, max(arrayMax(arrayMap((o, r) -> abs(o - r), orig, arr))) <= 0.05 FROM t_recompress_lossy_arr;

DROP TABLE t_recompress_lossy_arr;

-- A lossy codec inside a pipeline must also be detected (`CompressionCodecMultiple` reports lossiness
-- of its inner codecs).
DROP TABLE IF EXISTS t_recompress_lossy_pipeline;
CREATE TABLE t_recompress_lossy_pipeline (key UInt64, orig Float64, val Float64 CODEC(ZSTD(1)))
ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_recompress_lossy_pipeline SELECT number, sin(number / 1000.) * 100, sin(number / 1000.) * 100 FROM numbers(100000);

ALTER TABLE t_recompress_lossy_pipeline MODIFY COLUMN val Float64 CODEC(SZ3('ALGO_INTERP', 'ABS', 0.01), LZ4);
ALTER TABLE t_recompress_lossy_pipeline RECOMPRESS COLUMN val;

SELECT 'pipeline lossy applied, within error', sum(val != orig) > 0, max(abs(val - orig)) <= 0.05 FROM t_recompress_lossy_pipeline;

DROP TABLE t_recompress_lossy_pipeline;

-- Control: the lossiness check resolves the codec against each data substream's type (like codec
-- validation does), so a type-dependent codec such as argument-less `Delta` on an `Array` column must
-- keep working with the in-place fast path -- and stay lossless.
DROP TABLE IF EXISTS t_recompress_delta_arr;
CREATE TABLE t_recompress_delta_arr (key UInt64, v Array(UInt64) CODEC(LZ4))
ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_recompress_delta_arr SELECT number, [number, number + 1, number + 2] FROM numbers(100000);

ALTER TABLE t_recompress_delta_arr MODIFY COLUMN v Array(UInt64) CODEC(Delta, ZSTD(1));
ALTER TABLE t_recompress_delta_arr RECOMPRESS COLUMN v;

SELECT 'delta array data intact', sum(arraySum(v)) = (SELECT sum(3 * number + 3) FROM numbers(100000)) FROM t_recompress_delta_arr;
CHECK TABLE t_recompress_delta_arr;

DROP TABLE t_recompress_delta_arr;
