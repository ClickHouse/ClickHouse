-- Tags: no-fasttest
-- no-fasttest: needs the SZ3 library

-- The lossy-codec guard of `ALTER TABLE ... RECOMPRESS COLUMN` also covers stored MATERIALIZED
-- columns: they are dependents that are not rebuilt by the recompression (their values are copied
-- from the source part unchanged).
--
-- The partition key and the sorting key cases cannot be constructed anymore: a lossy codec on a
-- column backing either key is rejected at CREATE and ALTER time (see
-- 04791_lossy_codec_key_column), so the corresponding `RECOMPRESS COLUMN` guards only protect
-- tables attached from metadata written by earlier versions and are not testable here.

-- `CHECK TABLE` is used only on the lossless control: a part holding data inserted through a lossy
-- codec fails the `uncompressed_hash` check by construction (the stored hash describes the original
-- values, the decompression returns the approximated ones), independently of `RECOMPRESS COLUMN`.

SET allow_experimental_codecs = 1;
SET mutations_sync = 2;
SET check_query_single_value_result = 1;

-- A stored MATERIALIZED column is computed from the lossy column: rejected.
DROP TABLE IF EXISTS t_recompress_lossy_materialized;
CREATE TABLE t_recompress_lossy_materialized
(
    key UInt64,
    val Float64 CODEC(SZ3('ALGO_INTERP', 'ABS', 0.01)),
    doubled Float64 MATERIALIZED val * 2
)
ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_recompress_lossy_materialized (key, val) SELECT number, sin(number / 1000.) * 100 FROM numbers(1000);

ALTER TABLE t_recompress_lossy_materialized RECOMPRESS COLUMN val; -- { serverError SUPPORT_IS_DISABLED }

-- Removing the MATERIALIZED property first in the same ALTER makes it work.
ALTER TABLE t_recompress_lossy_materialized MODIFY COLUMN doubled REMOVE MATERIALIZED, RECOMPRESS COLUMN val;
SELECT 'materialized property removed first', count(), sum(key) FROM t_recompress_lossy_materialized;

DROP TABLE t_recompress_lossy_materialized;

-- A MATERIALIZED column reading a subcolumn of the lossy column is rejected too.
DROP TABLE IF EXISTS t_recompress_lossy_materialized_subcolumn;
CREATE TABLE t_recompress_lossy_materialized_subcolumn
(
    key UInt64,
    val Tuple(x Float64, y Float64) CODEC(SZ3('ALGO_INTERP', 'ABS', 0.01)),
    projected Float64 MATERIALIZED val.x
)
ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_recompress_lossy_materialized_subcolumn (key, val)
SELECT number, (sin(number / 1000.) * 100, cos(number / 1000.) * 100) FROM numbers(1000);

ALTER TABLE t_recompress_lossy_materialized_subcolumn RECOMPRESS COLUMN val; -- { serverError SUPPORT_IS_DISABLED }

DROP TABLE t_recompress_lossy_materialized_subcolumn;

-- A MATERIALIZED column computed from a different column does not block the recompression.
DROP TABLE IF EXISTS t_recompress_lossy_materialized_other;
CREATE TABLE t_recompress_lossy_materialized_other
(
    key UInt64,
    val Float64 CODEC(SZ3('ALGO_INTERP', 'ABS', 0.01)),
    doubled UInt64 MATERIALIZED key * 2
)
ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_recompress_lossy_materialized_other (key, val) SELECT number, sin(number / 1000.) * 100 FROM numbers(1000);

ALTER TABLE t_recompress_lossy_materialized_other RECOMPRESS COLUMN val;
SELECT 'materialized over another column', count(), countIf(doubled = key * 2) FROM t_recompress_lossy_materialized_other;

DROP TABLE t_recompress_lossy_materialized_other;

-- Control: with a lossless codec the partition key, the sorting key and the MATERIALIZED dependents
-- stay valid, so the recompression is allowed.
DROP TABLE IF EXISTS t_recompress_lossless_dependents;
CREATE TABLE t_recompress_lossless_dependents
(
    key UInt64,
    val Float64 CODEC(LZ4),
    doubled Float64 MATERIALIZED val * 2
)
ENGINE = MergeTree PARTITION BY toUInt64(val / 100) ORDER BY (key, val)
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_recompress_lossless_dependents (key, val) SELECT number, number / 10. FROM numbers(1000);

ALTER TABLE t_recompress_lossless_dependents MODIFY COLUMN val Float64 CODEC(ZSTD(3));
ALTER TABLE t_recompress_lossless_dependents RECOMPRESS COLUMN val;

SELECT 'lossless dependents', count(), countIf(val = key / 10.), countIf(doubled = val * 2) FROM t_recompress_lossless_dependents;
CHECK TABLE t_recompress_lossless_dependents;

DROP TABLE t_recompress_lossless_dependents;
