-- Tags: no-fasttest
-- no-fasttest: needs the SZ3 library

-- The lossy-codec guard of `ALTER TABLE ... RECOMPRESS COLUMN` compares the recompressed column against
-- the columns a projection or a skip index requires. Such a dependency list may name a subcolumn
-- (`val.x` of a `Tuple`) instead of the stored column that holds it, and reading a subcolumn reads the
-- very stream the recompression rewrites. The required columns are normalized back to their owning
-- stored column, so a dependent that reads a subcolumn is rejected too.

SET allow_experimental_codecs = 1;
SET mutations_sync = 2;
SET check_query_single_value_result = 1;

DROP TABLE IF EXISTS t_recompress_lossy_tuple_subcolumn;
CREATE TABLE t_recompress_lossy_tuple_subcolumn
(
    key UInt64,
    val Tuple(x Float64, y Float64) CODEC(SZ3('ALGO_INTERP', 'ABS', 0.01)),
    INDEX idx val.x TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_recompress_lossy_tuple_subcolumn
SELECT number, (sin(number / 1000.) * 100, cos(number / 1000.) * 100) FROM numbers(1000);

ALTER TABLE t_recompress_lossy_tuple_subcolumn RECOMPRESS COLUMN val; -- { serverError SUPPORT_IS_DISABLED }

-- Dropping the index first makes it work, as for a dependent that names the stored column directly.
ALTER TABLE t_recompress_lossy_tuple_subcolumn DROP INDEX idx, RECOMPRESS COLUMN val;
SELECT 'tuple subcolumn index dropped first', count() FROM t_recompress_lossy_tuple_subcolumn;
CHECK TABLE t_recompress_lossy_tuple_subcolumn;

DROP TABLE t_recompress_lossy_tuple_subcolumn;

-- An index over a subcolumn of a different column does not block the recompression.
DROP TABLE IF EXISTS t_recompress_lossy_other_subcolumn;
CREATE TABLE t_recompress_lossy_other_subcolumn
(
    key UInt64,
    other Tuple(x Float64, y Float64),
    val Float64 CODEC(SZ3('ALGO_INTERP', 'ABS', 0.01)),
    INDEX idx other.x TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_recompress_lossy_other_subcolumn
SELECT number, (sin(number / 1000.) * 100, cos(number / 1000.) * 100), sin(number / 1000.) * 100 FROM numbers(1000);

ALTER TABLE t_recompress_lossy_other_subcolumn RECOMPRESS COLUMN val;
SELECT 'index over a subcolumn of another column', count() FROM t_recompress_lossy_other_subcolumn;
CHECK TABLE t_recompress_lossy_other_subcolumn;

DROP TABLE t_recompress_lossy_other_subcolumn;

-- Control: with a lossless codec a dependent reading a subcolumn stays valid, so it is allowed.
DROP TABLE IF EXISTS t_recompress_lossless_tuple_subcolumn;
CREATE TABLE t_recompress_lossless_tuple_subcolumn
(
    key UInt64,
    val Tuple(x Float64, y Float64) CODEC(LZ4),
    INDEX idx val.x TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_recompress_lossless_tuple_subcolumn
SELECT number, (sin(number / 1000.) * 100, cos(number / 1000.) * 100) FROM numbers(1000);

ALTER TABLE t_recompress_lossless_tuple_subcolumn MODIFY COLUMN val Tuple(x Float64, y Float64) CODEC(ZSTD(3));
ALTER TABLE t_recompress_lossless_tuple_subcolumn RECOMPRESS COLUMN val;

SELECT 'lossless tuple subcolumn dependent', count(), countIf(val.x = sin(key / 1000.) * 100) FROM t_recompress_lossless_tuple_subcolumn;
CHECK TABLE t_recompress_lossless_tuple_subcolumn;

DROP TABLE t_recompress_lossless_tuple_subcolumn;
