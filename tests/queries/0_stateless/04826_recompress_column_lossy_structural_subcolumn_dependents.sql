-- Tags: no-fasttest
-- no-fasttest: needs the SZ3 library

-- The lossy-codec guard of `ALTER TABLE ... RECOMPRESS COLUMN` must not reject dependents that
-- read the recompressed column only through a structural subcolumn (`arr.size0`, `x.null`, ...):
-- a lossy codec is applied only to the value-bearing substreams, while array sizes and null maps
-- are always written with the generic codecs of the chain, so such dependents stay valid.

SET allow_experimental_codecs = 1;
SET mutations_sync = 2;

-- A skipping index over `arr.size0` does not block the recompression, and its data stays valid.
DROP TABLE IF EXISTS t_recompress_lossy_size0_index;
CREATE TABLE t_recompress_lossy_size0_index
(
    key UInt64,
    arr Array(Float64) CODEC(SZ3('ALGO_INTERP', 'ABS', 0.01)),
    INDEX idx arr.size0 TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_recompress_lossy_size0_index
    SELECT number, arrayMap(x -> sin(x / 100.) * 50, range(number % 10)) FROM numbers(1000);

SELECT sum(length(arr)), sum(arr.size0) FROM t_recompress_lossy_size0_index;

ALTER TABLE t_recompress_lossy_size0_index RECOMPRESS COLUMN arr;

-- The sizes are bit-identical after the recompression and the index still prunes correctly.
SELECT sum(length(arr)), sum(arr.size0) FROM t_recompress_lossy_size0_index;
SELECT count() FROM t_recompress_lossy_size0_index WHERE arr.size0 = 9;

DROP TABLE t_recompress_lossy_size0_index;

-- A stored MATERIALIZED column computed from `x.null` does not block the recompression.
DROP TABLE IF EXISTS t_recompress_lossy_null_materialized;
CREATE TABLE t_recompress_lossy_null_materialized
(
    key UInt64,
    x Nullable(Float64) CODEC(SZ3('ALGO_INTERP', 'ABS', 0.01)),
    is_null UInt8 MATERIALIZED x.null
)
ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_recompress_lossy_null_materialized (key, x)
    SELECT number, if(number % 7 = 0, NULL, sin(number / 100.) * 50) FROM numbers(1000);

SELECT countIf(x IS NULL), sum(is_null) FROM t_recompress_lossy_null_materialized;

ALTER TABLE t_recompress_lossy_null_materialized RECOMPRESS COLUMN x;

SELECT countIf(x IS NULL), sum(is_null) FROM t_recompress_lossy_null_materialized;

DROP TABLE t_recompress_lossy_null_materialized;

-- A table TTL over `arr.size0` does not block the recompression either.
DROP TABLE IF EXISTS t_recompress_lossy_size0_ttl;
CREATE TABLE t_recompress_lossy_size0_ttl
(
    d Date,
    arr Array(Float64) CODEC(SZ3('ALGO_INTERP', 'ABS', 0.01))
)
ENGINE = MergeTree ORDER BY d
TTL d + toIntervalDay(arr.size0 * 3650)
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_recompress_lossy_size0_ttl
    SELECT today(), arrayMap(x -> sin(x / 100.) * 50, range(number % 10 + 1)) FROM numbers(1000);

ALTER TABLE t_recompress_lossy_size0_ttl RECOMPRESS COLUMN arr;

SELECT count(), sum(arr.size0) FROM t_recompress_lossy_size0_ttl;

DROP TABLE t_recompress_lossy_size0_ttl;

-- Control: a value-bearing subcolumn (`val.x` of a Tuple) still blocks the recompression,
-- both as an index dependency and as a stored MATERIALIZED dependency.
DROP TABLE IF EXISTS t_recompress_lossy_value_subcolumn;
CREATE TABLE t_recompress_lossy_value_subcolumn
(
    key UInt64,
    val Tuple(x Float64, y Float64) CODEC(SZ3('ALGO_INTERP', 'ABS', 0.01)),
    INDEX idx val.x TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_recompress_lossy_value_subcolumn (key, val)
    SELECT number, (sin(number / 100.) * 50, cos(number / 100.) * 50) FROM numbers(1000);

ALTER TABLE t_recompress_lossy_value_subcolumn RECOMPRESS COLUMN val; -- { serverError SUPPORT_IS_DISABLED }

DROP TABLE t_recompress_lossy_value_subcolumn;

DROP TABLE IF EXISTS t_recompress_lossy_value_subcolumn_mat;
CREATE TABLE t_recompress_lossy_value_subcolumn_mat
(
    key UInt64,
    val Tuple(x Float64, y Float64) CODEC(SZ3('ALGO_INTERP', 'ABS', 0.01)),
    mat Float64 MATERIALIZED val.x + 1
)
ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_recompress_lossy_value_subcolumn_mat (key, val)
    SELECT number, (sin(number / 100.) * 50, cos(number / 100.) * 50) FROM numbers(1000);

ALTER TABLE t_recompress_lossy_value_subcolumn_mat RECOMPRESS COLUMN val; -- { serverError SUPPORT_IS_DISABLED }

DROP TABLE t_recompress_lossy_value_subcolumn_mat;

-- A structural subcolumn of a *different* column never blocks anything.
DROP TABLE IF EXISTS t_recompress_lossy_other_size0;
CREATE TABLE t_recompress_lossy_other_size0
(
    key UInt64,
    val Float64 CODEC(SZ3('ALGO_INTERP', 'ABS', 0.01)),
    other Array(UInt64),
    INDEX idx other.size0 TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_recompress_lossy_other_size0 (key, val, other)
    SELECT number, sin(number / 100.) * 50, range(number % 5) FROM numbers(1000);

ALTER TABLE t_recompress_lossy_other_size0 RECOMPRESS COLUMN val;

SELECT count() FROM t_recompress_lossy_other_size0 WHERE other.size0 = 4;

DROP TABLE t_recompress_lossy_other_size0;
