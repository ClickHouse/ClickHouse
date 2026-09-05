-- Tags: no-fasttest
-- no-fasttest: needs the SZ3 library

-- Helper expansion in the lossy-codec guard must preserve a requested structural subcolumn.
-- Both an `ALIAS` and an `EPHEMERAL` helper are expanded here; `arr.size0` and `x.null` do
-- not change under lossy recompression, so their stored MATERIALIZED dependents stay valid.

SET allow_experimental_codecs = 1;
SET mutations_sync = 2;

DROP TABLE IF EXISTS t_recompress_lossy_helper_size0;
CREATE TABLE t_recompress_lossy_helper_size0
(
    key UInt64,
    arr Array(Float64) CODEC(SZ3('ALGO_INTERP', 'ABS', 0.01)),
    a UInt64 ALIAS getSubcolumn(arr, 'size0'),
    e UInt64 EPHEMERAL a,
    size UInt64 MATERIALIZED e
)
ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_recompress_lossy_helper_size0 (key, arr)
    SELECT number, arrayMap(x -> sin(x / 100.) * 50, range(number % 10)) FROM numbers(1000);

ALTER TABLE t_recompress_lossy_helper_size0 RECOMPRESS COLUMN arr;
SELECT sum(length(arr)), sum(size) FROM t_recompress_lossy_helper_size0;

DROP TABLE t_recompress_lossy_helper_size0;

DROP TABLE IF EXISTS t_recompress_lossy_helper_null;
CREATE TABLE t_recompress_lossy_helper_null
(
    key UInt64,
    x Nullable(Float64) CODEC(SZ3('ALGO_INTERP', 'ABS', 0.01)),
    a UInt8 ALIAS getSubcolumn(x, 'null'),
    e UInt8 EPHEMERAL a,
    is_null UInt8 MATERIALIZED e
)
ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_recompress_lossy_helper_null (key, x)
    SELECT number, if(number % 7 = 0, NULL, sin(number / 100.) * 50) FROM numbers(1000);

ALTER TABLE t_recompress_lossy_helper_null RECOMPRESS COLUMN x;
SELECT countIf(x IS NULL), sum(is_null) FROM t_recompress_lossy_helper_null;

DROP TABLE t_recompress_lossy_helper_null;
