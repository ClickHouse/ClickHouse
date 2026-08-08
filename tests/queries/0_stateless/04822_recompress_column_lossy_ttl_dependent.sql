-- Tags: no-fasttest
-- no-fasttest: needs the SZ3 library

-- The lossy-codec guard of `ALTER TABLE ... RECOMPRESS COLUMN` must reject a column that a TTL
-- expression reads: the mutation copies the TTL bounds (`ttl.txt`) from the source part
-- unchanged, so rows and parts would keep being expired, moved, or recompressed according to the
-- values as they were before the recompression.

SET allow_experimental_codecs = 1;
SET mutations_sync = 2;

-- A table (DELETE) TTL reads the lossy column: rejected; removing the TTL unblocks it.
DROP TABLE IF EXISTS t_recompress_lossy_rows_ttl;
CREATE TABLE t_recompress_lossy_rows_ttl
(
    key UInt64,
    val Float64 CODEC(SZ3('ALGO_INTERP', 'ABS', 0.01))
)
ENGINE = MergeTree ORDER BY key
TTL toDateTime(val) + INTERVAL 100 YEAR
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_recompress_lossy_rows_ttl (key, val) SELECT number, sin(number / 100.) * 50 + 100 FROM numbers(1000);

ALTER TABLE t_recompress_lossy_rows_ttl RECOMPRESS COLUMN val; -- { serverError SUPPORT_IS_DISABLED }

ALTER TABLE t_recompress_lossy_rows_ttl REMOVE TTL;
ALTER TABLE t_recompress_lossy_rows_ttl RECOMPRESS COLUMN val;
SELECT 'rows ttl after remove ttl', count(), sum(key) FROM t_recompress_lossy_rows_ttl;

DROP TABLE t_recompress_lossy_rows_ttl;

-- A table recompression TTL reads the lossy column: rejected.
DROP TABLE IF EXISTS t_recompress_lossy_recompress_ttl;
CREATE TABLE t_recompress_lossy_recompress_ttl
(
    key UInt64,
    val Float64 CODEC(SZ3('ALGO_INTERP', 'ABS', 0.01))
)
ENGINE = MergeTree ORDER BY key
TTL toDateTime(val) + INTERVAL 100 YEAR RECOMPRESS CODEC(ZSTD(1))
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_recompress_lossy_recompress_ttl (key, val) SELECT number, sin(number / 100.) * 50 + 100 FROM numbers(1000);

ALTER TABLE t_recompress_lossy_recompress_ttl RECOMPRESS COLUMN val; -- { serverError SUPPORT_IS_DISABLED }

DROP TABLE t_recompress_lossy_recompress_ttl;

-- A column TTL whose expression reads the lossy column: rejected; removing the column TTL
-- unblocks it.
DROP TABLE IF EXISTS t_recompress_lossy_column_ttl;
CREATE TABLE t_recompress_lossy_column_ttl
(
    key UInt64,
    val Float64 CODEC(SZ3('ALGO_INTERP', 'ABS', 0.01)),
    comment String TTL toDateTime(val) + INTERVAL 100 YEAR
)
ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_recompress_lossy_column_ttl (key, val, comment) SELECT number, sin(number / 100.) * 50 + 100, toString(number) FROM numbers(1000);

ALTER TABLE t_recompress_lossy_column_ttl RECOMPRESS COLUMN val; -- { serverError SUPPORT_IS_DISABLED }

ALTER TABLE t_recompress_lossy_column_ttl MODIFY COLUMN comment REMOVE TTL;
ALTER TABLE t_recompress_lossy_column_ttl RECOMPRESS COLUMN val;
SELECT 'column ttl after remove ttl', count(), sum(key) FROM t_recompress_lossy_column_ttl;

DROP TABLE t_recompress_lossy_column_ttl;

-- A TTL that reads only other columns does not block the recompression.
DROP TABLE IF EXISTS t_recompress_lossy_unrelated_ttl;
CREATE TABLE t_recompress_lossy_unrelated_ttl
(
    key UInt64,
    val Float64 CODEC(SZ3('ALGO_INTERP', 'ABS', 0.01)),
    d DateTime
)
ENGINE = MergeTree ORDER BY key
TTL d + INTERVAL 50 YEAR
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_recompress_lossy_unrelated_ttl (key, val, d) SELECT number, sin(number / 100.) * 50 + 100, now() FROM numbers(1000);

ALTER TABLE t_recompress_lossy_unrelated_ttl RECOMPRESS COLUMN val;
SELECT 'unrelated ttl', count(), sum(key) FROM t_recompress_lossy_unrelated_ttl;

DROP TABLE t_recompress_lossy_unrelated_ttl;
