-- Tags: no-fasttest
-- no-fasttest: needs the SZ3 library

-- The lossy-codec guard of `ALTER TABLE ... RECOMPRESS COLUMN` rejects the mutation while a projection or
-- a skip index depends on the column. Dropping the dependent in the same `ALTER` is allowed, as long as the
-- drop is written first: `DROP PROJECTION` / `DROP INDEX` are `AlterCommand`s, so they form their own
-- command segment that is executed in the order the commands were written, and the metadata drop lands
-- before the guard reads the metadata. The opposite order really does recompress while the dependent is
-- still live (two separate mutations, with a window where a query sees the stale projection), so it stays
-- rejected.

SET allow_experimental_codecs = 1;
SET mutations_sync = 2;
SET check_query_single_value_result = 1;

DROP TABLE IF EXISTS t_recompress_drop_projection;
CREATE TABLE t_recompress_drop_projection
(
    key UInt64,
    val Float64 CODEC(SZ3('ALGO_INTERP', 'ABS', 0.01)),
    PROJECTION p (SELECT key, val ORDER BY val)
)
ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_recompress_drop_projection SELECT number, sin(number / 1000.) * 100 FROM numbers(1000);

ALTER TABLE t_recompress_drop_projection DROP PROJECTION p, RECOMPRESS COLUMN val;
SELECT 'drop projection then recompress', count(), countIf(val > 0) FROM t_recompress_drop_projection;
CHECK TABLE t_recompress_drop_projection;

DROP TABLE t_recompress_drop_projection;

DROP TABLE IF EXISTS t_recompress_drop_index;
CREATE TABLE t_recompress_drop_index
(
    key UInt64,
    val Float64 CODEC(SZ3('ALGO_INTERP', 'ABS', 0.01)),
    INDEX idx val TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_recompress_drop_index SELECT number, sin(number / 1000.) * 100 FROM numbers(1000);

ALTER TABLE t_recompress_drop_index DROP INDEX idx, RECOMPRESS COLUMN val;
SELECT 'drop index then recompress', count(), countIf(val > 0) FROM t_recompress_drop_index;
CHECK TABLE t_recompress_drop_index;

DROP TABLE t_recompress_drop_index;

-- Both dependents dropped in one ALTER.
DROP TABLE IF EXISTS t_recompress_drop_both;
CREATE TABLE t_recompress_drop_both
(
    key UInt64,
    val Float64 CODEC(SZ3('ALGO_INTERP', 'ABS', 0.01)),
    INDEX idx floor(val) TYPE set(100) GRANULARITY 1,
    PROJECTION p (SELECT key, val ORDER BY val)
)
ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_recompress_drop_both SELECT number, sin(number / 1000.) * 100 FROM numbers(1000);

ALTER TABLE t_recompress_drop_both DROP INDEX idx, DROP PROJECTION p, RECOMPRESS COLUMN val;
SELECT 'drop both then recompress', count(), countIf(val > 0) FROM t_recompress_drop_both;
CHECK TABLE t_recompress_drop_both;

DROP TABLE t_recompress_drop_both;

-- Recompressing before the drop is still rejected: the recompression mutation runs while the dependent
-- is live.
DROP TABLE IF EXISTS t_recompress_before_drop;
CREATE TABLE t_recompress_before_drop
(
    key UInt64,
    val Float64 CODEC(SZ3('ALGO_INTERP', 'ABS', 0.01)),
    PROJECTION p (SELECT key, val ORDER BY val)
)
ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_recompress_before_drop SELECT number, sin(number / 1000.) * 100 FROM numbers(1000);

ALTER TABLE t_recompress_before_drop RECOMPRESS COLUMN val, DROP PROJECTION p; -- { serverError SUPPORT_IS_DISABLED }
SELECT 'recompress before drop rejected', count() FROM t_recompress_before_drop;

DROP TABLE t_recompress_before_drop;
