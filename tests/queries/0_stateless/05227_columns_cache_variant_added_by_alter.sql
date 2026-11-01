-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-replicated-database
-- A `Variant` column added by an `ALTER` after a part was written is absent from that part, even
-- though its type has substreams (the null maps of its elements) that would be derived rather than
-- read. The columns cache must treat it and its subcolumns as absent - filled with defaults after
-- the read - while the other columns are served from the cache, and a block whose columns read
-- from the part produce no rows is sized by the served columns.

SET max_threads = 1;

DROP TABLE IF EXISTS t_cc_alter_variant;

CREATE TABLE t_cc_alter_variant (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY x
SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1, index_granularity = 8192;

INSERT INTO t_cc_alter_variant SELECT number, number * 2 FROM numbers(3);

SYSTEM DROP COLUMNS CACHE;

-- The columns of the part are cached by the first read.
SELECT x, y FROM t_cc_alter_variant ORDER BY x SETTINGS use_columns_cache = 1;

ALTER TABLE t_cc_alter_variant ADD COLUMN v Variant(String, UInt64);

-- `x` and `y` are served from the cache, `v` and its subcolumns are absent from the part.
SELECT x, y, v, v.String, v.UInt64 FROM t_cc_alter_variant ORDER BY x SETTINGS use_columns_cache = 1;
SELECT x, y, v, v.String, v.UInt64 FROM t_cc_alter_variant ORDER BY x SETTINGS use_columns_cache = 1;
SELECT x, y, v, v.String, v.UInt64 FROM t_cc_alter_variant ORDER BY x SETTINGS use_columns_cache = 0;

-- After an insert with values, the new part has the column and the old part still does not.
INSERT INTO t_cc_alter_variant SELECT number, number * 2, if(number % 2, 'str'::Variant(String, UInt64), number::Variant(String, UInt64)) FROM numbers(3, 3);

SELECT x, y, v, v.String, v.UInt64 FROM t_cc_alter_variant ORDER BY x SETTINGS use_columns_cache = 1;
SELECT x, y, v, v.String, v.UInt64 FROM t_cc_alter_variant ORDER BY x SETTINGS use_columns_cache = 1;
SELECT x, y, v, v.String, v.UInt64 FROM t_cc_alter_variant ORDER BY x SETTINGS use_columns_cache = 0;

SELECT column, count() > 0 FROM system.columns_cache WHERE database = currentDatabase() AND table = 't_cc_alter_variant' GROUP BY column ORDER BY column;

DROP TABLE t_cc_alter_variant;
