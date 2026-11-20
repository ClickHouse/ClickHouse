-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-replicated-database
-- Regression test: `ALTER TABLE ... RENAME COLUMN a TO b, ADD COLUMN a ...` in a single
-- statement where the freshly added `a` has the SAME type and default as the old `a`.
-- The cache must still be invalidated, because the new `a` is a different column with
-- different on-disk data, even though its `(name, type, default)` signature matches.

SET use_columns_cache = 1;
SYSTEM DROP COLUMNS CACHE;

DROP TABLE IF EXISTS t_columns_cache_rename_same_sig;

CREATE TABLE t_columns_cache_rename_same_sig (id UInt64, a UInt64 DEFAULT 42)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1000;

-- Populate with non-default values: a = number + 1000, not 42.
INSERT INTO t_columns_cache_rename_same_sig SELECT number, number + 1000 FROM numbers(3000);

-- Populate the cache with the original `a` data: sum = sum(number + 1000) over [0, 3000) = 4498500 + 3000000 = 7498500.
SELECT sum(a), count() FROM t_columns_cache_rename_same_sig SETTINGS use_columns_cache = 1;
SELECT sum(a), count() FROM t_columns_cache_rename_same_sig SETTINGS use_columns_cache = 1;

SELECT count() FROM system.columns_cache
WHERE database = currentDatabase() AND table = 't_columns_cache_rename_same_sig' AND column = 'a';

-- Rename `a` away and add a new `a` with the SAME signature (UInt64 DEFAULT 42).
-- The cache must drop the stale entry: the new `a` has only the default 42 on disk,
-- so reads must return 42 * 3000 = 126000.
ALTER TABLE t_columns_cache_rename_same_sig RENAME COLUMN a TO b, ADD COLUMN a UInt64 DEFAULT 42;

SELECT count() FROM system.columns_cache
WHERE database = currentDatabase() AND table = 't_columns_cache_rename_same_sig' AND column = 'a';

SELECT sum(a), count() FROM t_columns_cache_rename_same_sig SETTINGS use_columns_cache = 1;
SELECT sum(a), count() FROM t_columns_cache_rename_same_sig SETTINGS use_columns_cache = 1;

-- The renamed `b` still carries the original data.
SELECT sum(b), count() FROM t_columns_cache_rename_same_sig SETTINGS use_columns_cache = 1;

DROP TABLE t_columns_cache_rename_same_sig;
