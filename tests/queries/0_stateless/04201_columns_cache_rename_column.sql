-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-replicated-database
-- Verify the columns cache is invalidated when a column is renamed away and a new
-- column with the same name is added back. Reads of the freshly added column must
-- return its default values, not stale data from the previous incarnation, and the
-- cache must not leak entries pointing at the pre-alter part.

SET use_columns_cache = 1;
SYSTEM DROP COLUMNS CACHE;

DROP TABLE IF EXISTS t_columns_cache_rename;

CREATE TABLE t_columns_cache_rename (id UInt64, a UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1000;

INSERT INTO t_columns_cache_rename SELECT number, number + 1000 FROM numbers(3000);

-- Populate cache with the original `a` column.
SELECT sum(a), count() FROM t_columns_cache_rename SETTINGS use_columns_cache = 1;
SELECT sum(a), count() FROM t_columns_cache_rename SETTINGS use_columns_cache = 1;

-- The cache should now contain an entry for column `a`.
SELECT count() FROM system.columns_cache
WHERE database = currentDatabase() AND table = 't_columns_cache_rename' AND column = 'a';

-- Rename `a` away and add a new `a` with a different default. Without invalidation,
-- the cache would still hold a stale entry for column `a` from the pre-alter part.
ALTER TABLE t_columns_cache_rename RENAME COLUMN a TO b;
ALTER TABLE t_columns_cache_rename ADD COLUMN a UInt64 DEFAULT 42;

-- After the rename the cache must not retain entries for the dropped column name.
SELECT count() FROM system.columns_cache
WHERE database = currentDatabase() AND table = 't_columns_cache_rename' AND column = 'a';

-- The new `a` is a constant 42 for all 3000 rows -> sum must be 3000 * 42 = 126000.
SELECT sum(a), count() FROM t_columns_cache_rename SETTINGS use_columns_cache = 1;
SELECT sum(a), count() FROM t_columns_cache_rename SETTINGS use_columns_cache = 1;

-- The renamed `b` still carries the original data (sum of number + 1000 over 0..2999).
SELECT sum(b), count() FROM t_columns_cache_rename SETTINGS use_columns_cache = 1;

DROP TABLE t_columns_cache_rename;
