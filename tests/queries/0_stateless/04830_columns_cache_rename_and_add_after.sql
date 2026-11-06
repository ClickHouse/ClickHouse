-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-replicated-database
-- Regression test: `ALTER TABLE ... RENAME COLUMN a TO b, ADD COLUMN a ... AFTER id` in a
-- single statement, where the reintroduced `a` is inserted back into `a`'s old position.
-- The resulting column list `(id, a, b)` has the same positional prefix `(id, a)` as the
-- old schema `(id, a)`, so a prefix-only identity check would treat the change as a pure
-- addition of `b` and keep the stale cache entries for the old `a`.

SET use_columns_cache = 1;
SYSTEM DROP COLUMNS CACHE;

DROP TABLE IF EXISTS t_columns_cache_rename_after;

CREATE TABLE t_columns_cache_rename_after (id UInt64, a UInt64 DEFAULT 42)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1000;

-- Populate with non-default values: a = number + 1000, not 42.
INSERT INTO t_columns_cache_rename_after SELECT number, number + 1000 FROM numbers(3000);

-- Populate the cache with the original `a` data: sum = sum(number) + 3000 * 1000 = 4498500 + 3000000 = 7498500.
SELECT sum(a), count() FROM t_columns_cache_rename_after SETTINGS use_columns_cache = 1;
SELECT sum(a), count() FROM t_columns_cache_rename_after SETTINGS use_columns_cache = 1;

SELECT count() FROM system.columns_cache
WHERE database = currentDatabase() AND table = 't_columns_cache_rename_after' AND column = 'a';

-- Rename `a` away and add a new `a` with the SAME signature back into `a`'s old slot.
-- The new column list is (id, a, b): the old prefix (id, a) matches positionally and
-- only the renamed `b` is left over at the end.
ALTER TABLE t_columns_cache_rename_after RENAME COLUMN a TO b, ADD COLUMN a UInt64 DEFAULT 42 AFTER id;

SELECT count() FROM system.columns_cache
WHERE database = currentDatabase() AND table = 't_columns_cache_rename_after' AND column = 'a';

-- The new `a` has only the default 42 on disk -> sum must be 42 * 3000 = 126000.
SELECT sum(a), count() FROM t_columns_cache_rename_after SETTINGS use_columns_cache = 1;
SELECT sum(a), count() FROM t_columns_cache_rename_after SETTINGS use_columns_cache = 1;

-- The renamed `b` still carries the original data.
SELECT sum(b), count() FROM t_columns_cache_rename_after SETTINGS use_columns_cache = 1;

DROP TABLE t_columns_cache_rename_after;
