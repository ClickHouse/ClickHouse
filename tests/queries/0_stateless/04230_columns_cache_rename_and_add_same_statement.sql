-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-replicated-database
-- Regression test: `ALTER TABLE ... RENAME COLUMN a TO b, ADD COLUMN a ...` in a single
-- statement reintroduces the name `a` for a different column. The columns cache must be
-- invalidated so that reads of the freshly added `a` return its default values, not stale
-- data from the previous incarnation that survived because the name itself did not disappear
-- from the new metadata.

SET use_columns_cache = 1;
SYSTEM DROP COLUMNS CACHE;

DROP TABLE IF EXISTS t_columns_cache_rename_combined;

CREATE TABLE t_columns_cache_rename_combined (id UInt64, a UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1000;

INSERT INTO t_columns_cache_rename_combined SELECT number, number + 1000 FROM numbers(3000);

SELECT sum(a), count() FROM t_columns_cache_rename_combined SETTINGS use_columns_cache = 1;
SELECT sum(a), count() FROM t_columns_cache_rename_combined SETTINGS use_columns_cache = 1;

SELECT count() FROM system.columns_cache
WHERE database = currentDatabase() AND table = 't_columns_cache_rename_combined' AND column = 'a';

ALTER TABLE t_columns_cache_rename_combined RENAME COLUMN a TO b, ADD COLUMN a UInt64 DEFAULT 42;

SELECT count() FROM system.columns_cache
WHERE database = currentDatabase() AND table = 't_columns_cache_rename_combined' AND column = 'a';

SELECT sum(a), count() FROM t_columns_cache_rename_combined SETTINGS use_columns_cache = 1;
SELECT sum(a), count() FROM t_columns_cache_rename_combined SETTINGS use_columns_cache = 1;

SELECT sum(b), count() FROM t_columns_cache_rename_combined SETTINGS use_columns_cache = 1;

DROP TABLE t_columns_cache_rename_combined;
