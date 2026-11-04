-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-replicated-database
-- The null map of a `Nullable` column with sparse serialization has no stream of its own: it is
-- derived from the sparse offsets while reading. The columns cache must read such a column like
-- any other - it is not a column absent from the part - and cache it like any other.

SET max_threads = 1;

DROP TABLE IF EXISTS t_cc_sparse_null;

CREATE TABLE t_cc_sparse_null (id UInt64, n Nullable(Int32), u UInt32)
ENGINE = MergeTree ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9, nullable_serialization_version = 'allow_sparse', min_bytes_for_wide_part = 0, index_granularity = 1024;

INSERT INTO t_cc_sparse_null SELECT number, if(number % 20 = 0, number::Int32, NULL), if(number % 20 = 0, number, 0) FROM numbers(10000);

SELECT column, serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_cc_sparse_null' AND active AND column IN ('n', 'u') ORDER BY column;

SYSTEM DROP COLUMNS CACHE;

-- The null map subcolumn alone, the whole column, and both columns: without the cache, with the
-- cache while it is filled, and with the cache warm.
SELECT 'null map', countIf(n IS NULL), countIf(n IS NOT NULL) FROM t_cc_sparse_null SETTINGS use_columns_cache = 0;
SELECT 'null map', countIf(n IS NULL), countIf(n IS NOT NULL) FROM t_cc_sparse_null SETTINGS use_columns_cache = 1;
SELECT 'null map', countIf(n IS NULL), countIf(n IS NOT NULL) FROM t_cc_sparse_null SETTINGS use_columns_cache = 1;

SELECT 'values', sum(n), sum(u), count() FROM t_cc_sparse_null SETTINGS use_columns_cache = 0;
SELECT 'values', sum(n), sum(u), count() FROM t_cc_sparse_null SETTINGS use_columns_cache = 1;
SELECT 'values', sum(n), sum(u), count() FROM t_cc_sparse_null SETTINGS use_columns_cache = 1;

SELECT 'filtered', count(), sum(u) FROM t_cc_sparse_null WHERE n IS NOT NULL SETTINGS use_columns_cache = 0;
SELECT 'filtered', count(), sum(u) FROM t_cc_sparse_null WHERE n IS NOT NULL SETTINGS use_columns_cache = 1;
SELECT 'filtered', count(), sum(u) FROM t_cc_sparse_null WHERE n IS NOT NULL SETTINGS use_columns_cache = 1;

SELECT column, count() > 0 FROM system.columns_cache WHERE database = currentDatabase() AND table = 't_cc_sparse_null' GROUP BY column ORDER BY column;

DROP TABLE t_cc_sparse_null;
