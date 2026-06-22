-- Tags: no-random-merge-tree-settings
-- no-random-merge-tree-settings: truncation of suffix PK columns depends on fixed index_granularity and the skip-suffix ratio.

-- A nullable suffix PK column `b` is dropped from the in-memory primary index because the
-- high-cardinality prefix `a` (unique per granule, so its distinct-mark ratio of ~0.99 clears the
-- 0.9 threshold) triggers primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns. The
-- ratio must be in (0, 1): optimizeIndexColumns (IMergeTreeDataPart.cpp:1454) only drops suffix
-- columns when 0 < ratio < 1, so 0.0 would leave `b` loaded and follow the normal present-column
-- path. Filtering on `b IS NULL` / `b IS NOT NULL` then references a key column absent from the
-- sparse representation, exercising the `!is_key_col_present` branch of FUNCTION_IS_NULL in sparse
-- index analysis. primary_key_lazy_load = 0 keeps primary_key_bytes_in_memory populated so the
-- shape assertion below is deterministic.

DROP TABLE IF EXISTS t_sparse_pk_trunc_isnull;
DROP TABLE IF EXISTS t_full_pk_isnull;

CREATE TABLE t_sparse_pk_trunc_isnull (a UInt64, b Nullable(UInt64))
ENGINE = MergeTree
ORDER BY (a, b)
SETTINGS index_granularity = 1, primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 0.9, primary_key_lazy_load = 0, allow_nullable_key = 1, min_bytes_for_wide_part = 0;

-- Control: identical data with suffix truncation disabled (ratio = 0), so `b` stays in the index.
CREATE TABLE t_full_pk_isnull (a UInt64, b Nullable(UInt64))
ENGINE = MergeTree
ORDER BY (a, b)
SETTINGS index_granularity = 1, primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 0.0, primary_key_lazy_load = 0, allow_nullable_key = 1, min_bytes_for_wide_part = 0;

INSERT INTO t_sparse_pk_trunc_isnull SELECT number, if(number % 7 = 0, NULL, number) FROM numbers(100);
INSERT INTO t_full_pk_isnull SELECT number, if(number % 7 = 0, NULL, number) FROM numbers(100);

-- Prove `b` is actually skipped: the truncated index keeps fewer bytes in memory than the full one.
-- If suffix truncation stops dropping `b`, both sides become equal and this assertion fails.
SELECT 'b skipped from sparse index',
       (SELECT sum(primary_key_bytes_in_memory) FROM system.parts WHERE active AND database = currentDatabase() AND table = 't_sparse_pk_trunc_isnull')
       < (SELECT sum(primary_key_bytes_in_memory) FROM system.parts WHERE active AND database = currentDatabase() AND table = 't_full_pk_isnull');

SELECT count() FROM t_sparse_pk_trunc_isnull WHERE b IS NULL SETTINGS use_lightweight_primary_key_index_analysis = 1;
SELECT count() FROM t_sparse_pk_trunc_isnull WHERE b IS NULL SETTINGS use_lightweight_primary_key_index_analysis = 0;
SELECT count() FROM t_sparse_pk_trunc_isnull WHERE b IS NOT NULL SETTINGS use_lightweight_primary_key_index_analysis = 1;
SELECT count() FROM t_sparse_pk_trunc_isnull WHERE b IS NOT NULL SETTINGS use_lightweight_primary_key_index_analysis = 0;

DROP TABLE t_sparse_pk_trunc_isnull;
DROP TABLE t_full_pk_isnull;
