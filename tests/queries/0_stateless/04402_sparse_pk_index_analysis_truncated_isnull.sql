-- Tags: no-random-merge-tree-settings
-- no-random-merge-tree-settings: truncation of suffix PK columns depends on fixed index_granularity and the skip-suffix ratio.

-- A nullable suffix PK column `b` is dropped from the in-memory primary index because the
-- high-cardinality prefix `a` triggers primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns.
-- Filtering on `b IS NULL` / `b IS NOT NULL` then references a key column absent from the sparse
-- representation, exercising the `!is_key_col_present` branch of FUNCTION_IS_NULL in sparse index analysis.

DROP TABLE IF EXISTS t_sparse_pk_trunc_isnull;

CREATE TABLE t_sparse_pk_trunc_isnull (a UInt64, b Nullable(UInt64))
ENGINE = MergeTree
ORDER BY (a, b)
SETTINGS index_granularity = 1, primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 0.0, allow_nullable_key = 1, min_bytes_for_wide_part = 0;

INSERT INTO t_sparse_pk_trunc_isnull SELECT number, if(number % 7 = 0, NULL, number) FROM numbers(100);

SELECT count() FROM t_sparse_pk_trunc_isnull WHERE b IS NULL SETTINGS use_lightweight_primary_key_index_analysis = 1;
SELECT count() FROM t_sparse_pk_trunc_isnull WHERE b IS NULL SETTINGS use_lightweight_primary_key_index_analysis = 0;
SELECT count() FROM t_sparse_pk_trunc_isnull WHERE b IS NOT NULL SETTINGS use_lightweight_primary_key_index_analysis = 1;
SELECT count() FROM t_sparse_pk_trunc_isnull WHERE b IS NOT NULL SETTINGS use_lightweight_primary_key_index_analysis = 0;

DROP TABLE t_sparse_pk_trunc_isnull;
