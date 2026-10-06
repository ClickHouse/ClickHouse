-- Tags: no-old-analyzer
-- no-old-analyzer: Not supported

-- `count(expr)` counts non-NULL values of `expr`. The trivial-count-with-sparsity-filter
-- rewrite seeds the aggregate state with a row count derived from `num_defaults`, which
-- matches `count()` semantics only. The rewrite must not fire for `count(expr)`.

SET optimize_trivial_count_query = 1;
SET optimize_trivial_count_with_sparsity_filter = 1;
SET optimize_functions_to_subcolumns = 0;

DROP TABLE IF EXISTS t_count_expr;

CREATE TABLE t_count_expr (id UInt64, n Nullable(UInt32))
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 512,
         ratio_of_defaults_for_sparse_serialization = 0.5,
         compute_exact_num_defaults_for_sparse_columns = 1,
         nullable_serialization_version = 'allow_sparse',
         serialization_info_version = 'with_types',
         min_bytes_for_wide_part = 0;

INSERT INTO t_count_expr SELECT number, if(number < 3000, NULL, toUInt32(0)) FROM numbers(5000) SETTINGS optimize_on_insert = 0;

-- `count(n)` counts non-NULL rows: 2000. If the rewrite fired, it would return
-- `num_defaults` (3000 NULLs), which would be wrong.
SELECT 'count(n) IS NULL:',    count(n) FROM t_count_expr WHERE n IS NULL;
SELECT 'count(n) IS NOT NULL:', count(n) FROM t_count_expr WHERE n IS NOT NULL;

-- Plan check: rewrite must NOT be present for `count(n)`, but must be present for `count()`.
SELECT 'rewrite count(n):', countIf(explain LIKE '%Optimized trivial count with sparsity filter%') FROM (EXPLAIN SELECT count(n) FROM t_count_expr WHERE n IS NULL);
SELECT 'rewrite count():', countIf(explain LIKE '%Optimized trivial count with sparsity filter%') FROM (EXPLAIN SELECT count() FROM t_count_expr WHERE n IS NULL);

DROP TABLE t_count_expr;
