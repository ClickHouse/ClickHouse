-- Test that two conjuncts on the same column (different SparsityPredicateClass)
-- do not double-seed the `SparseOffsetsShare`. Without dedup, a scan window
-- straddling a chunk boundary would walk two copies of the offsets and
-- produce wrong row counts under `use_sparsity_info_for_pruning = 'data_read'`.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_sparse_unsat;

-- Both `x` and `n` must actually end up sparse-serialized so the analyzer
-- inspects them: `x` needs enough zero rows to clear the sparse threshold, and
-- `Nullable` columns need `nullable_serialization_version = 'allow_sparse'` to
-- be eligible at all.
CREATE TABLE t_sparse_unsat (x UInt32, n Nullable(UInt32))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9,
         compute_exact_num_defaults_for_sparse_columns = 1,
         serialization_info_version = 'with_types',
         nullable_serialization_version = 'allow_sparse',
         min_bytes_for_wide_part = 0;

-- 95% defaults / NULLs so both columns clear the 0.9 sparse threshold.
INSERT INTO t_sparse_unsat
SELECT
    if(number % 20 = 0, number, 0)::UInt32,
    if(number % 20 = 0, number, NULL)
FROM numbers(5000);

SELECT 'kinds',
       countIf(column = 'x' AND serialization_kind = 'Sparse'),
       countIf(column = 'n' AND serialization_kind = 'Sparse')
FROM system.parts_columns
WHERE table = 't_sparse_unsat' AND database = currentDatabase() AND active;

-- Unsatisfiable; result is 0 either way, but the analyzer must not double-insert.
SELECT 'unsat_eq', count() FROM t_sparse_unsat WHERE x = 0 AND x != 0
    SETTINGS use_query_condition_cache = 0,
             use_sparsity_info_for_pruning = 'planning';

SELECT 'unsat_null', count() FROM t_sparse_unsat WHERE n IS NULL AND n IS NOT NULL
    SETTINGS use_query_condition_cache = 0,
             use_sparsity_info_for_pruning = 'planning';

SELECT 'unsat_eq_dr', count() FROM t_sparse_unsat WHERE x = 0 AND x != 0
    SETTINGS use_query_condition_cache = 0,
             use_sparsity_info_for_pruning = 'data_read';

SELECT 'unsat_null_dr', count() FROM t_sparse_unsat WHERE n IS NULL AND n IS NOT NULL
    SETTINGS use_query_condition_cache = 0,
             use_sparsity_info_for_pruning = 'data_read';

DROP TABLE t_sparse_unsat;
