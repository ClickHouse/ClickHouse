-- Tags: no-shared-merge-tree
-- Test that two conjuncts on the same column (different SparsityPredicateClass)
-- do not double-seed the `SparseOffsetsShare`. Without dedup, a scan window
-- straddling a chunk boundary would walk two copies of the offsets and
-- produce wrong row counts under `use_sparsity_info_for_pruning = 'data_read'`.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_sparse_unsat;

CREATE TABLE t_sparse_unsat (x UInt32, n Nullable(UInt32))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5,
         compute_exact_num_defaults_for_sparse_columns = 1,
         serialization_info_version = 'with_types',
         min_bytes_for_wide_part = 0;

INSERT INTO t_sparse_unsat SELECT number, if(number % 3 = 0, NULL, number) FROM numbers(5000);

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
