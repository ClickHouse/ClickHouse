-- Tags: no-old-analyzer
-- no-old-analyzer: Not supported

SET optimize_trivial_count_query = 1;

DROP TABLE IF EXISTS t_sparse_neg_zero;

CREATE TABLE t_sparse_neg_zero (x Float64)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5,
         compute_exact_num_defaults_for_sparse_columns = 1,
         serialization_info_version = 'with_types',
         min_bytes_for_wide_part = 0;

-- 4000 rows of -0.0 and 1000 rows of 1.0. None of them are the bit-pattern default
-- (+0.0), so `num_defaults` is 0. SQL `x = 0` must still match the 4000 negative
-- zeros; the sparsity rewrite would return 0 if applied, hence we must opt out.
INSERT INTO t_sparse_neg_zero SELECT if(number < 4000, -0.0, 1.0)::Float64 FROM numbers(5000);

-- Trivial count rewrite must not fire for `x = 0` on Float, so the count matches the scan.
SELECT 'rewrite', count() FROM t_sparse_neg_zero WHERE x = 0
    SETTINGS optimize_trivial_count_with_sparsity_filter = 1;
SELECT 'scan',    count() FROM t_sparse_neg_zero WHERE x = 0
    SETTINGS optimize_trivial_count_with_sparsity_filter = 0;

-- Same for `!= 0`: rewrite must not drop the negative zeros.
SELECT 'rewrite_ne', count() FROM t_sparse_neg_zero WHERE x != 0
    SETTINGS optimize_trivial_count_with_sparsity_filter = 1;
SELECT 'scan_ne',    count() FROM t_sparse_neg_zero WHERE x != 0
    SETTINGS optimize_trivial_count_with_sparsity_filter = 0;

DROP TABLE t_sparse_neg_zero;
