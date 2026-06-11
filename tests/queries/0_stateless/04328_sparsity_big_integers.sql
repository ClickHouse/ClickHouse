-- Tags: no-old-analyzer
-- no-old-analyzer: Not supported

-- Big integer columns (Int128 / Int256 / UInt128 / UInt256) classify the same way
-- as native integers for the sparsity-filter equality patterns: SQL `col = 0`
-- matches exactly the rows the persisted `num_defaults` counter records.

SET optimize_trivial_count_query = 1;

DROP TABLE IF EXISTS t_sparse_bigint;

CREATE TABLE t_sparse_bigint
(
    i128 Int128,
    u128 UInt128,
    i256 Int256,
    u256 UInt256
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5,
         compute_exact_num_defaults_for_sparse_columns = 1,
         serialization_info_version = 'with_types',
         min_bytes_for_wide_part = 0;

INSERT INTO t_sparse_bigint
SELECT
    if(number < 4000, 0::Int128,  1::Int128),
    if(number < 4000, 0::UInt128, 1::UInt128),
    if(number < 4000, 0::Int256,  1::Int256),
    if(number < 4000, 0::UInt256, 1::UInt256)
FROM numbers(5000);

-- The rewrite must fire and return the same value as the scan.
SELECT 'i128_rewrite', count() FROM t_sparse_bigint WHERE i128 = 0
    SETTINGS optimize_trivial_count_with_sparsity_filter = 1, use_sparsity_info_for_pruning = 'off';
SELECT 'i128_scan',    count() FROM t_sparse_bigint WHERE i128 = 0
    SETTINGS optimize_trivial_count_with_sparsity_filter = 0, use_sparsity_info_for_pruning = 'off';

SELECT 'u128_rewrite', count() FROM t_sparse_bigint WHERE u128 != 0
    SETTINGS optimize_trivial_count_with_sparsity_filter = 1, use_sparsity_info_for_pruning = 'off';
SELECT 'u128_scan',    count() FROM t_sparse_bigint WHERE u128 != 0
    SETTINGS optimize_trivial_count_with_sparsity_filter = 0, use_sparsity_info_for_pruning = 'off';

SELECT 'i256_rewrite', count() FROM t_sparse_bigint WHERE i256 = 0
    SETTINGS optimize_trivial_count_with_sparsity_filter = 1, use_sparsity_info_for_pruning = 'off';
SELECT 'i256_scan',    count() FROM t_sparse_bigint WHERE i256 = 0
    SETTINGS optimize_trivial_count_with_sparsity_filter = 0, use_sparsity_info_for_pruning = 'off';

SELECT 'u256_rewrite', count() FROM t_sparse_bigint WHERE u256 != 0
    SETTINGS optimize_trivial_count_with_sparsity_filter = 1, use_sparsity_info_for_pruning = 'off';
SELECT 'u256_scan',    count() FROM t_sparse_bigint WHERE u256 != 0
    SETTINGS optimize_trivial_count_with_sparsity_filter = 0, use_sparsity_info_for_pruning = 'off';

-- Confirm the rewrite physically fires (not the scan path).
SELECT replaceRegexpAll(explain, '\\d+', '#') AS plan
FROM (
    EXPLAIN SELECT count() FROM t_sparse_bigint WHERE i256 = 0
    SETTINGS optimize_trivial_count_with_sparsity_filter = 1
);

DROP TABLE t_sparse_bigint;
