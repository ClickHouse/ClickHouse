-- Tags: no-parallel-replicas

-- A constant whose conversion to the column type is lossy must not be classified
-- as matching the column default. For `d Decimal(9, 2)` the predicate
-- `d = toDecimal32('0.001', 3)` would otherwise truncate `0.001` to `0.00` and be
-- treated as `MatchesDefault`, making the trivial-count rewrite return the
-- default-row count instead of the true count of zero matching rows.

SET enable_analyzer = 1;
SET optimize_trivial_count_with_sparsity_filter = 1;

DROP TABLE IF EXISTS t_dec_lossy SYNC;

CREATE TABLE t_dec_lossy (id UInt64, d Decimal(9, 2))
ENGINE = MergeTree ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5,
         serialization_info_version = 'with_types',
         min_bytes_for_wide_part = 0;

INSERT INTO t_dec_lossy
SELECT number, if(number < 4000, toDecimal32(0, 2), toDecimal32('1.5', 2))
FROM numbers(5000)
SETTINGS optimize_on_insert = 0;

-- Truth: no row equals 0.001.
SELECT 'truth',         count() FROM t_dec_lossy WHERE d = toDecimal32('0.001', 3)
    SETTINGS optimize_trivial_count_with_sparsity_filter = 0;

-- Rewrite must not classify the lossy constant as the default.
SELECT 'rewrite_lossy', count() FROM t_dec_lossy WHERE d = toDecimal32('0.001', 3);

-- Sanity: the exact-default predicate is still recognised and answered from the stat.
SELECT 'rewrite_exact', count() FROM t_dec_lossy WHERE d = toDecimal32(0, 2);

DROP TABLE t_dec_lossy SYNC;
