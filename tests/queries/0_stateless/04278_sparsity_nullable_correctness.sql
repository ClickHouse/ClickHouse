-- Correctness of sparsity based pruning and trivial count on Nullable columns.
-- The per column `num_defaults` stat counts NULLs, so equality, inequality, empty
-- and notEmpty predicates with non NULL constants on Nullable columns must not be
-- answered from that stat. All modes must agree with the baseline count.

DROP TABLE IF EXISTS t_sparse_nullable;

CREATE TABLE t_sparse_nullable
(
    id UInt64,
    n Nullable(UInt32),
    s Nullable(String)
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 512,
         ratio_of_defaults_for_sparse_serialization = 0.5,
         compute_exact_num_defaults_for_sparse_columns = 1,
         nullable_serialization_version = 'allow_sparse',
         serialization_info_version = 'with_types',
         min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES t_sparse_nullable;

-- 5000 rows. 3000 NULL, 1000 zero or empty string, 1000 nonzero or 'x'.
-- 60% NULL ratio forces sparse serialization for the Nullable columns. NULL count
-- (3000) differs from zero count (1000) so a wrong classification of `n = 0` as
-- "matches default" would return 3000 instead of 1000.
INSERT INTO t_sparse_nullable
SELECT
    number,
    if(number < 3000, NULL, if(number < 4000, toUInt32(0), toUInt32(1))),
    if(number < 3000, NULL, if(number < 4000, '', 'x'))
FROM numbers(5000)
SETTINGS optimize_on_insert = 0;

SELECT 'serialization_kind:', column, serialization_kind
FROM system.parts_columns
WHERE table = 't_sparse_nullable' AND database = currentDatabase()
ORDER BY column;

-- Baseline ground truths (3000 NULL, 1000 zero, 1000 nonzero on both columns).
SELECT 'baseline isNull(n)='     , countIf(n IS NULL)     FROM t_sparse_nullable;
SELECT 'baseline isNotNull(n)='  , countIf(n IS NOT NULL) FROM t_sparse_nullable;
SELECT 'baseline n=0='           , countIf(n = 0)         FROM t_sparse_nullable;
SELECT 'baseline n!=0='          , countIf(n != 0)        FROM t_sparse_nullable;
SELECT 'baseline empty(s)='      , countIf(empty(s))      FROM t_sparse_nullable;
SELECT 'baseline notEmpty(s)='   , countIf(notEmpty(s))   FROM t_sparse_nullable;
SELECT 'baseline s=empty='       , countIf(s = '')        FROM t_sparse_nullable;
SELECT 'baseline s!=empty='      , countIf(s != '')       FROM t_sparse_nullable;

-- Predicates that match NULL as default and should answer via stats.
SELECT 'planning  isNull(n)='   , count() FROM t_sparse_nullable WHERE n IS NULL
    SETTINGS use_sparsity_info_for_pruning='planning' , optimize_trivial_count_with_sparsity_filter=1;
SELECT 'data_read isNull(n)='   , count() FROM t_sparse_nullable WHERE n IS NULL
    SETTINGS use_sparsity_info_for_pruning='data_read', optimize_trivial_count_with_sparsity_filter=1;
SELECT 'planning  isNotNull(n)=', count() FROM t_sparse_nullable WHERE n IS NOT NULL
    SETTINGS use_sparsity_info_for_pruning='planning' , optimize_trivial_count_with_sparsity_filter=1;
SELECT 'data_read isNotNull(n)=', count() FROM t_sparse_nullable WHERE n IS NOT NULL
    SETTINGS use_sparsity_info_for_pruning='data_read', optimize_trivial_count_with_sparsity_filter=1;
SELECT 'planning  empty(s)='    , count() FROM t_sparse_nullable WHERE empty(s)
    SETTINGS use_sparsity_info_for_pruning='planning' , optimize_trivial_count_with_sparsity_filter=1;
SELECT 'planning  notEmpty(s)=' , count() FROM t_sparse_nullable WHERE notEmpty(s)
    SETTINGS use_sparsity_info_for_pruning='planning' , optimize_trivial_count_with_sparsity_filter=1;

-- Equality and inequality against a literal on a Nullable column must not be
-- answered from the NULL count. Expected: 1000 for both `n = 0` and `n != 0`.
SELECT 'planning  n=0 trivial=1=' , count() FROM t_sparse_nullable WHERE n = 0
    SETTINGS use_sparsity_info_for_pruning='planning' , optimize_trivial_count_with_sparsity_filter=1;
SELECT 'planning  n=0 trivial=0=' , count() FROM t_sparse_nullable WHERE n = 0
    SETTINGS use_sparsity_info_for_pruning='planning' , optimize_trivial_count_with_sparsity_filter=0;
SELECT 'data_read n=0 trivial=0=' , count() FROM t_sparse_nullable WHERE n = 0
    SETTINGS use_sparsity_info_for_pruning='data_read', optimize_trivial_count_with_sparsity_filter=0;

SELECT 'planning  n!=0 trivial=1=', count() FROM t_sparse_nullable WHERE n != 0
    SETTINGS use_sparsity_info_for_pruning='planning' , optimize_trivial_count_with_sparsity_filter=1;
SELECT 'planning  n!=0 trivial=0=', count() FROM t_sparse_nullable WHERE n != 0
    SETTINGS use_sparsity_info_for_pruning='planning' , optimize_trivial_count_with_sparsity_filter=0;
SELECT 'data_read n!=0 trivial=0=', count() FROM t_sparse_nullable WHERE n != 0
    SETTINGS use_sparsity_info_for_pruning='data_read', optimize_trivial_count_with_sparsity_filter=0;

SELECT 'planning  s=empty trivial=1=' , count() FROM t_sparse_nullable WHERE s = ''
    SETTINGS use_sparsity_info_for_pruning='planning' , optimize_trivial_count_with_sparsity_filter=1;
SELECT 'data_read s=empty trivial=0=' , count() FROM t_sparse_nullable WHERE s = ''
    SETTINGS use_sparsity_info_for_pruning='data_read', optimize_trivial_count_with_sparsity_filter=0;
SELECT 'planning  s!=empty trivial=1=', count() FROM t_sparse_nullable WHERE s != ''
    SETTINGS use_sparsity_info_for_pruning='planning' , optimize_trivial_count_with_sparsity_filter=1;
SELECT 'data_read s!=empty trivial=0=', count() FROM t_sparse_nullable WHERE s != ''
    SETTINGS use_sparsity_info_for_pruning='data_read', optimize_trivial_count_with_sparsity_filter=0;

DROP TABLE t_sparse_nullable;
