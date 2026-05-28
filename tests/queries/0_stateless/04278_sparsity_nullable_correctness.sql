-- Correctness of sparsity-based pruning / trivial-count on Nullable columns.
-- The serialization stats count NULLs as defaults
-- (`ColumnNullable::isDefaultAt(n) == isNullAt(n)`), so equality/inequality
-- predicates with non-NULL constants on Nullable columns must NOT be answered from
-- those stats. All modes must agree on the count.

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
         nullable_serialization_version = 'allow_sparse';

SYSTEM STOP MERGES t_sparse_nullable;

-- 5000 rows. 3000 NULL, 1000 zero/'', 1000 non-zero/'x'.
-- NULL ratio 60% forces sparse serialization (which makes the trivial-count rewrite
-- look at the per-column stats). NULL count (3000) != zero count (1000), so any bug
-- that answers `n = 0` from `num_defaults` (= NULL count) will return 3000 instead of 1000.
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

-- Baseline ground-truths (3000 NULL / 1000 zero / 1000 nonzero on both columns).
SELECT 'baseline isNull(n)='     , countIf(n IS NULL)     FROM t_sparse_nullable;
SELECT 'baseline isNotNull(n)='  , countIf(n IS NOT NULL) FROM t_sparse_nullable;
SELECT 'baseline n=0='           , countIf(n = 0)         FROM t_sparse_nullable;
SELECT 'baseline n!=0='          , countIf(n != 0)        FROM t_sparse_nullable;
SELECT 'baseline empty(s)='      , countIf(empty(s))      FROM t_sparse_nullable;
SELECT 'baseline notEmpty(s)='   , countIf(notEmpty(s))   FROM t_sparse_nullable;
SELECT 'baseline s=empty='       , countIf(s = '')        FROM t_sparse_nullable;
SELECT 'baseline s!=empty='      , countIf(s != '')       FROM t_sparse_nullable;

-- Predicates that *do* match NULL-as-default and should answer via stats.
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

-- col = / != literal on Nullable: must NOT be answered from NULL count.
-- Expected: all 2000 (n=0) and 2500 (n!=0). A buggy classifier would return 500 or 4500.
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
