-- Tags: no-old-analyzer
-- no-old-analyzer: Not supported

-- Trivial-count correctness on Nullable sparse columns. The per-column
-- `num_defaults` counts NULL rows, so classification of predicates on Nullable
-- columns must match that convention (NULL is the default, zero/empty is not).

SET optimize_trivial_count_query = 1;

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

-- 3000 NULL, 1000 zero/empty, 1000 nonzero/'x'.
INSERT INTO t_sparse_nullable
SELECT
    number,
    if(number < 3000, NULL, if(number < 4000, toUInt32(0), toUInt32(1))),
    if(number < 3000, NULL, if(number < 4000, '', 'x'))
FROM numbers(5000)
SETTINGS optimize_on_insert = 0;

-- Baseline (3000 NULL, 1000 zero, 1000 nonzero).
SELECT 'baseline isNull(n)='     , countIf(n IS NULL)     FROM t_sparse_nullable;
SELECT 'baseline isNotNull(n)='  , countIf(n IS NOT NULL) FROM t_sparse_nullable;
SELECT 'baseline n=0='           , countIf(n = 0)         FROM t_sparse_nullable;
SELECT 'baseline n!=0='          , countIf(n != 0)        FROM t_sparse_nullable;
SELECT 'baseline empty(s)='      , countIf(empty(s))      FROM t_sparse_nullable;
SELECT 'baseline notEmpty(s)='   , countIf(notEmpty(s))   FROM t_sparse_nullable;

-- Rewrite fires on NULL predicates (num_defaults counts NULLs).
SELECT 'rewrite  isNull(n)='    , count() FROM t_sparse_nullable WHERE n IS NULL
    SETTINGS optimize_trivial_count_with_sparsity_filter=1;
SELECT 'rewrite  isNotNull(n)=' , count() FROM t_sparse_nullable WHERE n IS NOT NULL
    SETTINGS optimize_trivial_count_with_sparsity_filter=1;
SELECT 'rewrite  empty(s)='     , count() FROM t_sparse_nullable WHERE empty(s)
    SETTINGS optimize_trivial_count_with_sparsity_filter=1;
SELECT 'rewrite  notEmpty(s)='  , count() FROM t_sparse_nullable WHERE notEmpty(s)
    SETTINGS optimize_trivial_count_with_sparsity_filter=1;

-- `n = 0` on a Nullable column must NOT be answered from NULL count (1000, not 3000).
SELECT 'rewrite  n=0='  , count() FROM t_sparse_nullable WHERE n = 0
    SETTINGS optimize_trivial_count_with_sparsity_filter=1;
SELECT 'rewrite  n!=0=' , count() FROM t_sparse_nullable WHERE n != 0
    SETTINGS optimize_trivial_count_with_sparsity_filter=1;
SELECT 'rewrite  s=empty=' , count() FROM t_sparse_nullable WHERE s = ''
    SETTINGS optimize_trivial_count_with_sparsity_filter=1;
SELECT 'rewrite  s!=empty=', count() FROM t_sparse_nullable WHERE s != ''
    SETTINGS optimize_trivial_count_with_sparsity_filter=1;

DROP TABLE t_sparse_nullable;
