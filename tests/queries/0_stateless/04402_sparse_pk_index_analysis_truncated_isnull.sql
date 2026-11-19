-- Tags: no-random-settings, no-random-merge-tree-settings
-- no-random-merge-tree-settings: truncation of suffix PK columns depends on a fixed index_granularity and the skip-suffix ratio.
-- EXPLAIN indexes output differs under parallel replicas, so enable_parallel_replicas = 0 is pinned per query.

-- A nullable suffix PK column `b` is dropped from the in-memory primary index because the
-- high-cardinality prefix `a` (unique per granule, distinct-mark ratio ~0.99 clears the 0.9 threshold)
-- triggers primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns. The ratio must be in
-- (0, 1): optimizeIndexColumns (IMergeTreeDataPart.cpp:1454) only drops suffix columns when
-- 0 < ratio < 1, so the control table at 0.0 keeps `b` loaded. The `!is_key_col_present` branch of
-- the FUNCTION_IS_NULL/FUNCTION_IS_NOT_NULL handler in sparse index analysis (KeyCondition.cpp) is
-- reached only when a present prefix column keeps the loaded sparse key set non-empty: the
-- `a >= 90 AND b IS NULL/IS NOT NULL` cases below constrain `a`, so checkInRange runs and meets the
-- missing `b` column. Filtering on `b` alone references only the truncated suffix, which is not loaded
-- in the sparse prefix, so the used key columns reduce to nothing usable and no granule is pruned --
-- those queries only cover the empty-sparse fallback and legacy/lightweight parity. primary_key_lazy_load = 0 keeps
-- primary_key_bytes_in_memory populated.

SET enable_analyzer = 1;
SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS t_sparse_pk_trunc_isnull;
DROP TABLE IF EXISTS t_full_pk_isnull;

CREATE TABLE t_sparse_pk_trunc_isnull (a UInt64, b Nullable(UInt64))
ENGINE = MergeTree
ORDER BY (a, b)
SETTINGS index_granularity = 1, primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 0.9, primary_key_lazy_load = 0, allow_nullable_key = 1, min_bytes_for_wide_part = 0;

-- Control: identical data with suffix truncation disabled (ratio = 0), so `b` stays in the index.
CREATE TABLE t_full_pk_isnull (a UInt64, b Nullable(UInt64))
ENGINE = MergeTree
ORDER BY (a, b)
SETTINGS index_granularity = 1, primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 0.0, primary_key_lazy_load = 0, allow_nullable_key = 1, min_bytes_for_wide_part = 0;

INSERT INTO t_sparse_pk_trunc_isnull SELECT number, if(number % 7 = 0, NULL, number) FROM numbers(100);
INSERT INTO t_full_pk_isnull SELECT number, if(number % 7 = 0, NULL, number) FROM numbers(100);

-- Guard the premise: `b` must actually be dropped from the sparse index, otherwise the queries
-- below would follow the present-column path instead of the `!is_key_col_present` branch.
SELECT 'b skipped from sparse index',
       (SELECT sum(primary_key_bytes_in_memory) FROM system.parts WHERE active AND database = currentDatabase() AND table = 't_sparse_pk_trunc_isnull')
       < (SELECT sum(primary_key_bytes_in_memory) FROM system.parts WHERE active AND database = currentDatabase() AND table = 't_full_pk_isnull');

-- { echoOn }

-- Filtering only on the truncated suffix `b`: the only referenced key column is not loaded in the
-- sparse prefix, so no usable key column remains and no granule is pruned
-- (Granules: 100/100). This covers the empty-sparse fallback and that the lightweight path (=1)
-- matches the legacy path (=0); the `!is_key_col_present` branch itself is exercised by the
-- prefix-constrained cases below.
EXPLAIN indexes = 1
SELECT count() FROM t_sparse_pk_trunc_isnull
WHERE b IS NULL
SETTINGS use_lightweight_primary_key_index_analysis = 0, enable_parallel_replicas = 0;

EXPLAIN indexes = 1
SELECT count() FROM t_sparse_pk_trunc_isnull
WHERE b IS NULL
SETTINGS use_lightweight_primary_key_index_analysis = 1, enable_parallel_replicas = 0;

SELECT count() FROM t_sparse_pk_trunc_isnull
WHERE b IS NULL
SETTINGS use_lightweight_primary_key_index_analysis = 1, enable_parallel_replicas = 0;

EXPLAIN indexes = 1
SELECT count() FROM t_sparse_pk_trunc_isnull
WHERE b IS NOT NULL
SETTINGS use_lightweight_primary_key_index_analysis = 0, enable_parallel_replicas = 0;

EXPLAIN indexes = 1
SELECT count() FROM t_sparse_pk_trunc_isnull
WHERE b IS NOT NULL
SETTINGS use_lightweight_primary_key_index_analysis = 1, enable_parallel_replicas = 0;

SELECT count() FROM t_sparse_pk_trunc_isnull
WHERE b IS NOT NULL
SETTINGS use_lightweight_primary_key_index_analysis = 1, enable_parallel_replicas = 0;

-- Constraining the present prefix `a` alongside the truncated-suffix IS NULL/IS NOT NULL: `a` keeps
-- the loaded sparse key set non-empty, so checkInRange runs and hits the `!is_key_col_present` branch
-- for the missing `b` column. Pruning still fires on `a` (Granules: 11/100), and the lightweight path
-- matches the legacy path. The sparse path (=1) is asserted with its own EXPLAIN so pruning is
-- checked, not just the count.
EXPLAIN indexes = 1
SELECT count() FROM t_sparse_pk_trunc_isnull
WHERE a >= 90 AND b IS NULL
SETTINGS use_lightweight_primary_key_index_analysis = 0, enable_parallel_replicas = 0;

EXPLAIN indexes = 1
SELECT count() FROM t_sparse_pk_trunc_isnull
WHERE a >= 90 AND b IS NULL
SETTINGS use_lightweight_primary_key_index_analysis = 1, enable_parallel_replicas = 0;

SELECT count() FROM t_sparse_pk_trunc_isnull
WHERE a >= 90 AND b IS NULL
SETTINGS use_lightweight_primary_key_index_analysis = 1, enable_parallel_replicas = 0;

EXPLAIN indexes = 1
SELECT count() FROM t_sparse_pk_trunc_isnull
WHERE a >= 90 AND b IS NOT NULL
SETTINGS use_lightweight_primary_key_index_analysis = 0, enable_parallel_replicas = 0;

EXPLAIN indexes = 1
SELECT count() FROM t_sparse_pk_trunc_isnull
WHERE a >= 90 AND b IS NOT NULL
SETTINGS use_lightweight_primary_key_index_analysis = 1, enable_parallel_replicas = 0;

SELECT count() FROM t_sparse_pk_trunc_isnull
WHERE a >= 90 AND b IS NOT NULL
SETTINGS use_lightweight_primary_key_index_analysis = 1, enable_parallel_replicas = 0;

-- { echoOff }

DROP TABLE t_sparse_pk_trunc_isnull;
DROP TABLE t_full_pk_isnull;
