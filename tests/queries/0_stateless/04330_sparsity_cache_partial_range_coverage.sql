-- Tags: no-old-analyzer, no-parallel
-- no-old-analyzer: Not supported
-- no-parallel: SYSTEM DROP QUERY CONDITION CACHE is server-wide

-- Regression test: the granule analyzer must not cache a verdict for the whole
-- part when it only inspected a subset of the part's marks. A first query with
-- a narrow primary-key range would otherwise leave a partial bitmap behind that
-- a follow-up full-range query reads as the complete verdict, suppressing
-- pruning for the marks the first query never analyzed.

DROP TABLE IF EXISTS t_partial_cache;

CREATE TABLE t_partial_cache (id UInt64, x UInt32)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 100,
         ratio_of_defaults_for_sparse_serialization = 0.5,
         compute_exact_num_defaults_for_sparse_columns = 1,
         serialization_info_version = 'with_types',
         min_bytes_for_wide_part = 0;

-- 10 granules of 100 rows. Only rows 0 and 500 are non-default, so granule 0 and
-- granule 5 are mixed and the other 8 granules are all-default; `x != 0` should
-- prune 8 of 10 granules on a fresh full-range analysis.
INSERT INTO t_partial_cache SELECT number, if(number IN (0, 500), 1, 0)::UInt32 FROM numbers(1000)
SETTINGS optimize_on_insert = 0;

SELECT 'kind', serialization_kind FROM system.parts_columns
WHERE table = 't_partial_cache' AND database = currentDatabase() AND column = 'x' AND active;

SYSTEM DROP QUERY CONDITION CACHE;

-- Q1: narrow PK range only covers marks 0..4. Without the fix this primes the
-- cache with a partial bitmap.
SELECT 'q1', count() FROM t_partial_cache WHERE id < 500 AND x != 0
    SETTINGS use_sparsity_info_for_pruning = 'planning',
             use_query_condition_cache = 1;

-- Q2: full range. The Sparsity step must still drop 8 of 10 granules; under the
-- old behaviour Q1's partial cache would suppress pruning of marks 6..9 and
-- this row would read `Granules: 6/10`.
SELECT 'q2_pruning' AS what, explain FROM (
    EXPLAIN indexes = 1 SELECT id FROM t_partial_cache WHERE x != 0
    SETTINGS use_sparsity_info_for_pruning = 'planning',
             use_query_condition_cache = 1
) WHERE trimLeft(explain) LIKE 'Sparsity%' OR trimLeft(explain) LIKE 'x' OR trimLeft(explain) LIKE 'Granules: %';

DROP TABLE t_partial_cache;
