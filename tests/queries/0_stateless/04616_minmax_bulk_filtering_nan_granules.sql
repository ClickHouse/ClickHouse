-- Tags: no-parallel-replicas
-- no-parallel-replicas: per-query SETTINGS toggling skip-index evaluation paths
-- must take effect on the executing replica, and EXPLAIN indexes output is inspected.

-- The test runner randomizes `secondary_indices_enable_bulk_filtering` and
-- `use_skip_indexes_on_data_read`; pin both so the bulk path actually runs.
SET secondary_indices_enable_bulk_filtering = 1;
SET use_skip_indexes_on_data_read = 0;
SET use_minmax_index_bulk_filtering = 1;

-- NaN semantics of the bulk minmax path must mirror `KeyCondition::checkInHyperrectangle`:
--   * granule min is NaN => the whole granule is NaN => prune it (intersects = false);
--   * granule max is NaN => the granule has NaN rows => it is never fully contained
--     (contains = false), so `NOT` above the leaf must not skip it.

DROP TABLE IF EXISTS t_bulk_all_nan;
DROP TABLE IF EXISTS t_bulk_mixed_nan;

-- 1. All-NaN granules under a lower-bounded predicate. Every granule is [NaN, NaN],
-- and NaN satisfies no comparison, so the scalar path prunes all granules. The bulk
-- path used to keep them: the lower-bound NaN relaxation (`or isNaN(max)`) fired for
-- [NaN, NaN] granules too. Observe the selected granules to prove pruning happens.
CREATE TABLE t_bulk_all_nan
(
    f Float64,
    INDEX idx_f f TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 128;

INSERT INTO t_bulk_all_nan SELECT nan FROM numbers(2048);

SELECT 'all-NaN, lower bound, granules selected (bulk)';
SELECT trim(explain)
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bulk_all_nan WHERE f >= 5.0)
WHERE explain LIKE '%Granules:%';

SELECT 'all-NaN, lower bound, granules selected (scalar)';
SELECT trim(explain)
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bulk_all_nan WHERE f >= 5.0
      SETTINGS use_minmax_index_bulk_filtering = 0)
WHERE explain LIKE '%Granules:%';

SELECT 'all-NaN, lower bound, count parity',
    (SELECT count() FROM t_bulk_all_nan WHERE f >= 5.0
         SETTINGS use_minmax_index_bulk_filtering = 0) =
    (SELECT count() FROM t_bulk_all_nan WHERE f >= 5.0
         SETTINGS use_minmax_index_bulk_filtering = 1) AS eq;

SELECT 'all-NaN, strict lower bound, count parity',
    (SELECT count() FROM t_bulk_all_nan WHERE f > 5.0
         SETTINGS use_minmax_index_bulk_filtering = 0) =
    (SELECT count() FROM t_bulk_all_nan WHERE f > 5.0
         SETTINGS use_minmax_index_bulk_filtering = 1) AS eq;

-- 2. Mixed [finite_min, NaN] granules under NOT of a lower-bounded predicate.
-- The NaN rows satisfy `NOT (f >= x)` (NaN compares false to everything), so every
-- granule must be read. For a lower-bounded predicate `contains_upper` is constant
-- true, and without the max-NaN guard the bulk path reported contains = true
-- (can_be_false = false) for granules whose min reaches the bound, making NOT skip
-- them and lose their NaN rows: a wrong result, not just a missed optimization.
-- Every granule below gets a NaN in its first row, so every granule's max is NaN.
CREATE TABLE t_bulk_mixed_nan
(
    f Float64,
    INDEX idx_f f TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 128;

INSERT INTO t_bulk_mixed_nan
SELECT if(number % 128 = 0, nan, toFloat64(number))
FROM numbers(2048);

-- 2048 rows, 16 NaN. `f >= 1000` holds for finite values 1000..2047 minus the 8 NaN
-- positions in that range => 1040 rows; NOT keeps the other 1008 (992 finite + 16 NaN).
SELECT 'mixed NaN, NOT lower bound, absolute count (bulk)',
    (SELECT count() FROM t_bulk_mixed_nan WHERE NOT (f >= 1000.0)
         SETTINGS use_minmax_index_bulk_filtering = 1) AS cnt;

SELECT 'mixed NaN, NOT lower bound, absolute count (scalar)',
    (SELECT count() FROM t_bulk_mixed_nan WHERE NOT (f >= 1000.0)
         SETTINGS use_minmax_index_bulk_filtering = 0) AS cnt;

SELECT 'mixed NaN, NOT BETWEEN, count parity',
    (SELECT count() FROM t_bulk_mixed_nan WHERE NOT (f BETWEEN 1000.0 AND 1500.0)
         SETTINGS use_minmax_index_bulk_filtering = 0) =
    (SELECT count() FROM t_bulk_mixed_nan WHERE NOT (f BETWEEN 1000.0 AND 1500.0)
         SETTINGS use_minmax_index_bulk_filtering = 1) AS eq;

-- Mixed granules under a plain lower bound must still be kept (the relaxation the
-- min-NaN guard must not break): finite values may match.
SELECT 'mixed NaN, lower bound, absolute count (bulk)',
    (SELECT count() FROM t_bulk_mixed_nan WHERE f >= 1000.0
         SETTINGS use_minmax_index_bulk_filtering = 1) AS cnt;

DROP TABLE t_bulk_all_nan;
DROP TABLE t_bulk_mixed_nan;
