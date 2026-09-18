-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/103103
-- A NULL-valued atom (e.g. toInt64OrNull('x')) reaching the set skip index was bit-wrapped by
-- __bitWrapperFunc and propagated NULL instead of the UNKNOWN mask, so the granule check wrongly
-- pruned matching granules and the query returned fewer rows than with skip indexes disabled.

DROP TABLE IF EXISTS t_null_const_set_index;

CREATE TABLE t_null_const_set_index (v Int64, lc LowCardinality(String), INDEX i lc TYPE set(64) GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 32;

INSERT INTO t_null_const_set_index SELECT number, toString(number % 3) FROM numbers(100);

-- Disable the query condition cache so every query below is analyzed by the index, not served
-- from a cached verdict, and both granule-filtering paths are exercised deterministically:
-- secondary_indices_enable_bulk_filtering = 1 -> getPossibleGranules, 0 -> mayBeTrueOnGranule.
SET use_query_condition_cache = 0;

-- All of the following match 50 rows via the second disjunct and must not be pruned.
-- Before the fix they returned 0 with the index. Run each under both granule paths.
SELECT count() FROM t_null_const_set_index WHERE (v = 1 AND toInt64OrNull('x')) OR v < 50 SETTINGS secondary_indices_enable_bulk_filtering = 1;
SELECT count() FROM t_null_const_set_index WHERE (v = 1 AND toInt64OrNull('x')) OR v < 50 SETTINGS secondary_indices_enable_bulk_filtering = 0;
SELECT count() FROM t_null_const_set_index WHERE (v = 1 AND nullIf(1, 1)) OR v < 50 SETTINGS secondary_indices_enable_bulk_filtering = 1;
SELECT count() FROM t_null_const_set_index WHERE (v = 1 AND nullIf(1, 1)) OR v < 50 SETTINGS secondary_indices_enable_bulk_filtering = 0;
SELECT count() FROM t_null_const_set_index WHERE (v = 1 AND CAST(NULL AS Nullable(Int64))) OR v < 50;
SELECT count() FROM t_null_const_set_index WHERE (v = 1 AND toUInt8OrNull('x')) OR v < 50;
-- The NULL value may be hidden behind a function that suppresses constant folding, so the atom
-- is not a folded ColumnConst; identity() and materialize() must be handled the same way.
SELECT count() FROM t_null_const_set_index WHERE (v = 1 AND identity(toInt64OrNull('x'))) OR v < 50;
SELECT count() FROM t_null_const_set_index WHERE (v = 1 AND materialize(toInt64OrNull('x'))) OR v < 50;
-- NULL value next to a predicate on the indexed column itself, and under NOT.
SELECT count() FROM t_null_const_set_index WHERE (lc = '1' AND toInt64OrNull('x')) OR v < 50;
SELECT count() FROM t_null_const_set_index WHERE NOT ((lc = '1' AND toInt64OrNull('x')) OR v >= 50);
-- Non-integer typed NULL and bare NULL route to UNKNOWN already; must stay 50.
SELECT count() FROM t_null_const_set_index WHERE (v = 1 AND CAST(NULL AS Nullable(Float64))) OR v < 50;
SELECT count() FROM t_null_const_set_index WHERE (v = 1 AND NULL) OR v < 50;

-- The NULL atom must not disable pruning: the index still prunes a granule it can legitimately
-- exclude. `lc = '999'` matches nothing, so every granule is skipped (`Granules: 0/<total>`,
-- asserted format-agnostically since the total depends on the randomized part type).
SELECT count() > 0 AS prunes_nonmatching_granules
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_null_const_set_index WHERE lc = '999')
WHERE explain ILIKE '%Granules: 0/%';

-- With the NULL atom present, a matching disjunct must still keep its granules: the index is
-- analyzed (the UNKNOWN atom does not disable it) and does not prune to zero.
SELECT count() = 0 AS null_atom_keeps_matching_granules
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_null_const_set_index WHERE (lc = '1' AND toInt64OrNull('x')) OR lc = '2')
WHERE explain ILIKE '%Granules: 0/%';

DROP TABLE t_null_const_set_index;
