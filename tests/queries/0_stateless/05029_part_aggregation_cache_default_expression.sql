-- Tags: no-parallel

-- Regression test for the part aggregation cache and read-time default expressions.
--
-- A column that a part does not physically store is materialized while reading by
-- `MergeTreeSequentialSource` (`fillMissingColumns` / `evaluateMissingDefaults`) from the column's
-- `DEFAULT` expression in the *current* table metadata. That expression is not part of the cache
-- key, and changing it with `ALTER TABLE ... MODIFY COLUMN x ... DEFAULT ...` is a metadata-only
-- operation: the part keeps its name and the column keeps its type, so the key stays the same and a
-- per-part state aggregated from the previous default could be reused. The optimizer therefore
-- fails closed when a selected part does not store every column it would read.

-- The functional-test config (`tests/config/users.d/limits.yaml`) sets `max_rows_to_group_by = 10G`
-- and read limits (`max_rows_to_read`, `max_bytes_to_read`, `*_leaf`), on which the optimization
-- fails closed; pin them all to 0 so the cache is actually exercised (as in
-- `04033_part_aggregation_cache`).
SET allow_experimental_analyzer = 0, allow_experimental_part_aggregation_cache = 1, optimize_aggregation_in_order = 0, enable_memory_bound_merging_of_aggregation_results = 0, max_rows_to_group_by = 0, max_rows_to_read = 0, max_bytes_to_read = 0, max_rows_to_read_leaf = 0, max_bytes_to_read_leaf = 0;

SYSTEM DROP PART AGGREGATION CACHE;

DROP TABLE IF EXISTS t_part_agg_cache_default;

CREATE TABLE t_part_agg_cache_default (k UInt32) ENGINE = MergeTree ORDER BY k;

-- Keep the part layout stable so a cached entry would stay addressable by part name.
SYSTEM STOP MERGES t_part_agg_cache_default;

INSERT INTO t_part_agg_cache_default SELECT number % 2 FROM numbers(4);

-- Exactly one part, which does not store `x` below.
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_part_agg_cache_default' AND active;

-- Metadata-only `ALTER`: the existing part is not rewritten.
ALTER TABLE t_part_agg_cache_default ADD COLUMN x UInt32 DEFAULT 1;

-- Two rows per key, each contributing the default 1.
SELECT k, sum(x) FROM t_part_agg_cache_default GROUP BY k ORDER BY k;

-- Nothing was cached: `x` is not stored by the part, so the optimization failed closed.
SELECT count() FROM system.part_aggregation_cache;

-- Also metadata-only: same part name, same column type, different value for the old part.
ALTER TABLE t_part_agg_cache_default MODIFY COLUMN x UInt32 DEFAULT 2;

-- Must be 4 per key now. Reusing a state cached under the previous default would return 2.
SELECT k, sum(x) FROM t_part_agg_cache_default GROUP BY k ORDER BY k;
SELECT count() FROM system.part_aggregation_cache;

-- Control: a column the part does store is cached as usual, so the guard above is not vacuous.
SELECT k, sum(k) FROM t_part_agg_cache_default GROUP BY k ORDER BY k;
SELECT count() FROM system.part_aggregation_cache;

DROP TABLE t_part_agg_cache_default;
SYSTEM DROP PART AGGREGATION CACHE;
