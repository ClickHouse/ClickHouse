-- Tags: no-parallel-replicas
-- no-parallel-replicas: the `read_rows` assertion below reads the initiator's own plan.

-- Restricting a filter to a projection's columns replaces a conjunct it cannot compute with true.
-- Weakening a conjunct is sound only where the `AND` is read with positive polarity: under `NOT`, as a
-- comparison operand or as a branch condition it makes the restricted filter stronger instead, and the
-- projection index then prunes granules holding rows that do match.

SET enable_analyzer = 1;
SET optimize_use_projections = 1, optimize_use_projection_filtering = 1;
SET use_indexes_refiner_in_read_pools = 0;  -- decides whether pruned ranges are dropped or skipped while reading, which moves read_rows
SET use_query_condition_cache = 0;          -- a cached granule set would leak between the read_rows arms below

DROP TABLE IF EXISTS t_projection_polarity;

CREATE TABLE t_projection_polarity (id UInt64, a UInt64, x UInt64, b UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

-- `b` is outside the projection, which is what makes a conjunct over it non-computable there.
ALTER TABLE t_projection_polarity ADD PROJECTION p (SELECT a, x, _part_offset ORDER BY a);

-- A single part, above `min_table_rows_to_use_projection_index`, so the projection is used as an index.
INSERT INTO t_projection_polarity SELECT number, number % 1000, intDiv(number, 1000), 7 FROM numbers(2000000)
SETTINGS max_insert_block_size = 2000000, min_insert_block_size_rows = 2000000;

-- Every row with a = 1 has b = 7, so each predicate below is true for all 2000 of them.
SELECT count() FROM t_projection_polarity WHERE a = 1 AND NOT (a = 1 AND b = 2);
SELECT count() FROM t_projection_polarity WHERE a = 1 AND (a = 1 AND b = 2) = 0;
SELECT count() FROM t_projection_polarity WHERE a = 1 AND multiIf(a = 1 AND b = 2, 0, 1);
-- `b` is read as a condition by one conjunct and as a value by another, through the same input node.
SELECT count() FROM t_projection_polarity WHERE a = 1 AND b AND b = 7;
-- A `NOT` under an `OR` is still not weakened.
SELECT count() FROM t_projection_polarity WHERE a = 1 AND (x = 5 OR NOT (a = 1 AND b = 2));
-- A conjunct whose type has no true value stays in place; a float conjunct is replaced with a float true.
SELECT count() FROM t_projection_polarity WHERE (a = 1 AND if(b = 1, NULL, NULL)) OR a = 2;
SELECT count() FROM t_projection_polarity WHERE a = 1 AND toFloat64(b);
-- `throwIf` runs only where `b = 1`, so `b = 1` must not be weakened under the `OR`.
SELECT count() FROM t_projection_polarity WHERE (a <= 2 AND b = 1 AND throwIf(a = 0) = 0) OR a = 2;
-- Under `force_enable` the `gcd` also runs only where `b = 1`, so `b = 1` must not be weakened under the `OR`.
SELECT count() FROM t_projection_polarity WHERE (a <= 2 AND b = 1 AND gcd(a, 0) = 0) OR a = 2
SETTINGS short_circuit_function_evaluation = 'force_enable';
-- `rand()` must not be drawn a second time over the projection: about half of the a = 1 rows are kept.
SELECT count() BETWEEN 2800 AND 3200 FROM t_projection_polarity WHERE (a = 1 AND b = 7 AND rand() % 2 = 0) OR a = 2;
-- The projection evaluates the filter on rows the main read skips (`id < 16384`), and `gcd` throws on `x = 5` there.
SELECT count() FROM t_projection_polarity WHERE id >= 16384 AND ((a = 1 AND b = 7 AND gcd(1, x - 5) = 1) OR (a = 2 AND b = 8));

-- A conjunct of an `AND` read with positive polarity must still be weakened, or the index stops pruning.
SELECT count() FROM t_projection_polarity WHERE a = 1 AND b = 7;
SELECT count() FROM t_projection_polarity WHERE a = 1 AND x = 5 AND b = 7
SETTINGS log_comment = '05218_pruning_alive';
SELECT count() FROM t_projection_polarity WHERE a = 1 AND x = 5 AND b = 7
SETTINGS log_comment = '05218_pruning_off', optimize_use_projection_filtering = 0;
-- Unmerged filter steps reach the projection as `and(and(x = 5, b = 7), a = 1)`, so only a descent into the
-- nested `AND` weakens `b = 7`; dropping that whole conjunct instead is still correct but stops pruning.
SELECT count() FROM (SELECT * FROM t_projection_polarity WHERE x = 5 AND b = 7) WHERE a = 1
SETTINGS log_comment = '05218_pruning_alive_nested', query_plan_merge_filters = 0;
-- `OR` is monotone too, so an `AND` under it must still be weakened.
SELECT count() FROM t_projection_polarity WHERE (a = 1 AND x = 5 AND b = 7) OR (a = 2 AND x = 6 AND b = 8)
SETTINGS log_comment = '05218_pruning_alive_or';
SELECT count() FROM t_projection_polarity WHERE a = 1 AND ((x = 5 AND b = 7) OR (x = 6 AND b = 8))
SETTINGS log_comment = '05218_pruning_alive_or_nested';
SELECT count() FROM t_projection_polarity WHERE a IN (1, 2) AND ((x = 5 AND b = 7) OR (x = 6 AND b = 8))
SETTINGS log_comment = '05218_pruning_alive_or_in';
-- A function in a conjunct that is replaced is never evaluated, so it does not stop the walk, also over projection columns.
SELECT count() FROM t_projection_polarity WHERE (a = 1 AND x = 5 AND toFloat64(b)) OR (a = 2 AND x = 6 AND toFloat64(a) + b > 0)
SETTINGS log_comment = '05218_pruning_alive_or_dropped';
-- A conjunct outside the weakened `OR` is evaluated without the walk too, so it does not stop the walk, also in another `OR`.
SELECT count() FROM t_projection_polarity WHERE toString(a) != '' AND ((a = 1 AND x = 5 AND b = 7) OR (a = 2 AND x = 6 AND b = 8))
SETTINGS log_comment = '05218_pruning_alive_or_outside';
SELECT count() FROM t_projection_polarity
WHERE (toString(a) != '' OR toString(x) = '6') AND ((a = 1 AND x = 5 AND b = 7) OR (a = 2 AND x = 6 AND b = 8))
SETTINGS log_comment = '05218_pruning_alive_or_outside_or';

SYSTEM FLUSH LOGS query_log;

-- The table pins a granule at 8192 rows and one carries the single matching row, so a live projection index
-- reads at most two; with the row-level filter off the whole table is read. The pair fails from both sides:
-- if the relaxation stops firing the first flips, if the fixture stops pruning the second does.
SELECT
    maxIf(read_rows, log_comment = '05218_pruning_alive') <= 2 * 8192 AS pruned,
    minIf(read_rows, log_comment = '05218_pruning_off') >= 2000000 AS not_pruned,
    maxIf(read_rows, log_comment = '05218_pruning_alive_nested') <= 2 * 8192 AS pruned_nested,
    maxIf(read_rows, log_comment = '05218_pruning_alive_or') <= 2 * 8192 AS pruned_or,
    maxIf(read_rows, log_comment = '05218_pruning_alive_or_nested') <= 2 * 8192 AS pruned_or_nested,
    maxIf(read_rows, log_comment = '05218_pruning_alive_or_in') <= 2 * 8192 AS pruned_or_in,
    maxIf(read_rows, log_comment = '05218_pruning_alive_or_dropped') <= 2 * 8192 AS pruned_or_dropped,
    maxIf(read_rows, log_comment = '05218_pruning_alive_or_outside') <= 2 * 8192 AS pruned_or_outside,
    maxIf(read_rows, log_comment = '05218_pruning_alive_or_outside_or') <= 2 * 8192 AS pruned_or_outside_or
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
  AND log_comment IN ('05218_pruning_alive', '05218_pruning_off', '05218_pruning_alive_nested',
                      '05218_pruning_alive_or', '05218_pruning_alive_or_nested', '05218_pruning_alive_or_in',
                      '05218_pruning_alive_or_dropped', '05218_pruning_alive_or_outside', '05218_pruning_alive_or_outside_or');

DROP TABLE t_projection_polarity;
