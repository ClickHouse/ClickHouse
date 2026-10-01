-- Aggregation in order does not produce the aggregation overflow row, so a query that needs the
-- overflow row (`WITH TOTALS` together with `max_rows_to_group_by` and
-- `group_by_overflow_mode = 'any'`) must not be aggregated in order. `force_aggregation_in_order` used
-- to build it anyway, and `TotalsHavingTransform` then raised
-- `Chunk should have AggregatedChunkInfo in TotalsHavingTransform.`

DROP TABLE IF EXISTS t_in_order_overflow_row;
CREATE TABLE t_in_order_overflow_row (t UInt64, v UInt64) ENGINE = MergeTree ORDER BY t;
-- One part per INSERT, kept apart so that the table is read with several streams. A single stream
-- ends in `FinalizeAggregatedTransform`, which back-fills a missing chunk info and hides the problem.
SYSTEM STOP MERGES t_in_order_overflow_row;
INSERT INTO t_in_order_overflow_row VALUES (6, 1);
INSERT INTO t_in_order_overflow_row VALUES (5, 2);
INSERT INTO t_in_order_overflow_row VALUES (6, 3);
INSERT INTO t_in_order_overflow_row VALUES (5, 4);

SET force_aggregation_in_order = 1;
-- Only the forced path may introduce the in-order transform, so keep the optimizer out of it
-- (the setting is randomized in CI).
SET optimize_aggregation_in_order = 0;
-- Several streams are needed, as above, and the effective count is randomized in CI down to 1 and
-- can be clamped further under memory pressure (as in 02493, 01556).
SET max_threads = 4;
SET max_threads_min_free_memory_per_thread = 0;
SET totals_mode = 'after_having_auto', max_rows_to_group_by = 1000, group_by_overflow_mode = 'any';

-- The filter has to match no rows while still reading every part: once parts are skipped the read
-- collapses to a single stream and the case above applies.
SELECT t FROM t_in_order_overflow_row WHERE sipHash64(v) = 0 GROUP BY t WITH TOTALS ORDER BY t;
SELECT t, count() FROM t_in_order_overflow_row WHERE sipHash64(v) = 0 GROUP BY t WITH ROLLUP WITH TOTALS ORDER BY t;

-- The feature itself keeps working.
SELECT t FROM t_in_order_overflow_row GROUP BY t WITH TOTALS ORDER BY t;

SELECT 'aggregation in order refused when the overflow row is needed', count() = 0
FROM (EXPLAIN PIPELINE SELECT t FROM t_in_order_overflow_row GROUP BY t WITH TOTALS)
WHERE explain ILIKE '%AggregatingInOrderTransform%';

-- The opposite direction, so that a guard keyed on `force_aggregation_in_order` alone would fail:
-- without the overflow row the forced path is unchanged.
SELECT 'aggregation in order still forced without the overflow row', count() > 0
FROM (EXPLAIN PIPELINE SELECT t FROM t_in_order_overflow_row GROUP BY t WITH TOTALS SETTINGS max_rows_to_group_by = 0)
WHERE explain ILIKE '%AggregatingInOrderTransform%';

-- A scalar subquery of a mutation is planned by the old query analysis, which had the same hole.
DROP TABLE IF EXISTS t_in_order_overflow_row_mut;
CREATE TABLE t_in_order_overflow_row_mut (c0 Int32, c1 Nullable(Int32)) ENGINE = MergeTree ORDER BY c0;
INSERT INTO t_in_order_overflow_row_mut VALUES (1, 2), (3, 5);
ALTER TABLE t_in_order_overflow_row_mut
UPDATE c1 = (SELECT t FROM t_in_order_overflow_row WHERE sipHash64(v) = 0 GROUP BY t WITH TOTALS ORDER BY t LIMIT 1)
WHERE TRUE
SETTINGS mutations_execute_subqueries_on_initiator = 1, mutations_sync = 2;

SELECT 'mutation scalar subquery', c0, c1 FROM t_in_order_overflow_row_mut ORDER BY c0;

DROP TABLE t_in_order_overflow_row_mut;
DROP TABLE t_in_order_overflow_row;
