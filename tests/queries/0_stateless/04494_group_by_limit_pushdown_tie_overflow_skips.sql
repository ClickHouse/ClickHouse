-- Pattern 2 (`GROUP BY ... LIMIT`, no ORDER BY) must not freeze the heap on
-- tie overflow when rows were already skipped: a skipped key would re-enter
-- after the freeze as a fresh group missing its earlier rows, and the unsorted
-- LIMIT could return that incomplete aggregate.
--
-- Construction (single thread, deterministic input order, LIMIT 1):
--   row 0:           key (0.0, nan)  -- admitted, becomes the boundary
--   row 1:           key (1.0, 0.0)  -- worse than the boundary, skipped
--   rows 2..~1.05M:  keys (0.0, nan_i) with bit-distinct NaN payloads -- each
--                    compares equal to the boundary, so it can never be
--                    evicted; the tie set grows past the internal cap
--                    (capacity + 2^20) and raises `tie_overflow`
--   tail rows:       key (1.0, 0.0) again -- must stay skipped
SET enable_group_by_top_k_optimization = 1;
SET max_rows_to_group_by = 0;
SET optimize_trivial_group_by_limit_query = 0;
SET max_threads = 1;
SET max_block_size = 65536;

SELECT count() FROM
(
    SELECT k1, k2, count() AS cnt
    FROM
    (
        SELECT
            if(number = 1 OR number >= 1100000, 1.0, 0.0) AS k1,
            if(number = 1 OR number >= 1100000, 0.0, reinterpret(0x7FF0000000000001 + number, 'Float64')) AS k2
        FROM numbers(1100100)
    )
    GROUP BY k1, k2
    LIMIT 1
) SETTINGS log_comment = '04494_tie_overflow_skips';

SYSTEM FLUSH LOGS query_log;

-- The heap must have rejected rows (the construction is exercising the skip
-- path) and must NOT have frozen (a freeze here is the wrong-result bug).
SELECT
    sum(ProfileEvents['AggregationTopKRowsSkipped']) > 0 AS skipped,
    sum(ProfileEvents['AggregationTopKHeapsFrozen']) AS frozen
FROM system.query_log
WHERE current_database = currentDatabase()
    AND log_comment = '04494_tie_overflow_skips'
    AND type = 'QueryFinish'
    AND event_date >= yesterday();
