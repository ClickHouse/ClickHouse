-- The whole file runs in one client invocation, i.e. one native/TCP session.
-- Recording happens at query start, so every query below (including TRUNCATE and the SELECTs themselves) is captured.
TRUNCATE TABLE system.session_query_ids;

SELECT 'marker 1' FORMAT Null;
SELECT 'marker 2' FORMAT Null;

-- Two markers plus this query: the current query is recorded at its start, so it counts itself.
SELECT count() FROM system.session_query_ids;

-- The history grows with every executed query.
SELECT count() FROM system.session_query_ids;

-- Sequence numbers are distinct and contiguous, i.e. monotonically increasing.
SELECT count() = uniqExact(sequence_number), max(sequence_number) - min(sequence_number) + 1 = count() FROM system.session_query_ids;

-- Rows are emitted in execution order.
SELECT groupArray(sequence_number) = arraySort(groupArray(sequence_number)) FROM system.session_query_ids;

-- The current query is visible in the table.
SELECT count() FROM system.session_query_ids WHERE query_id = queryID();

-- A failed query is recorded too, because recording happens at query start.
SELECT throwIf(1); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
SELECT count() FROM system.session_query_ids;

SYSTEM FLUSH LOGS query_log;

-- Every recorded id joins with system.query_log. The last two entries are excluded: the flush query
-- flushes the logs before logging its own start, and the current query has not finished yet.
SELECT count() FROM system.session_query_ids
WHERE sequence_number + 2 <= (SELECT max(sequence_number) FROM system.session_query_ids)
    AND query_id NOT IN (SELECT query_id FROM system.query_log WHERE current_database = currentDatabase() AND event_date >= yesterday());

-- The failed query's id joins to its exception entry in system.query_log.
SELECT count() FROM system.query_log
WHERE current_database = currentDatabase() AND event_date >= yesterday()
    AND type IN ('ExceptionBeforeStart', 'ExceptionWhileProcessing')
    AND query_id IN (SELECT query_id FROM system.session_query_ids);

-- Eviction: with a small history size only the newest entries remain and sequence numbers stay monotonic.
SET session_query_ids_history_size = 3;
SELECT 'evicted 1' FORMAT Null;
SELECT 'evicted 2' FORMAT Null;
SELECT 'evicted 3' FORMAT Null;
SELECT count(), countIf(query_id = queryID()), max(sequence_number) - min(sequence_number) + 1 = count() FROM system.session_query_ids;

-- Disabling recording keeps the already recorded entries visible, but new queries are not recorded:
-- neither the marker nor the reading query below appears (the SET itself is still recorded).
SET session_query_ids_history_size = 0;
SELECT 'not recorded' FORMAT Null;
SELECT count(), countIf(query_id = queryID()) FROM system.session_query_ids;
