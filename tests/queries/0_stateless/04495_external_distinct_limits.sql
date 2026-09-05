SET max_bytes_ratio_before_external_distinct = 0;
-- The spill threshold is compared with the memory tracked by the query; flush the thread-local
-- untracked buffers on every allocation so that the tiny threshold triggers deterministically.
SET max_untracked_memory = 0;

-- LIMIT is reached before any spill: the query stops early and never spills, so it reads only a part of
-- the source (checked against the query log below).
SELECT count() FROM (SELECT DISTINCT number % 100 AS k FROM numbers(100000) LIMIT 10) SETTINGS max_bytes_before_external_distinct = '1G', max_block_size = 1000, log_comment = '04495_external_distinct_limits/limit_before_spill';

-- LIMIT is applied to the merged stream after the spill (a new distinct key arrives only every 100 rows,
-- so the spill happens long before the limit is reached). After the spill nothing can be emitted until the
-- input is exhausted, so the query reads the whole source (checked against the query log below).
SELECT count() FROM (SELECT DISTINCT bitXor(intDiv(number, 100), 5) AS k FROM numbers(3000) LIMIT 10) SETTINGS max_bytes_before_external_distinct = 1, max_block_size = 100, log_comment = '04495_external_distinct_limits/limit_after_spill';

-- max_rows_in_distinct with the 'throw' overflow mode, before and after the spill.
SELECT count() FROM (SELECT DISTINCT number % 10000 AS k FROM numbers(20000)) SETTINGS max_rows_in_distinct = 100, distinct_overflow_mode = 'throw', max_bytes_before_external_distinct = 0; -- { serverError SET_SIZE_LIMIT_EXCEEDED }
SELECT count() FROM (SELECT DISTINCT number % 10000 AS k FROM numbers(20000)) SETTINGS max_rows_in_distinct = 100, distinct_overflow_mode = 'throw', max_bytes_before_external_distinct = 1, max_block_size = 50; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

-- max_bytes_in_distinct restricts the in-memory state of the set; a spilled DISTINCT is memory-bounded
-- by the spilling itself, so the limit must not fire on the volume of the produced data. The UInt64 key
-- gives a dynamically growing set that stays tiny before the spill (a fixed-table key type like UInt16
-- would exceed such a small limit upfront), while the distinct data is way over the limit.
SELECT count() FROM (SELECT DISTINCT bitXor(intDiv(number, 2), 5) AS k FROM numbers(20000)) SETTINGS max_bytes_in_distinct = '16K', distinct_overflow_mode = 'throw', max_bytes_before_external_distinct = 1, max_block_size = 50;

-- The 'break' overflow mode returns the partial result without an error: the new rows of the chunk
-- that reaches the limit (the check uses >=) are still returned, then reading stops. With 50-row
-- chunks the limit of 100 is reached by the second chunk, so the in-memory result is exactly 100 rows.
-- After the spill the limit is checked against the emitted rows with chunk granularity.
SELECT count() FROM (SELECT DISTINCT number % 10000 AS k FROM numbers(20000)) SETTINGS max_rows_in_distinct = 100, distinct_overflow_mode = 'break', max_bytes_before_external_distinct = 0, max_block_size = 50;
SELECT count() BETWEEN 50 AND 200 FROM (SELECT DISTINCT number % 10000 AS k FROM numbers(20000)) SETTINGS max_rows_in_distinct = 100, distinct_overflow_mode = 'break', max_bytes_before_external_distinct = 1, max_block_size = 50;

-- The rows read from the source by the two LIMIT queries above: a part of it when the limit stops the
-- query before any spill, all of it once a spill happened.
SYSTEM FLUSH LOGS query_log;
SELECT anyIf(read_rows < 100000, log_comment LIKE '%/limit_before_spill'), anyIf(read_rows, log_comment LIKE '%/limit_after_spill')
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
    AND current_database = currentDatabase() AND log_comment LIKE '04495_external_distinct_limits/%';
