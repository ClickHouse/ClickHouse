SET max_bytes_ratio_before_external_distinct = 0;
-- The spill threshold is compared with the memory tracked by the query; flush the thread-local
-- untracked buffers on every allocation so that the tiny threshold triggers deterministically.
SET max_untracked_memory = 0;

-- LIMIT is reached before any spill: the query stops early and never spills.
SELECT count() FROM (SELECT DISTINCT number % 100 AS k FROM numbers(100000) LIMIT 10) SETTINGS max_bytes_before_external_distinct = '1G';

-- LIMIT is applied to the merged stream after the spill (a new distinct key arrives only every 100 rows,
-- so the spill happens long before the limit is reached).
SELECT count() FROM (SELECT DISTINCT bitXor(intDiv(number, 100), 5) AS k FROM numbers(3000) LIMIT 10) SETTINGS max_bytes_before_external_distinct = 1, max_block_size = 100;

-- max_rows_in_distinct with the 'throw' overflow mode, before and after the spill.
SELECT count() FROM (SELECT DISTINCT number % 10000 AS k FROM numbers(20000)) SETTINGS max_rows_in_distinct = 100, distinct_overflow_mode = 'throw', max_bytes_before_external_distinct = 0; -- { serverError SET_SIZE_LIMIT_EXCEEDED }
SELECT count() FROM (SELECT DISTINCT number % 10000 AS k FROM numbers(20000)) SETTINGS max_rows_in_distinct = 100, distinct_overflow_mode = 'throw', max_bytes_before_external_distinct = 1, max_block_size = 50; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

-- max_bytes_in_distinct restricts the in-memory state of the set; a spilled DISTINCT is memory-bounded
-- by the spilling itself, so the limit must not fire on the volume of the produced data. The UInt64 key
-- gives a dynamically growing set that stays tiny before the spill (a fixed-table key type like UInt16
-- would exceed such a small limit upfront), while the distinct data is way over the limit.
SELECT count() FROM (SELECT DISTINCT bitXor(intDiv(number, 2), 5) AS k FROM numbers(20000)) SETTINGS max_bytes_in_distinct = '16K', distinct_overflow_mode = 'throw', max_bytes_before_external_distinct = 1, max_block_size = 50;

-- The 'break' overflow mode truncates the result without an error. In-memory the check uses >=, so the
-- chunk that makes the set reach the limit is dropped: with 50-row chunks the result is exactly 50 rows.
-- After the spill the limit is checked against the emitted rows with chunk granularity.
SELECT count() FROM (SELECT DISTINCT number % 10000 AS k FROM numbers(20000)) SETTINGS max_rows_in_distinct = 100, distinct_overflow_mode = 'break', max_bytes_before_external_distinct = 0, max_block_size = 50;
SELECT count() BETWEEN 50 AND 200 FROM (SELECT DISTINCT number % 10000 AS k FROM numbers(20000)) SETTINGS max_rows_in_distinct = 100, distinct_overflow_mode = 'break', max_bytes_before_external_distinct = 1, max_block_size = 50;
