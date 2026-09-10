SET max_threads = 1;
SET max_block_size = 2;
SET max_bytes_ratio_before_external_distinct = 0;
SET max_untracked_memory = 0;
SET optimize_distinct_in_order = 0;
SET allow_preliminary_distinct_abandoning = 0;

-- The second distinct output chunk reaches the `LIMIT` hint and exceeds the row limit together.
-- The row limit must throw in both the hashing and merged-output phases.
SELECT count() FROM (SELECT DISTINCT number FROM numbers(8) LIMIT 4) SETTINGS max_rows_in_distinct = 3, max_bytes_before_external_distinct = 0; -- { serverError SET_SIZE_LIMIT_EXCEEDED }
SELECT count() FROM (SELECT DISTINCT number FROM numbers(8) LIMIT 4) SETTINGS max_rows_in_distinct = 3, max_bytes_before_external_distinct = 1; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

-- `BREAK` returns the whole crossing chunk, allowing the outer `LIMIT` to retain four rows.
SELECT count() FROM (SELECT DISTINCT number FROM numbers(8) LIMIT 4) SETTINGS max_rows_in_distinct = 3, distinct_overflow_mode = 'break', max_bytes_before_external_distinct = 0;
SELECT count() FROM (SELECT DISTINCT number FROM numbers(8) LIMIT 4) SETTINGS max_rows_in_distinct = 3, distinct_overflow_mode = 'break', max_bytes_before_external_distinct = 1;

-- Reaching the row limit exactly is permitted in `THROW` mode.
SELECT count() FROM (SELECT DISTINCT number FROM numbers(8) LIMIT 4) SETTINGS max_rows_in_distinct = 4, max_bytes_before_external_distinct = 0;
SELECT count() FROM (SELECT DISTINCT number FROM numbers(8) LIMIT 4) SETTINGS max_rows_in_distinct = 4, max_bytes_before_external_distinct = 1;

-- A smaller hint can finish hashing before either the row limit or the spill transition.
SELECT count() FROM (SELECT DISTINCT number FROM numbers(8) LIMIT 2) SETTINGS max_rows_in_distinct = 3, max_bytes_before_external_distinct = 0;
SELECT count() FROM (SELECT DISTINCT number FROM numbers(8) LIMIT 2) SETTINGS max_rows_in_distinct = 3, max_bytes_before_external_distinct = 1;
