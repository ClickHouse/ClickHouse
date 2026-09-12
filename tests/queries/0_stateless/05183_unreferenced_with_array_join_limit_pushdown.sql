-- An unreferenced top-level `WITH` alias never reaches the read, so a call to `arrayJoin` inside it
-- must not cost the source limit: `max_rows_to_read` below is the observable - the query reads the
-- three rows it asks for and no whole block. A referenced alias, and a plain call in the `SELECT`
-- list, do multiply the rows and keep reading a block. Both spellings of the function are covered,
-- and both analyzers.

SELECT '-- no arrayJoin at all: pushdown';
SELECT number FROM numbers(1000000000000) LIMIT 3 SETTINGS max_rows_to_read = 10, max_block_size = 65505, max_threads = 1, enable_analyzer = 1;
SELECT number FROM numbers(1000000000000) LIMIT 3 SETTINGS max_rows_to_read = 10, max_block_size = 65505, max_threads = 1, enable_analyzer = 0;

SELECT '-- unreferenced WITH arrayJoin: still pushdown';
WITH arrayJoin([10, 20, 30]) AS unused SELECT number FROM numbers(1000000000000) LIMIT 3 SETTINGS max_rows_to_read = 10, max_block_size = 65505, max_threads = 1, enable_analyzer = 1;
WITH arrayJoin([10, 20, 30]) AS unused SELECT number FROM numbers(1000000000000) LIMIT 3 SETTINGS max_rows_to_read = 10, max_block_size = 65505, max_threads = 1, enable_analyzer = 0;

SELECT '-- the same through the `unnest` spelling';
WITH unnest([10, 20, 30]) AS unused SELECT number FROM numbers(1000000000000) LIMIT 3 SETTINGS max_rows_to_read = 10, max_block_size = 65505, max_threads = 1, enable_analyzer = 1;
WITH unnest([10, 20, 30]) AS unused SELECT number FROM numbers(1000000000000) LIMIT 3 SETTINGS max_rows_to_read = 10, max_block_size = 65505, max_threads = 1, enable_analyzer = 0;

SELECT '-- referenced WITH arrayJoin: no pushdown';
WITH arrayJoin([10, 20, 30]) AS used SELECT number FROM numbers(1000000000000) WHERE used > 0 LIMIT 3 SETTINGS max_rows_to_read = 10, max_block_size = 65505, max_threads = 1, enable_analyzer = 1; -- { serverError TOO_MANY_ROWS }
WITH arrayJoin([10, 20, 30]) AS used SELECT number FROM numbers(1000000000000) WHERE used > 0 LIMIT 3 SETTINGS max_rows_to_read = 10, max_block_size = 65505, max_threads = 1, enable_analyzer = 0; -- { serverError TOO_MANY_ROWS }

SELECT '-- arrayJoin in the SELECT list: no pushdown';
SELECT arrayJoin([number]) FROM numbers(1000000000000) LIMIT 3 SETTINGS max_rows_to_read = 10, max_block_size = 65505, max_threads = 1, enable_analyzer = 1; -- { serverError TOO_MANY_ROWS }
SELECT arrayJoin([number]) FROM numbers(1000000000000) LIMIT 3 SETTINGS max_rows_to_read = 10, max_block_size = 65505, max_threads = 1, enable_analyzer = 0; -- { serverError TOO_MANY_ROWS }
