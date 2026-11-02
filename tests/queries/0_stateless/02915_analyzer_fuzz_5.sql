set enable_analyzer=1;
SET max_block_size = 1000;
SET max_threads = 4;
SET max_rows_to_group_by = 3000, group_by_overflow_mode = 'any';
SELECT 'limit w/ GROUP BY', count(NULL), number FROM remote('127.{1,2}', view(SELECT intDiv(number, 2147483647)
 AS number FROM numbers(10))) GROUP BY number WITH ROLLUP ORDER BY count() ASC, number DESC NULLS LAST SETTINGS limit = 2;
