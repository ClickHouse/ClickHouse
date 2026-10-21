-- `max_rows_in_join` counts the distinct keys of the hash table, as `HashJoin` counted them, not the
-- right rows. A build of 20000 rows over 100 keys stays under a limit of 1000 keys, and a build of 5000
-- keys exceeds it, whether one thread fills the table or several threads fill it in parallel.
SET enable_analyzer = 1;
SET join_algorithm = 'hash';
SET query_plan_join_swap_table = 0;
SET max_bytes_before_external_join = 0;
SET max_bytes_ratio_before_external_join = 0;
SET max_bytes_in_join = 0;
SET max_rows_in_join = 1000;
SET join_overflow_mode = 'throw';

SELECT 'one fill thread';
SELECT count() FROM numbers(100) AS l INNER JOIN (SELECT number % 100 AS k FROM numbers(20000)) AS r ON l.number = r.k SETTINGS max_threads = 1;
SELECT count() FROM numbers(100) AS l INNER JOIN (SELECT number AS k FROM numbers(5000)) AS r ON l.number = r.k SETTINGS max_threads = 1; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

SELECT 'parallel fill';
SELECT count() FROM numbers(100) AS l INNER JOIN (SELECT number % 100 AS k FROM numbers_mt(20000)) AS r ON l.number = r.k SETTINGS max_threads = 4, max_block_size = 4096, parallel_hash_join_threshold = 0;
SELECT count() FROM numbers(100) AS l INNER JOIN (SELECT number AS k FROM numbers_mt(5000)) AS r ON l.number = r.k SETTINGS max_threads = 4, max_block_size = 4096, parallel_hash_join_threshold = 0; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

SELECT 'parallel fill, join_overflow_mode = break';
SELECT count() FROM numbers(100) AS l INNER JOIN (SELECT number % 100 AS k FROM numbers_mt(20000)) AS r ON l.number = r.k SETTINGS max_threads = 4, max_block_size = 4096, parallel_hash_join_threshold = 0, join_overflow_mode = 'break';
