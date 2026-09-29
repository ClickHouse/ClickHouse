-- The streams after `query_plan_window_functions_hash_partitioning` hold whole partitions, also after the last window
-- with `query_plan_enable_multithreading_after_window_functions`, so the steps after it can process them independently.
-- Each query must return the same with sorting (the first line) and with hash partitioning (the second line).

SET max_threads = 8, max_block_size = 1024;
SET query_plan_reuse_storage_ordering_for_window_functions = 0;
SET query_plan_enable_multithreading_after_window_functions = 1;
SET allow_distinct_partitions_independently = 1;
SET max_rows_to_sort = 0, max_bytes_to_sort = 0;

SELECT 'DISTINCT', (SELECT count() FROM (SELECT DISTINCT k, sum(number) OVER (PARTITION BY k) FROM (SELECT number, number % 10 AS k FROM numbers_mt(1000000)))) SETTINGS query_plan_window_functions_hash_partitioning = 0;
SELECT 'DISTINCT', (SELECT count() FROM (SELECT DISTINCT k, sum(number) OVER (PARTITION BY k) FROM (SELECT number, number % 10 AS k FROM numbers_mt(1000000)))) SETTINGS query_plan_window_functions_hash_partitioning = 1;

SELECT 'LIMIT BY', (SELECT count() FROM (SELECT k, sum(number) OVER (PARTITION BY k) FROM (SELECT number, number % 10 AS k FROM numbers_mt(1000000)) LIMIT 1 BY k)) SETTINGS query_plan_window_functions_hash_partitioning = 0;
SELECT 'LIMIT BY', (SELECT count() FROM (SELECT k, sum(number) OVER (PARTITION BY k) FROM (SELECT number, number % 10 AS k FROM numbers_mt(1000000)) LIMIT 1 BY k)) SETTINGS query_plan_window_functions_hash_partitioning = 1;

SELECT 'GROUP BY', (SELECT count(), sum(c) FROM (SELECT k, count() AS c FROM (SELECT k, sum(number) OVER (PARTITION BY k) FROM (SELECT number, number % 10 AS k FROM numbers_mt(1000000))) GROUP BY k)) SETTINGS query_plan_window_functions_hash_partitioning = 0;
SELECT 'GROUP BY', (SELECT count(), sum(c) FROM (SELECT k, count() AS c FROM (SELECT k, sum(number) OVER (PARTITION BY k) FROM (SELECT number, number % 10 AS k FROM numbers_mt(1000000))) GROUP BY k)) SETTINGS query_plan_window_functions_hash_partitioning = 1;

SELECT 'another window', (SELECT sum(c) FROM (SELECT count() OVER (PARTITION BY k) AS c FROM (SELECT k, sum(number) OVER (PARTITION BY k) AS s FROM (SELECT number, number % 10 AS k FROM numbers_mt(1000000))))) SETTINGS query_plan_window_functions_hash_partitioning = 0;
SELECT 'another window', (SELECT sum(c) FROM (SELECT count() OVER (PARTITION BY k) AS c FROM (SELECT k, sum(number) OVER (PARTITION BY k) AS s FROM (SELECT number, number % 10 AS k FROM numbers_mt(1000000))))) SETTINGS query_plan_window_functions_hash_partitioning = 1;
