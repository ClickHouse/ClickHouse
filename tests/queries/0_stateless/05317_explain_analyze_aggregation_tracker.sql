-- Track aggregation memory under the same group during pipeline construction and execution.
SELECT 'single_thread', countIf(match(explain, 'Hash table: memory [1-9]')) = 1
FROM (EXPLAIN ANALYZE SELECT number % 1000 AS k, uniqExact(number) FROM numbers_mt(100000) GROUP BY k
    SETTINGS max_threads = 1);

SELECT 'parallel', countIf(match(explain, 'Hash table: memory [1-9]')) = 1
FROM (EXPLAIN ANALYZE SELECT number % 1000 AS k, uniqExact(number) FROM numbers_mt(100000) GROUP BY k
    SETTINGS max_threads = 4);
