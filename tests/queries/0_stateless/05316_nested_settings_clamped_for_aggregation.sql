-- The aggregation of a query with a nested SETTINGS clause uses the settings in effect for that query,
-- after the settings constraints are applied, not the clause as written.

SELECT 'a nested max_threads reaches the aggregation';
SELECT max(toUInt64(n)) FROM
(
    EXPLAIN PIPELINE SELECT count() FROM (SELECT number FROM numbers_mt(100000) GROUP BY number SETTINGS max_threads = 23, max_threads_min_free_memory_per_thread = 0)
    SETTINGS max_threads = 2
)
ARRAY JOIN extractAll(explain, '\\d+') AS n;

SELECT 'a nested clause dropped in readonly mode does not reach the aggregation';
SELECT max(toUInt64(n)) <= 2 FROM
(
    EXPLAIN PIPELINE SELECT count() FROM (SELECT number FROM numbers_mt(100000) GROUP BY number SETTINGS max_threads = 23, max_threads_min_free_memory_per_thread = 0)
    SETTINGS max_threads = 2, readonly = 1
)
ARRAY JOIN extractAll(explain, '\\d+') AS n;

SELECT 'GROUP BY LIMIT stops at LIMIT keys';
SELECT count(), sum(c) > 0 FROM (SELECT toUInt64(number % 1000) AS k, count() AS c FROM numbers(10000) GROUP BY k LIMIT 10 SETTINGS max_rows_to_group_by = 100);

SELECT 'a nested group_by_overflow_mode equal to the inherited value is still respected';
SELECT count(), sum(c) > 0 FROM (SELECT toUInt64(number % 1000) AS k, count() AS c FROM numbers(10000) GROUP BY k LIMIT 10 SETTINGS max_rows_to_group_by = 100, group_by_overflow_mode = 'throw'); -- { serverError TOO_MANY_ROWS }
