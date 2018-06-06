DROP TABLE IF EXISTS test.numbers_100k_log;
CREATE TABLE test.numbers_100k_log ENGINE = Log AS SELECT * FROM system.numbers LIMIT 100000;

SELECT count() = 200000 FROM remote('127.0.0.{2,3}', test.numbers_100k_log) GROUP BY number WITH TOTALS ORDER BY number LIMIT 10;

SET distributed_aggregation_memory_efficient = 1,
    group_by_two_level_threshold = 1000,
    group_by_overflow_mode = 'any',
    max_rows_to_group_by = 1000,
    totals_mode = 'after_having_auto';

SELECT count() = 200000 FROM remote('127.0.0.{2,3}', test.numbers_100k_log) GROUP BY number WITH TOTALS ORDER BY number LIMIT 10;

DROP TABLE test.numbers_100k_log;
