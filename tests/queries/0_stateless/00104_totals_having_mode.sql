SET max_threads = 1;
SET max_block_size = 65536;
SET max_rows_to_group_by = 65535;
SET group_by_overflow_mode = 'any';

SET totals_mode = 'before_having';
SELECT number, count() FROM (SELECT * FROM system.numbers LIMIT 100000) GROUP BY number WITH TOTALS HAVING number % 3 = 0 ORDER BY number LIMIT 1;

SET totals_mode = 'after_having_inclusive';
SELECT number, count() FROM (SELECT * FROM system.numbers LIMIT 100000) GROUP BY number WITH TOTALS HAVING number % 3 = 0 ORDER BY number LIMIT 1;

SET totals_mode = 'after_having_exclusive';
SELECT number, count() FROM (SELECT * FROM system.numbers LIMIT 100000) GROUP BY number WITH TOTALS HAVING number % 3 = 0 ORDER BY number LIMIT 1;

SET totals_mode = 'after_having_auto';
SET totals_auto_threshold = 0.5;
SELECT number, count() FROM (SELECT * FROM system.numbers LIMIT 100000) GROUP BY number WITH TOTALS HAVING number % 3 = 0 ORDER BY number LIMIT 1;
