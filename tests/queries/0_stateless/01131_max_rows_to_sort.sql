SET max_rows_to_sort = 100;
SELECT * FROM system.numbers ORDER BY number; -- { serverError 396 }

SET sort_overflow_mode = 'break';
SET max_block_size = 1000;

SELECT count() >= 100 AND count() <= 1000 FROM (SELECT * FROM system.numbers ORDER BY number);
