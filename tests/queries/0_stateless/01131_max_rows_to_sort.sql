SET max_rows_to_sort = 100;
SELECT * FROM system.numbers ORDER BY number; -- { serverError TOO_MANY_ROWS_OR_BYTES }

SET sort_overflow_mode = 'break';
SET max_block_size = 1000;

set query_plan_remove_redundant_sorting=0; -- to keep sorting in the query below
SELECT count() >= 100 AND count() <= 1000 FROM (SELECT * FROM system.numbers ORDER BY number);
