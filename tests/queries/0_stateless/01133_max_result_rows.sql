SET max_block_size = 10;
SET max_result_rows = 20;
SET result_overflow_mode = 'throw';

SELECT DISTINCT intDiv(number, 10) FROM numbers(300); -- { serverError 396 }
SELECT DISTINCT intDiv(number, 10) FROM numbers(190);
SELECT DISTINCT intDiv(number, 10) FROM numbers(200);
SELECT DISTINCT intDiv(number, 10) FROM numbers(210); -- { serverError 396 }

SET result_overflow_mode = 'break';

SELECT DISTINCT intDiv(number, 10) FROM numbers(300);
SELECT DISTINCT intDiv(number, 10) FROM numbers(190);
SELECT DISTINCT intDiv(number, 10) FROM numbers(200);
SELECT DISTINCT intDiv(number, 10) FROM numbers(210);
