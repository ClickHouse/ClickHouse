SET max_block_size = 10;
SET max_rows_to_read = 20;
SET read_overflow_mode = 'throw';

SELECT count() FROM numbers(30); -- { serverError 158 }
SELECT count() FROM numbers(19);
SELECT count() FROM numbers(20);
SELECT count() FROM numbers(21); -- { serverError 158 }

-- check early exception if the estimated number of rows is high
SELECT * FROM numbers(30); -- { serverError 158 }

SET read_overflow_mode = 'break';

SELECT count() FROM numbers(19);
SELECT count() FROM numbers(20);
SELECT count() FROM numbers(21);
SELECT count() FROM numbers(29);
SELECT count() FROM numbers(30);
SELECT count() FROM numbers(31);

-- check that partial result is returned even if the estimated number of rows is high
SELECT * FROM numbers(30);

-- the same for uneven block sizes
-- NOTE: currently it outputs less amount of data; it will be better to output the latest block also
SET max_block_size = 11;
SELECT * FROM numbers(30);
SET max_block_size = 9;
SELECT * FROM numbers(30);
