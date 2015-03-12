SET max_rows_to_read = 1000000;
SET read_overflow_mode = 'break';
SELECT concat(toString(number % 256 AS n), '') AS s, n, max(s) FROM system.numbers_mt GROUP BY s, n, n, n, n, n, n, n, n, n ORDER BY s, n;
