SET max_rows_to_read = 1e11;

SELECT * FROM system.numbers LIMIT 1e12 FORMAT Null; -- { serverError TOO_MANY_ROWS }
SELECT * FROM system.numbers_mt LIMIT 1e12 FORMAT Null; -- { serverError TOO_MANY_ROWS }

SELECT * FROM system.zeros LIMIT 1e12 FORMAT Null; -- { serverError TOO_MANY_ROWS }
SELECT * FROM system.zeros_mt LIMIT 1e12 FORMAT Null; -- { serverError TOO_MANY_ROWS }

SELECT * FROM generateRandom() LIMIT 1e12 FORMAT Null; -- { serverError TOO_MANY_ROWS }
