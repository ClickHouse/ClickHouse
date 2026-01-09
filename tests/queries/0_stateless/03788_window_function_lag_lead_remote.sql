SET enable_analyzer = 1;

SELECT lag(number) OVER (ORDER BY number)
FROM remote('127.0.0.1', numbers(5));

SELECT lead(number) OVER (ORDER BY number)
FROM remote('127.0.0.1', numbers(5));

SELECT lag(number) OVER (ORDER BY number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
FROM numbers(5); -- { serverError BAD_ARGUMENTS }

SELECT lead(number) OVER (ORDER BY number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
FROM numbers(5); -- { serverError BAD_ARGUMENTS }
