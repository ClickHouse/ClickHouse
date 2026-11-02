-- Offsets at the top of the accepted range point far outside any frame, so the functions return the default.
SELECT number, lead(number, 9223372036854775807) OVER w, lag(number, 9223372036854775807) OVER w, lead(number, 9223372036854775807, 42) OVER w
FROM numbers(4) WINDOW w AS (PARTITION BY number % 2 ORDER BY number) ORDER BY number;

SELECT number, leadInFrame(number, 9223372036854775807) OVER w, lagInFrame(number, 9223372036854775807) OVER w, nth_value(number, 9223372036854775807) OVER w
FROM numbers(4) WINDOW w AS (PARTITION BY number % 2 ORDER BY number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) ORDER BY number;

SELECT number, nth_value(number, 9223372036854775806) OVER (ORDER BY number ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM numbers(4);
