-- The value and the default of lag and lead are brought to their common type, as in PostgreSQL. With a
-- narrower default the result equals the query that writes the default in the value type.
SELECT number, lag(number, 1, 7) OVER (ORDER BY number) FROM numbers(3);
SELECT number, lag(number, 1, toUInt64(7)) OVER (ORDER BY number) FROM numbers(3);
SELECT toFloat64(number) AS x, lead(x, 1, 1) OVER (ORDER BY x) FROM numbers(3);
SELECT number, lagInFrame(number, 1, 7) OVER (ORDER BY number ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM numbers(3);
SELECT number, leadInFrame(number, 1, 7) OVER (ORDER BY number ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) FROM numbers(3);

-- A wider default widens the result; a default with no common type is rejected.
SELECT lag(toUInt8(number), 1, 300) OVER (ORDER BY number) AS v, toTypeName(v) FROM numbers(3);
SELECT lag(number, 1, 'x') OVER (ORDER BY number) FROM numbers(3); -- { serverError NO_COMMON_TYPE }

-- A grouping key that becomes Nullable under group_by_use_nulls still takes a default of the plain type.
WITH 'x' AS v
SELECT lag(v, 1, '') OVER (ORDER BY v)
GROUP BY ROLLUP(v)
ORDER BY 1 NULLS FIRST
SETTINGS group_by_use_nulls = 1;
