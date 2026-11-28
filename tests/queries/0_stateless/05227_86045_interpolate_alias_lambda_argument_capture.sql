-- The synthetic column of an INTERPOLATE expression lives in the same per-scope map as lambda arguments.
-- An alias written outside of a lambda must bind to it, not to a lambda argument of the same name.
-- https://github.com/ClickHouse/ClickHouse/issues/86045

-- `value` inside `d` is the interpolated column (10), so the fill row is (10 + 1) + (1 + 11) = 23.
SELECT number, toUInt64(10) AS value
FROM numbers(1)
ORDER BY number WITH FILL FROM 0 TO 2
INTERPOLATE (value AS (value + 1 AS d) + arrayMap(value -> value + d, [1])[1]);

-- Same expression with a lambda argument that shadows nothing.
SELECT number, toUInt64(10) AS value
FROM numbers(1)
ORDER BY number WITH FILL FROM 0 TO 2
INTERPOLATE (value AS (value + 1 AS d) + arrayMap(x -> x + d, [1])[1]);

-- The lambda argument itself still shadows the interpolated column inside the lambda body.
SELECT number, toUInt64(10) AS value
FROM numbers(1)
ORDER BY number WITH FILL FROM 0 TO 2
INTERPOLATE (value AS arrayMap(value -> value + 1, [1])[1]);

-- An alias defined in the lambda body keeps referring to the lambda argument.
SELECT number, toUInt64(10) AS value
FROM numbers(1)
ORDER BY number WITH FILL FROM 0 TO 2
INTERPOLATE (value AS arrayMap(value -> (value + 1 AS d) + d, [1])[1]);
