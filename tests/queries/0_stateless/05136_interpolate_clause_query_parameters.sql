-- A query parameter that is the whole INTERPOLATE expression must be substituted, not only one nested
-- inside a larger expression.

SET param_p = 7;

SELECT number AS bucket, toInt64(10) AS value
FROM numbers(3) WHERE bucket != 1
ORDER BY bucket WITH FILL
INTERPOLATE (value AS {p:Int64});

SELECT number AS bucket, toInt64(10) AS value
FROM numbers(3) WHERE bucket != 1
ORDER BY bucket WITH FILL
INTERPOLATE (value AS {unset_p:Int64}); -- { serverError UNKNOWN_QUERY_PARAMETER }
