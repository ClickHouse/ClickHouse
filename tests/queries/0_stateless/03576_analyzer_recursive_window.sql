SELECT 1
WINDOW w0 AS (ORDER BY min(1) OVER (w0))
SETTINGS allow_experimental_analyzer = 1; -- { serverError UNSUPPORTED_METHOD }

SELECT 1
WINDOW w0 AS (ORDER BY min(1) OVER (w0))
SETTINGS allow_experimental_analyzer = 0; -- { serverError ILLEGAL_AGGREGATION }
