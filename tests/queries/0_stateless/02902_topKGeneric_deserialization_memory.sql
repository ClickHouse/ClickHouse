-- Tags: no-fasttest

-- https://github.com/ClickHouse/ClickHouse/issues/49706
-- Using format Parquet for convenience so it errors out without output (but still deserializes the output)
-- Without the fix this would OOM the client when deserializing the state
SELECT
    topKResampleState(1048576, 257, 65536, 10)(toString(number), number)
FROM numbers(3)
FORMAT Parquet; -- { clientError UNKNOWN_TYPE }
