SET enable_analyzer=0;

WITH RECURSIVE test_table AS
    (
    SELECT 1 AS number
    UNION ALL
    SELECT number + 1
    FROM test_table
    WHERE number < 100
    )
SELECT sum(number)
FROM test_table; -- { serverError UNSUPPORTED_METHOD }
