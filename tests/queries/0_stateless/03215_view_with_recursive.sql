SET allow_experimental_analyzer = 1;

CREATE VIEW 03215_test_v
AS WITH RECURSIVE test_table AS
    (
        SELECT 1 AS number
        UNION ALL
        SELECT number + 1
        FROM test_table
        WHERE number < 100
    )
SELECT sum(number)
FROM test_table;

SELECT * FROM 03215_test_v;
